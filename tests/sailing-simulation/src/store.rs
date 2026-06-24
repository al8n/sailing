//! In-memory `LogStore`/`StableStore` impls for the simulator.
//!
//! Two write modes ([`StoreMode`]):
//!
//! - **Sync (default)** — `submit_*` applies the write to durable state AND enqueues the
//!   completion immediately (commit-on-submit). `discard_inflight()` is a no-op. This is
//!   BYTE-IDENTICAL to the original synchronous store, so every existing
//!   test passes unchanged.
//! - **Async (opt-in) — visible state + durable snapshot.** `submit_*` applies the write to
//!   the VISIBLE state IMMEDIATELY (so reads see it — this is required: the proto submits an
//!   append then reads `last_index()`/`entries()` to replicate it in the SAME call), but only
//!   DEFERS durability — it records the op in an `in_flight` list and enqueues NO completion. The
//!   driver pumps an explicit `flush()` each tick (modeling fsync completing between iterations):
//!   `flush()` snapshots the visible state into the durable snapshot and releases each deferred
//!   completion in submission order. `discard_inflight()` (a crash) ROLLS BACK the visible state
//!   to the durable snapshot — losing exactly the submitted-but-unflushed tail. This matches a
//!   real log: an appended entry is visible immediately; a crash before fsync loses only the
//!   un-synced tail. **Already-durable (fsync'd) state survives `discard_inflight`.** Reads
//!   (`entries`/`last_index`/`term`/`hard_state`/`snapshot`) reflect the VISIBLE state; the
//!   per-tick safety oracles read the DURABLE snapshot via the non-faulting
//!   [`MemLog::durable_entries`] seam, so they observe fsync'd state, never the optimistic tail.
//!
//! **Flush model — explicit `flush()`:** we use an explicit `flush()` that `Cluster::tick` calls
//! each step (before draining completions) rather than auto-releasing on the Nth `poll()`. This
//! makes the crash window directly controllable: a `crash()` that calls `discard_inflight()`
//! WITHOUT a preceding `flush()` rolls back exactly the un-flushed window.
//!
//! **Storage faults** ([`StorageFaults`]) are seeded and surface as VALUES (errors / dropped
//! writes), NEVER panics, and are OFF by default. See the struct docs for which are implemented
//! vs scaffolded. The fault PRNG is a sim-local SplitMix64 (`sailing_proto::Prng` is
//! `pub(crate)`); the read-side fault advances it through a `Cell` so the `&self` `entries` read
//! stays deterministic without changing the trait signature.
use bytes::Bytes;
use sailing_proto::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, MaybeOwned, OpId, SnapshotMeta,
  StableDone, StableStore, Term,
};
use std::{cell::Cell, collections::VecDeque, vec::Vec};

/// A small deterministic SplitMix64 PRNG, local to the simulator.
///
/// `sailing_proto::Prng` is `pub(crate)`, so the sim cannot reuse it; this is the same
/// SplitMix64 algorithm. Seeded from the cluster seed so faults are reproducible: the same seed
/// yields the same fault schedule. Never reads wall-clock / platform entropy. Shared by the
/// storage-fault stores here and the [`crate::network`] fault model (one PRNG kind, distinct
/// streams seeded from the cluster seed).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FaultPrng(u64);

impl FaultPrng {
  #[inline(always)]
  pub(crate) const fn new(seed: u64) -> Self {
    Self(seed)
  }

  #[inline]
  pub(crate) fn next_u64(&mut self) -> u64 {
    self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.0;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }

  /// `true` with probability `per_mille / 1000` (deterministic given the seed/stream).
  /// `per_mille == 0` is always `false`; `>= 1000` is always `true`.
  #[inline]
  pub(crate) fn chance_per_mille(&mut self, per_mille: u16) -> bool {
    if per_mille == 0 {
      return false;
    }
    if per_mille >= 1000 {
      return true;
    }
    (self.next_u64() % 1000) < per_mille as u64
  }
}

/// A `Cell`-wrapped [`FaultPrng`] so the `&self` committed-range `entries` read can roll + advance
/// the fault PRNG deterministically without interior `&mut`. Single-threaded (the sim is
/// single-threaded), so `Cell` is sufficient and keeps the `LogStore` trait signature unchanged.
#[derive(Debug, Default)]
struct ReadFaultPrng(Cell<FaultPrng>);

impl ReadFaultPrng {
  fn new(seed: u64) -> Self {
    Self(Cell::new(FaultPrng::new(seed)))
  }

  fn reseed(&self, seed: u64) {
    self.0.set(FaultPrng::new(seed));
  }

  /// Roll a read fault with probability `per_mille`, advancing the PRNG. `false` when off.
  fn fires(&self, per_mille: u16) -> bool {
    if per_mille == 0 {
      return false;
    }
    let mut p = self.0.get();
    let hit = p.chance_per_mille(per_mille);
    self.0.set(p);
    hit
  }
}

/// The write mode of a [`MemLog`] / [`MemStable`].
///
/// `Sync` (default) is commit-on-submit (byte-identical to the original). `Async` applies writes
/// to the VISIBLE state immediately but defers DURABILITY (the completion) until `flush()`,
/// re-opening the fsync-loss window that the proto's durability-ordering rules (append-before-ack,
/// persist-vote-before-grant, deferred-compact, commit persistence) guard against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StoreMode {
  /// `submit_*` is durable immediately and enqueues its completion immediately.
  #[default]
  Sync,
  /// `submit_*` applies to the VISIBLE state immediately (so reads see it) but enqueues no
  /// completion; `flush()` makes it durable + enqueues the completion; `discard_inflight()` rolls
  /// the visible state back to the durable snapshot (fsync loss).
  Async,
}

impl StoreMode {
  /// The stable, lowercase name (`"sync"` / `"async"`).
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Sync => "sync",
      Self::Async => "async",
    }
  }

  /// Whether this is the async (staged-write) mode.
  pub const fn is_async(&self) -> bool {
    matches!(self, Self::Async)
  }
}

/// Seeded, faults-as-data injection config for the in-memory stores. **All off by default** so
/// existing tests are unaffected. Faults are deterministic given the seed (driven by a sim-local
/// SplitMix64 PRNG) and surface as VALUES through the existing error/completion channels — they
/// NEVER panic.
///
/// Implemented:
/// - `transient_read_per_mille` — a per-read probability that the committed-range
///   `LogStore::entries` read returns [`MemStoreError::TransientRead`]. The proto's
///   `apply_committed` treats an `entries` error as unrecoverable and POISONS the node
///   (`PoisonReason::LogRead`), so this makes that poison path reachable in the sim.
///   Each roll is independent and self-clearing (the next read may succeed) — "transient".
///   Deliberately confined to `entries`: the proto's `term` callers treat a failed/zero `term`
///   read as NON-fatal (`.unwrap_or`), so faulting `term` would model a scenario the proto does
///   not claim to survive rather than the poison path (see the `term`/`entries` impls).
/// - `torn_write_per_mille` — a per-flush probability (async mode only) that the in-flight batch's
///   fsync FAILS at `flush()`: the durable snapshot is NOT advanced and no completion fires, but the
///   VISIBLE (page-cache) state is left intact and the writes stay in flight (retried on the next
///   flush). A torn write that is never followed by a successful flush is lost on the next crash —
///   so it widens the fsync-loss window WITHOUT a crash, while never rolling back state under the
///   still-running node (which would desync the node's in-memory watermarks from its log). Off by
///   default.
///
/// Scaffolded (fields reserved; not yet wired — see the per-field docs):
/// - `bit_rot_per_mille` — flip durable bytes after the fact so a later read's checksum fails.
/// - `misdirected_read_per_mille` — return the wrong slot's bytes on a read.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageFaults {
  /// Probability (per mille) a read returns [`MemStoreError::TransientRead`]. Implemented.
  pub transient_read_per_mille: u16,
  /// Probability (per mille) the in-flight batch's fsync fails at `flush` (async mode): durability
  /// is deferred (writes stay in flight, retried next flush; lost on a crash before then), the
  /// visible state is left intact. Implemented.
  pub torn_write_per_mille: u16,
  /// Bit-rot — corrupt already-durable bytes so a later checksum read fails.
  /// Reserved; not yet wired (the wire `Entry`/`HardState` checksum lands with the VOPR in a
  /// later unit).
  pub bit_rot_per_mille: u16,
  /// Misdirected read — return another slot's bytes. Reserved; not yet wired.
  pub misdirected_read_per_mille: u16,
  /// Probability (per mille) a committed-range read returns `EntriesRead::Pending`: a COLD read — the
  /// range is not resident and the store is fetching it. The proto DEFERS (apply and replication retry
  /// on the next pump; the lease/election anchors fail closed) and NEVER poisons. OFF by default and OFF
  /// in the broad sweep (the byte-identical default); driven only by `run_vopr_cold`. Implemented.
  pub cold_fetch_per_mille: u16,
}

impl StorageFaults {
  /// All faults off (the default).
  pub const fn none() -> Self {
    Self {
      transient_read_per_mille: 0,
      torn_write_per_mille: 0,
      bit_rot_per_mille: 0,
      misdirected_read_per_mille: 0,
      cold_fetch_per_mille: 0,
    }
  }

  /// Whether every fault is off (the store behaves as a faultless store).
  pub const fn is_none(&self) -> bool {
    self.transient_read_per_mille == 0
      && self.torn_write_per_mille == 0
      && self.bit_rot_per_mille == 0
      && self.misdirected_read_per_mille == 0
      && self.cold_fetch_per_mille == 0
  }
}

/// A failure reading or writing one of the in-memory stores.
///
/// Was `core::convert::Infallible`; promoted to a real error enum so seeded [`StorageFaults`]
/// can surface as VALUES through `LogStore::Error` / `StableStore::Error`. The proto treats any
/// store error as fatal (poison), so a `TransientRead` returned from a read makes the
/// poison-on-read-error path reachable in the simulator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
#[non_exhaustive]
pub enum MemStoreError {
  /// A seeded transient read fault fired: the read failed but a retry may succeed.
  TransientRead,
  /// A snapshot chunk could not be staged: the configured staging cap was exceeded — a node-fatal
  /// resource exhaustion modeling an in-RAM store that runs out of room.
  StagingFull,
}

impl MemStoreError {
  /// The stable, snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::TransientRead => "transient_read",
      Self::StagingFull => "staging_full",
    }
  }
}

impl core::error::Error for MemStoreError {}

/// In-memory write-ahead log with compaction support.
///
/// Offset model (mirrors etcd `MemoryStorage`):
/// - `offset`: the compaction boundary — the index *before* `entries[0]`; equals the
///   snapshot's `last_index`. Starts at `Index::ZERO`.
/// - `compacted_term`: term at `offset` (the snapshot's last term). Starts at `Term::ZERO`.
/// - `first_index() == offset + 1`; `last_index() == offset + entries.len()`.
///
/// **Async model — visible state + durable snapshot.** In [`StoreMode::Async`], `submit_append`
/// applies the append to the VISIBLE `entries`/`offset` IMMEDIATELY (so the proto's submit-then-read
/// contract holds — `propose` submits, then `maybe_send_append` reads `last_index()`/`entries()` and
/// sees the just-appended entry, exactly as in sync mode), but only records the op id in `in_flight`
/// WITHOUT enqueuing a completion. `flush()` snapshots the visible state into the durable snapshot
/// (`durable_entries`/`durable_offset`/`durable_compacted_term`) and releases the deferred
/// completions. `discard_inflight()` (the crash) ROLLS BACK the visible state to the durable
/// snapshot, dropping exactly the submitted-but-unflushed tail — the genuine fsync-loss window.
/// This matches a real log: an appended entry is visible immediately; a crash before fsync loses
/// only the un-synced tail. The per-tick safety oracles read the DURABLE snapshot via
/// [`durable_entries`](Self::durable_entries), so they observe durable (fsync'd) state, not the
/// optimistic visible tail.
#[derive(Debug, Default)]
pub struct MemLog {
  entries: Vec<Entry>,
  completions: VecDeque<LogDone>,
  /// Index before entries[0]. Starts at ZERO (no compaction).
  offset: Index,
  /// Term at offset (boundary term kept for consistency checks after compaction).
  compacted_term: Term,
  /// Write mode. `Sync` (default) is byte-identical to the original store.
  mode: StoreMode,
  /// Async mode only: the last-flushed durable SNAPSHOT of `entries`. `flush()` copies the visible
  /// `entries` here; `discard_inflight()`/torn-write roll the visible `entries` back to this. In
  /// sync mode this mirror is kept consistent but never read (reads use the visible `entries`).
  durable_entries: Vec<Entry>,
  /// Async mode only: the durable snapshot's `offset` (paired with `durable_entries`).
  durable_offset: Index,
  /// Async mode only: the durable snapshot's `compacted_term` (paired with `durable_entries`).
  durable_compacted_term: Term,
  /// Async mode only: op ids submitted (and already applied to the VISIBLE `entries`) but not yet
  /// flushed. `flush()` enqueues `LogDone::Appended(id)` for each in order and clears this;
  /// `discard_inflight()` clears it without enqueuing (their completions never fired). Empty in
  /// sync mode.
  in_flight: Vec<OpId>,
  /// Seeded fault config (off by default).
  faults: StorageFaults,
  /// Write-side fault PRNG (drives `torn_write` at `flush`). Deterministic given the seed.
  prng: FaultPrng,
  /// Read-side fault PRNG (drives `transient_read` on the `&self` `term`/`entries` reads).
  read_prng: ReadFaultPrng,
  /// Count of COLD (`EntriesRead::Pending`) reads returned — interior-mutable (the read is `&self`) so the
  /// cold-fetch coverage can assert non-vacuity. `0` unless `cold_fetch_per_mille > 0`.
  cold_reads: Cell<u64>,
}

impl MemLog {
  /// Empty log in the default synchronous mode.
  pub fn new() -> Self {
    Self::default()
  }

  /// Empty log in [`StoreMode::Async`] (visible-state + durable-snapshot, fsync-loss window) seeded
  /// with `seed` for any storage faults. The durable snapshot starts equal to the empty visible
  /// state; `in_flight` is empty.
  pub fn new_async(seed: u64) -> Self {
    Self {
      mode: StoreMode::Async,
      prng: FaultPrng::new(seed),
      read_prng: ReadFaultPrng::new(seed ^ 0xA5A5_A5A5_A5A5_A5A5),
      ..Self::default()
    }
  }

  /// Set the write mode. Switching to `Sync` requires no in-flight writes (debug-asserted); we only
  /// ever switch at construction in practice.
  pub fn set_mode(&mut self, mode: StoreMode) {
    debug_assert!(
      mode.is_async() || self.in_flight.is_empty(),
      "switching MemLog to Sync mode with writes still in flight"
    );
    self.mode = mode;
  }

  /// The current write mode.
  pub fn mode(&self) -> StoreMode {
    self.mode
  }

  /// Install a seeded fault config (defaults are all-off). Re-seeds both fault PRNGs so the fault
  /// schedule is reproducible from `seed`.
  pub fn set_faults(&mut self, faults: StorageFaults, seed: u64) {
    self.faults = faults;
    self.prng = FaultPrng::new(seed);
    self.read_prng.reseed(seed ^ 0xA5A5_A5A5_A5A5_A5A5);
  }

  /// Suspend the cold-fetch fault (zero its rate), returning the prior rate, so the SYNCHRONOUS restart
  /// lease-floor scans read RESIDENT — restart is resident-only, and a Pending scan read would poison.
  /// Pair with `restore_cold_fetch` around an `Endpoint::restart`.
  pub fn suspend_cold_fetch(&mut self) -> u16 {
    core::mem::replace(&mut self.faults.cold_fetch_per_mille, 0)
  }

  /// Restore a rate previously taken by `suspend_cold_fetch`.
  pub fn restore_cold_fetch(&mut self, rate: u16) {
    self.faults.cold_fetch_per_mille = rate;
  }

  /// Total COLD (`EntriesRead::Pending`) reads returned so far — the cold-fetch coverage non-vacuity signal.
  pub fn cold_reads(&self) -> u64 {
    self.cold_reads.get()
  }

  /// Async mode: make the in-flight (already-visible) appends DURABLE by snapshotting the visible
  /// state into the durable snapshot and releasing each deferred completion in submission order.
  /// Models the fsync for the in-flight window completing between driver iterations.
  ///
  /// A seeded `torn_write` fault (off by default), rolled ONCE for the whole batch, models a FAILED
  /// fsync: the durable snapshot is NOT advanced and NO completion fires this flush, but the VISIBLE
  /// (page-cache) state is left intact and the in-flight writes STAY in flight — a later `flush()`
  /// retries the fsync. (Rolling back the visible state here would be wrong: the proc is still
  /// running and has already read/acted on the visible tail; only a CRASH — `discard_inflight` —
  /// rolls visible state back. A torn write that is never followed by a successful flush is lost on
  /// the next crash, exercising the fsync-loss recovery path WITHOUT a crash advancing durability.)
  ///
  /// No-op in sync mode (writes are already durable; nothing is in flight).
  pub fn flush(&mut self) {
    if !self.mode.is_async() {
      return;
    }
    // Seeded torn-write: roll ONCE for the whole in-flight batch. If it fires, this fsync FAILED —
    // do not advance the durable snapshot, fire no completions, and leave the writes in flight
    // (visible state intact; retried on the next flush, lost on a crash before then).
    if self.prng.chance_per_mille(self.faults.torn_write_per_mille) {
      return;
    }
    // Normal flush: snapshot visible → durable, then release the deferred completions in order.
    self.durable_entries.clone_from(&self.entries);
    self.durable_offset = self.offset;
    self.durable_compacted_term = self.compacted_term;
    for id in self.in_flight.drain(..) {
      self.completions.push_back(LogDone::Appended(id));
    }
  }

  /// Drop any in-flight (not-yet-durable) work, modeling fsync loss on crash.
  ///
  /// - Sync mode: nothing is un-flushed; no-op (`in_flight` is always empty).
  /// - Async mode: ROLL BACK the visible state to the durable snapshot and clear `in_flight`
  ///   (their completions were never enqueued). **The already-durable snapshot and already-flushed
  ///   `completions` survive** — a crash loses exactly the submitted-but-unflushed tail, not
  ///   committed data.
  pub fn discard_inflight(&mut self) {
    if !self.mode.is_async() {
      return;
    }
    self.entries.clone_from(&self.durable_entries);
    self.offset = self.durable_offset;
    self.compacted_term = self.durable_compacted_term;
    self.in_flight.clear();
  }

  /// Whether there is a submitted-but-not-yet-flushed append in the fsync window. Always `false`
  /// in sync mode. Used by tests to assert a crash genuinely lands mid-window.
  pub fn has_inflight(&self) -> bool {
    !self.in_flight.is_empty()
  }

  /// The DURABLE (fsync'd) entries currently present (those above the durable compaction offset),
  /// as a raw slice — NEVER subject to the seeded `transient_read` fault that [`LogStore::entries`]
  /// injects.
  ///
  /// This is the observation seam for the per-tick safety oracles (the `checker` module): a
  /// checker must read a node's durable log WITHOUT perturbing the simulated run (the
  /// `transient_read` fault advances a PRNG and would poison the node on a `LogStore::entries`
  /// error), so it reads here instead. In async mode this returns the durable SNAPSHOT, so a
  /// submitted-but-unflushed append (visible to [`last_index`](LogStore::last_index)) is NOT yet
  /// observed by the oracles — they see only fsync'd state. In sync mode the durable snapshot is
  /// unused, so this returns the visible `entries` (which is the durable state in sync mode).
  pub fn durable_entries(&self) -> &[Entry] {
    if self.mode.is_async() {
      &self.durable_entries
    } else {
      &self.entries
    }
  }

  /// The VISIBLE log entries in the committed prefix `[first_index, commit]`, read WITHOUT drawing the
  /// `transient_read` fault PRNG — a pure observer for the VOPR's committed-frontier oracle.
  ///
  /// Like [`durable_entries`](Self::durable_entries), this exists so an oracle can inspect the log
  /// without perturbing the simulated run: the `LogStore::entries` trait read advances the read-fault
  /// PRNG and can inject an error that POISONS the node, so an oracle that read through it would change
  /// the run's fault schedule (and could mask or fabricate a poison). This reads the VISIBLE `entries`
  /// (what the proto's `apply_committed` sees — including an async submitted-but-unflushed tail the
  /// leader's commit already covers), bounded above by `commit`. Entries strictly above `commit` are
  /// excluded; a `commit` below `first_index` (e.g. everything compacted away) yields an empty slice.
  pub fn committed_entries_no_fault(&self, commit: Index) -> &[Entry] {
    let offset = self.offset.get();
    let len = self.entries.len() as u64;
    // `entries` holds indices `(offset, offset+len]`; position = index - offset - 1. The committed
    // prefix starts at `first_index` (position 0) and ends at `commit` (inclusive).
    if commit.get() <= offset {
      return &[];
    }
    let hi = (commit.get() - offset).min(len) as usize;
    &self.entries[..hi]
  }

  /// The number of durable in-memory entries (above the compaction offset). Used by the
  /// boundedness oracle to assert per-node bookkeeping stays bounded under compaction. Returns the
  /// durable-snapshot length in async mode, the visible length in sync mode (they coincide in
  /// sync).
  pub fn durable_len(&self) -> usize {
    if self.mode.is_async() {
      self.durable_entries.len()
    } else {
      self.entries.len()
    }
  }

  /// The DURABLE (fsync'd) `first_index` — `durable_offset + 1` in async mode (matching the durable
  /// snapshot), the visible `first_index` in sync mode. The oracles consume this so their durable
  /// window `[first..=last]` stays consistent with [`durable_entries`](Self::durable_entries).
  pub fn durable_first_index(&self) -> Index {
    if self.mode.is_async() {
      Index::new(self.durable_offset.get() + 1)
    } else {
      self.first_index()
    }
  }

  /// The DURABLE (fsync'd) `last_index` — `durable_offset + durable_entries.len()` in async mode
  /// (excludes a submitted-but-unflushed tail), the visible `last_index` in sync mode. The oracles
  /// consume this so their durable window stays consistent with the durable snapshot.
  pub fn durable_last_index(&self) -> Index {
    if self.mode.is_async() {
      Index::new(self.durable_offset.get() + self.durable_entries.len() as u64)
    } else {
      self.last_index()
    }
  }

  /// Apply one append to the durable `entries` (truncate-then-extend). Shared by the sync
  /// `submit_append` fast path and the async `flush` path so the two are byte-identical.
  fn apply_append(&mut self, entries: &[Entry]) {
    if let Some(first) = entries.first() {
      debug_assert!(
        first.index().get() > self.offset.get(),
        "submit_append below the compaction offset"
      );
      // Truncate position: first.index() - offset - 1 (saturating to 0)
      let offset = self.offset.get();
      let fi = first.index().get();
      let from = if fi <= offset + 1 {
        0usize
      } else {
        (fi - offset - 1) as usize
      };
      self.entries.truncate(from);
    }
    self.entries.extend_from_slice(entries);
  }
}

impl LogStore for MemLog {
  type Error = MemStoreError;

  fn first_index(&self) -> Index {
    Index::new(self.offset.get() + 1)
  }

  fn last_index(&self) -> Index {
    Index::new(self.offset.get() + self.entries.len() as u64)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    // NOTE: `transient_read` is intentionally NOT injected on `term`. Per the trait's domain
    // contract a `term` Err means a GENUINE storage fault and is poison-fatal (the proto's
    // `log_term` choke point poisons on Err) — `term` is read on virtually every inbound message,
    // so injecting transient faults here would poison nodes constantly and the run would measure
    // the restart path, not consensus. The fatal-read poison path is exercised through the
    // committed-range `entries` read instead.
    if index == self.offset {
      return Ok(self.compacted_term);
    }
    if index < self.offset {
      // compacted away — out-of-range read
      return Ok(Term::ZERO);
    }
    let last = self.last_index();
    if index > last {
      return Ok(Term::ZERO);
    }
    // pos = index - offset - 1 (0-based into entries)
    let pos = (index.get() - self.offset.get() - 1) as usize;
    Ok(self.entries[pos].term())
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    // Seeded transient-read fault on the committed-range read: surface as a fatal read error. The
    // proto's `apply_committed` treats an `entries` error as unrecoverable and POISONS the node
    // (PoisonReason::LogRead), so this makes that poison path reachable in the sim.
    if self.read_prng.fires(self.faults.transient_read_per_mille) {
      return Err(MemStoreError::TransientRead);
    }
    // Seeded COLD-read fault: the range is "not resident" — defer (Pending). The proto retries on the
    // next pump (apply/replication) or fails closed (the lease/election anchors); it never poisons.
    if self.read_prng.fires(self.faults.cold_fetch_per_mille) {
      self.cold_reads.set(self.cold_reads.get().wrapping_add(1));
      return Ok(EntriesRead::Pending);
    }
    let start = range.start.get();
    let end = range.end.get();
    let offset = self.offset.get();
    let len = self.entries.len() as u64;
    // Convert 1-based absolute index to 0-based vec position: pos = index - offset - 1
    // Saturate to 0 if start <= offset (compacted), clamp to len.
    let lo = if start <= offset {
      0usize
    } else {
      (start - offset - 1) as usize
    };
    let mut hi = if end <= offset {
      0usize
    } else {
      ((end - offset - 1).min(len)) as usize
    };
    // Respect the byte cap (trait contract: roughly `max_bytes`, always at least one entry if the
    // range is non-empty) — a real store caps here, and the proto's send path relies on the cap to
    // bound per-message work. Charging only payload bytes is the "roughly".
    if hi > lo {
      let mut budget = max_bytes;
      let mut capped = lo;
      for e in &self.entries[lo..hi] {
        let cost = e.data().len() as u64;
        if capped > lo && cost > budget {
          break;
        }
        budget = budget.saturating_sub(cost);
        capped += 1;
      }
      hi = capped;
    }
    let lo = lo.min(self.entries.len());
    let hi = hi.max(lo).min(self.entries.len());
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(
      &self.entries[lo..hi],
    )))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if self.mode.is_async() {
      // Async: apply to the VISIBLE state IMMEDIATELY (so the proto's submit-then-read contract
      // holds — reads see the just-appended entry, exactly as in sync), but DEFER durability: record
      // the op id in `in_flight` and enqueue NO completion. `flush()` releases the completion and
      // makes it durable; a crash before the next `flush()` (via `discard_inflight`) rolls the
      // visible state back to the durable snapshot, losing exactly this in-flight tail.
      self.apply_append(entries);
      self.in_flight.push(id);
      return;
    }
    // Sync (byte-identical to the original): durable immediately + completion enqueued.
    self.apply_append(entries);
    self.completions.push_back(LogDone::Appended(id));
  }

  fn compact(&mut self, up_to: Index) {
    if up_to <= self.offset || self.entries.is_empty() {
      return; // no-op: already compacted or nothing to compact
    }
    let last = self.last_index();
    let up_to = if up_to > last { last } else { up_to };
    // Read the boundary term (from the VISIBLE state) before draining.
    let boundary_term = self.term(up_to).unwrap_or(Term::ZERO);
    // Number of VISIBLE entries to remove: up_to - offset (position of up_to in `entries`, +1).
    let drain_count = (up_to.get() - self.offset.get()) as usize;
    let drain_count = drain_count.min(self.entries.len());
    self.entries.drain(0..drain_count);
    self.offset = up_to;
    self.compacted_term = boundary_term;
    // Async mode: GC the same already-durable prefix from the durable snapshot so it stays
    // consistent. Compaction only ever removes already-durable entries (the proto compacts at or
    // below the applied index, which is durable), so the durable snapshot covers `up_to`. Compute
    // the durable drain relative to `durable_offset` and clamp to its (possibly shorter) length.
    if self.mode.is_async() && up_to > self.durable_offset {
      let durable_drain = (up_to.get() - self.durable_offset.get()) as usize;
      let durable_drain = durable_drain.min(self.durable_entries.len());
      self.durable_entries.drain(0..durable_drain);
      self.durable_offset = up_to;
      self.durable_compacted_term = boundary_term;
    }
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    // Discard all entries: the follower's entire log is replaced by the snapshot.
    // Drop any pending completions for discarded appends — they will never fire.
    // Also drop any in-flight (un-flushed) appends — a restore supersedes in-flight writes.
    self.entries.clear();
    self.completions.clear();
    self.in_flight.clear();
    // Re-baseline: offset == last_index so that first_index() == last_index + 1
    // and term(last_index) == last_term (the snapshot boundary term).
    self.offset = last_index;
    self.compacted_term = last_term;
    // A restore is an IMMEDIATE durable re-baseline: re-baseline the durable snapshot too (async).
    if self.mode.is_async() {
      self.durable_entries.clear();
      self.durable_offset = last_index;
      self.durable_compacted_term = last_term;
    }
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Ready-to-poll only: the async `in_flight` set is submitted-but-unflushed (no completion
    // enqueued until `flush`), so it is excluded — counting it would make the driver hot-spin.
    !self.completions.is_empty()
  }
}

/// In-memory durable metadata store.
///
/// **Async model — visible state + durable snapshot.** In [`StoreMode::Async`],
/// `submit_write`/`submit_snapshot` set the VISIBLE `hard_state`/`snapshot` IMMEDIATELY (so the
/// proto's submit-then-read contract holds) but DEFER durability: they record the op id + kind in
/// `in_flight` and enqueue NO completion. `flush()` snapshots the visible state into the durable
/// snapshot (`durable_hard_state`/`durable_snapshot`) and releases the deferred completions in
/// submission order. `discard_inflight()` (the crash) ROLLS BACK the visible state to the durable
/// snapshot, losing exactly the submitted-but-unflushed window (fsync loss). `hard_state()` /
/// `snapshot()` read the VISIBLE state.
#[derive(Debug)]
pub struct MemStable<I> {
  hard_state: HardState<I>,
  completions: VecDeque<StableDone>,
  snapshot: Option<(SnapshotMeta<I>, Bytes)>,
  /// Write mode. `Sync` (default) is byte-identical to the original store.
  mode: StoreMode,
  /// Async mode only: the last-flushed durable SNAPSHOT of `hard_state`. `flush()` copies the
  /// visible `hard_state` here; rollback (`discard_inflight`/torn-write) restores from it.
  durable_hard_state: HardState<I>,
  /// Async mode only: the last-flushed durable SNAPSHOT of `snapshot`. Paired with
  /// `durable_hard_state`.
  durable_snapshot: Option<(SnapshotMeta<I>, Bytes)>,
  /// Async mode only: writes submitted (and already applied to the VISIBLE state) but not yet
  /// flushed, with the kind of completion each owes. `flush()` enqueues the matching completion in
  /// order and clears this; `discard_inflight()` clears it without enqueuing. Empty in sync mode.
  in_flight: Vec<(OpId, StableKind)>,
  /// Seeded fault config (off by default).
  faults: StorageFaults,
  /// Write-side fault PRNG (drives `torn_write` at `flush`). Deterministic given the seed.
  prng: FaultPrng,
  /// Chunked-snapshot staging accumulator (one in-flight transfer at a time).
  snapshot_staging: Option<(SnapshotMeta<I>, sailing_proto::SnapshotStaging)>,
  /// Optional staging-capacity cap (bytes); a `total_len` beyond it fails `accept_snapshot_chunk`,
  /// modeling an in-RAM store that runs out of room.
  staging_cap: Option<usize>,
}

/// Which completion an in-flight (async, not-yet-flushed) stable-store write owes at `flush`.
#[derive(Debug, Clone, Copy)]
enum StableKind {
  /// A hard-state write → `StableDone::Wrote`.
  Wrote,
  /// A snapshot write → `StableDone::SnapshotWritten`.
  SnapshotWritten,
}

impl<I: sailing_proto::NodeId> MemStable<I> {
  /// Fresh store at the initial hard state, in the default synchronous mode.
  pub fn new() -> Self {
    Self {
      hard_state: HardState::initial(),
      completions: VecDeque::new(),
      snapshot: None,
      mode: StoreMode::Sync,
      durable_hard_state: HardState::initial(),
      durable_snapshot: None,
      in_flight: Vec::new(),
      faults: StorageFaults::none(),
      prng: FaultPrng::default(),
      snapshot_staging: None,
      staging_cap: None,
    }
  }

  /// Fresh store in [`StoreMode::Async`] (visible-state + durable-snapshot, fsync-loss window)
  /// seeded with `seed`. The durable snapshot starts equal to the initial visible state;
  /// `in_flight` is empty.
  pub fn new_async(seed: u64) -> Self {
    Self {
      mode: StoreMode::Async,
      prng: FaultPrng::new(seed),
      ..Self::new()
    }
  }

  /// Cap the chunked-snapshot staging buffer (bytes); a `total_len` beyond `cap` fails
  /// `accept_snapshot_chunk` with [`MemStoreError::StagingFull`] (a node-fatal resource error).
  #[must_use]
  pub fn with_staging_cap(mut self, cap: usize) -> Self {
    self.staging_cap = Some(cap);
    self
  }

  /// Set the write mode. Switching to `Sync` requires no in-flight writes (debug-asserted).
  pub fn set_mode(&mut self, mode: StoreMode) {
    debug_assert!(
      mode.is_async() || self.in_flight.is_empty(),
      "switching MemStable to Sync mode with writes still in flight"
    );
    self.mode = mode;
  }

  /// The current write mode.
  pub fn mode(&self) -> StoreMode {
    self.mode
  }

  /// Install a seeded fault config (defaults are all-off). Re-seeds the write-side fault PRNG.
  pub fn set_faults(&mut self, faults: StorageFaults, seed: u64) {
    self.faults = faults;
    self.prng = FaultPrng::new(seed);
  }

  /// Async mode: make the in-flight (already-visible) writes DURABLE by snapshotting the visible
  /// state into the durable snapshot and releasing each deferred completion in submission order.
  /// Models the fsync for the in-flight window completing between driver iterations.
  ///
  /// A seeded `torn_write` fault (off by default), rolled ONCE for the whole batch, models a FAILED
  /// fsync: the durable snapshot is NOT advanced and NO completion fires this flush, but the VISIBLE
  /// state is left intact and the writes STAY in flight (retried on the next flush, lost on a crash
  /// before then). Rolling back the visible state here would be wrong (the proc is still running and
  /// has acted on the visible value); only a CRASH (`discard_inflight`) rolls visible state back.
  ///
  /// No-op in sync mode (writes are already durable; nothing is in flight).
  pub fn flush(&mut self) {
    if !self.mode.is_async() {
      return;
    }
    // Seeded torn-write: roll ONCE for the whole in-flight batch. If it fires, this fsync FAILED —
    // do not advance the durable snapshot, fire no completions, and leave the writes in flight
    // (visible state intact; retried on the next flush, lost on a crash before then).
    if self.prng.chance_per_mille(self.faults.torn_write_per_mille) {
      return;
    }
    // Normal flush: snapshot visible → durable, then release the deferred completions in order.
    self.durable_hard_state = self.hard_state.clone();
    self.durable_snapshot.clone_from(&self.snapshot);
    for (id, kind) in self.in_flight.drain(..) {
      match kind {
        StableKind::Wrote => self.completions.push_back(StableDone::Wrote(id)),
        StableKind::SnapshotWritten => self.completions.push_back(StableDone::SnapshotWritten(id)),
      }
    }
  }

  /// Drop any in-flight (not-yet-durable) work, modeling fsync loss on crash.
  ///
  /// - Sync mode: nothing is un-flushed; no-op (`in_flight` is always empty).
  /// - Async mode: ROLL BACK the visible `hard_state`/`snapshot` to the durable snapshot and clear
  ///   `in_flight` (their completions were never enqueued). **The already-durable snapshot and
  ///   already-flushed `completions` survive** — a crash loses the fsync window, not committed
  ///   metadata.
  pub fn discard_inflight(&mut self) {
    if !self.mode.is_async() {
      return;
    }
    self.hard_state = self.durable_hard_state.clone();
    self.snapshot.clone_from(&self.durable_snapshot);
    self.in_flight.clear();
    // An in-RAM store loses chunk staging on a crash — the transfer restarts from offset 0.
    self.snapshot_staging = None;
  }

  /// Whether there is a submitted-but-not-yet-flushed write in the fsync window. Always `false` in
  /// sync mode.
  pub fn has_inflight(&self) -> bool {
    !self.in_flight.is_empty()
  }
}

impl<I: sailing_proto::NodeId> Default for MemStable<I> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I: sailing_proto::NodeId> StableStore for MemStable<I> {
  type NodeId = I;
  type Error = MemStoreError;

  fn hard_state(&self) -> HardState<I> {
    // NOTE: `hard_state` has no `Result` return, so a transient read fault cannot be surfaced here
    // as a value without changing the trait. `transient_read` is confined to the committed-range
    // `LogStore::entries` read (the proto's poison path).
    //
    // CONTRACT FIDELITY: the trait documents `hard_state()` as LAST-DURABLE. In async mode the
    // visible state runs ahead of durability (submit-then-flush), so returning it would make the
    // sim LAXER than the contract — a real store returning genuinely-durable state could then be
    // conformant-but-broken in ways the sim never exercises. Return the durable snapshot, exactly
    // what a disk store would read back.
    if self.mode.is_async() {
      self.durable_hard_state.clone()
    } else {
      self.hard_state.clone()
    }
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<I>) {
    if self.mode.is_async() {
      // Async: set the VISIBLE hard_state IMMEDIATELY (submit-then-read), DEFER durability.
      self.hard_state = hard_state;
      self.in_flight.push((id, StableKind::Wrote));
      return;
    }
    // Sync (byte-identical to the original): durable immediately + completion enqueued.
    self.hard_state = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<I>, data: Bytes) {
    if self.mode.is_async() {
      // Async: set the VISIBLE snapshot IMMEDIATELY (submit-then-read), DEFER durability.
      self.snapshot = Some((meta, data));
      self.in_flight.push((id, StableKind::SnapshotWritten));
      return;
    }
    // Sync (byte-identical to the original): durable immediately + completion enqueued.
    self.snapshot = Some((meta, data));
    self.completions.push_back(StableDone::SnapshotWritten(id));
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<I>, Bytes)> {
    self.snapshot.clone()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<I>> {
    // Async: the durable slot, advanced only by `flush()` (after a successful fsync) — `None` while a
    // submitted blob is visible-but-not-yet-flushed, and rolled back by `discard_inflight()`. Sync: every
    // submit is immediately durable, so the visible slot IS the durable one.
    if self.mode.is_async() {
      self.durable_snapshot.as_ref().map(|(m, _)| m.clone())
    } else {
      self.snapshot.as_ref().map(|(m, _)| m.clone())
    }
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<I>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    let boundary = meta.last_index();
    match &self.snapshot_staging {
      Some((m, _)) if m.last_index() > boundary => return Ok(0),
      Some((m, s)) if !m.identity_eq(meta) || s.total_len() != total_len => {
        self.snapshot_staging = None
      }
      _ => {}
    }
    if self.snapshot_staging.is_none() {
      let cap = self.staging_cap.unwrap_or(1 << 30);
      match sailing_proto::SnapshotStaging::new(boundary, total_len, cap) {
        Some(s) => self.snapshot_staging = Some((meta.clone(), s)),
        None => return Err(MemStoreError::StagingFull),
      }
    }
    Ok(
      self
        .snapshot_staging
        .as_mut()
        .expect("staging set above")
        .1
        .accept(offset, data),
    )
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<I>) -> Option<Bytes> {
    let complete = matches!(
      &self.snapshot_staging,
      Some((m, s)) if m.identity_eq(meta) && s.is_complete()
    );
    complete.then(|| {
      let (_, s) = self
        .snapshot_staging
        .take()
        .expect("checked complete above");
      Bytes::from(s.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.snapshot_staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    // Ready-to-poll only: the async `in_flight` set is submitted-but-unflushed (no completion
    // enqueued until `flush`), so it is excluded — counting it would make the driver hot-spin.
    !self.completions.is_empty()
  }
}

#[cfg(test)]
mod tests;
