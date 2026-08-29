//! The [`MultiEngine`] suite: the visibility matrix at every edge, barrier atomicity, the lineage
//! records, and what a crash leaves behind.

use super::{Durability, EngineSubject, Report};
use crate::fault::CrashClass;
use bytes::Bytes;
use core::time::Duration;
use sailing_proto::{
  ConfState, EntriesRead, Entry, EntryKind, FloorStore, ForkId, GroupStores,
  HIGHEST_WORKING_GENERATION, HardState, Index, LeaseSupport, LogDone, LogStore, MERGED_FLOOR,
  MultiEngine, NodeId, OpId, ReadOnlyOption, SnapshotMeta, StableDone, StableStore, Term,
};
use std::{format, vec::Vec};

/// The engine's per-group log handle type, spelled once.
type EngineLogOf<S> = <<S as EngineSubject>::Engine as MultiEngine<
  <S as EngineSubject>::Group,
  <S as EngineSubject>::NodeId,
>>::Log;
/// The engine's per-group stable handle type, spelled once.
type EngineStableOf<S> = <<S as EngineSubject>::Engine as MultiEngine<
  <S as EngineSubject>::Group,
  <S as EngineSubject>::NodeId,
>>::Stable;

/// EVERYTHING one group's durable state amounts to, as a reopen can read it back.
///
/// The crash checks compare these whole. A partial oracle — indices without payloads, a hard state
/// without its vote, a snapshot meta without its blob — passes an engine that reopens with the right
/// SHAPE and the wrong CONTENT, which is precisely the failure a durable store makes and the one
/// nothing downstream reports.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupImage<I> {
  hosted: bool,
  first_index: Index,
  last_index: Index,
  /// Every retained entry, verbatim: term, index, kind, and payload bytes.
  entries: Vec<Entry>,
  hard_state: HardState<I>,
  /// What the SERVING slot holds — the meta verbatim and its bytes. A store that keeps the header
  /// and loses the blob reopens claiming a boundary it cannot serve; one that keeps the shape and
  /// re-derives the fields reopens with a boundary whose lease bounds, read mode and shape
  /// generation are invented.
  visible_snapshot: Option<(SnapshotMeta<I>, Bytes)>,
  /// What the DURABLE reader answers, recorded separately. Folding the two together — taking the
  /// durable meta whenever the visible slot merely resembled it — hid both a visible slot with no
  /// durable backing and a durable answer that had drifted from the slot it describes.
  durable_snapshot: Option<SnapshotMeta<I>>,
  floor: u64,
  lineage: u64,
  removal_floor: u64,
}

impl<I: NodeId> GroupImage<I> {
  /// The image of an id no barrier ever reached.
  fn absent() -> Self {
    Self {
      hosted: false,
      first_index: Index::new(1),
      last_index: Index::ZERO,
      entries: Vec::new(),
      hard_state: HardState::initial(),
      visible_snapshot: None,
      durable_snapshot: None,
      floor: 0,
      lineage: 0,
      removal_floor: 0,
    }
  }
}

/// When the suite drains completions relative to the barriers it crashes around.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainPolicy {
  /// Never poll. Durability must precede a barrier's RETURN, so an engine that persists at the
  /// drain has nothing on the medium when this crashes.
  NoDrain,
  /// Drain the first barrier's completions only.
  Partial,
  /// Drain after every barrier.
  Full,
}

impl DrainPolicy {
  const fn label(self) -> &'static str {
    match self {
      Self::NoDrain => "no-drain",
      Self::Partial => "partial-drain",
      Self::Full => "full-drain",
    }
  }
}

/// One group's stores, lent out of a live engine, as a store subject: the engine's own barrier is
/// the barrier. This is what lets the store-level batteries run against the handles an engine
/// actually hands a driver, rather than against a store built beside it.
struct LentStores<'a, S: EngineSubject> {
  engine: &'a mut S::Engine,
  gid: S::Group,
  node: S::NodeId,
}

impl<S: EngineSubject> super::LogSubject for LentStores<'_, S> {
  type Log = EngineLogOf<S>;

  fn log(&mut self) -> &mut Self::Log {
    self
      .engine
      .stores(&self.gid)
      .expect("the battery's group is admitted before it runs")
      .0
  }

  fn barrier(&mut self) {
    self.engine.flush();
  }
}

impl<S: EngineSubject> super::StableSubject for LentStores<'_, S>
where
  S::NodeId: Clone,
{
  type Stable = EngineStableOf<S>;

  fn stable(&mut self) -> &mut Self::Stable {
    self
      .engine
      .stores(&self.gid)
      .expect("the battery's group is admitted before it runs")
      .1
  }

  fn barrier(&mut self) {
    self.engine.flush();
  }

  fn node_id(&self, _n: u64) -> S::NodeId {
    // The engine subject mints the ids; a store battery only ever records one vote.
    self.node.clone()
  }
}

fn entry(term: u64, index: u64) -> Entry {
  Entry::new(
    Term::new(term),
    Index::new(index),
    EntryKind::Normal,
    Bytes::from_static(b"conformance"),
  )
}

fn run(term: u64, from: u64, through: u64) -> Vec<Entry> {
  (from..=through).map(|i| entry(term, i)).collect()
}

/// Named so a store fault surfacing outside a fault battery is reported rather than read as
/// quiescence: `while let Some(Ok(_))` ends the loop on an error exactly as it ends on `None`.
const NO_SPURIOUS_ERROR: &str = "engine/poll-no-spurious-error";

fn drain_log<L>(log: &mut L, report: &mut Report) -> Vec<LogDone>
where
  L: LogStore,
{
  let mut out = Vec::new();
  loop {
    match log.poll() {
      Some(Ok(done)) => out.push(done),
      Some(Err(_)) => {
        report.require(
          NO_SPURIOUS_ERROR,
          false,
          "the log reported a store fault during a conforming sequence; nothing here injects one",
        );
        break;
      }
      None => break,
    }
  }
  report.require(NO_SPURIOUS_ERROR, true, "");
  out
}

fn drain_stable<S>(store: &mut S, report: &mut Report) -> Vec<StableDone>
where
  S: StableStore,
{
  let mut out = Vec::new();
  loop {
    match store.poll() {
      Some(Ok(done)) => out.push(done),
      Some(Err(_)) => {
        report.require(
          NO_SPURIOUS_ERROR,
          false,
          "the stable store reported a fault during a conforming sequence; nothing injects one",
        );
        break;
      }
      None => break,
    }
  }
  report.require(NO_SPURIOUS_ERROR, true, "");
  out
}

/// Write a lineage generation, and pin the CALLER's half of the contract in the same breath.
///
/// `set_group_gen` puts the obligation on the writer: the value is a WORKING generation, strictly
/// below [`HIGHEST_WORKING_GENERATION`]. An engine may assume that and is NOT required to clamp a
/// record it was handed in the reserved band. So the kit holds itself to it — the first assertion
/// is the suite's own fixture invariant, not a claim about the subject — and then requires the one
/// consequence a subject owes either way: the fence that follows a legal write is a fence, not the
/// reserved terminal. `MERGED_FLOOR` is read cluster-wide as proof that a lineage was absorbed
/// away, so an ordinary local removal producing one clears a live thaw obligation on every replica.
fn set_gen_checked<S>(engine: &mut S::Engine, gid: &S::Group, generation: u64, report: &mut Report)
where
  S: EngineSubject,
{
  assert!(
    generation < HIGHEST_WORKING_GENERATION,
    "the kit never hands a subject a reserved generation: {generation} is not a working one"
  );
  engine.set_group_gen(gid, generation);
  let fence = engine.removal_floor(gid);
  report.require(
    "engine/lineage-record-rejects-the-reserved-band",
    fence < MERGED_FLOOR,
    std::format!(
      "after a lineage record of {generation} — a working generation, as the caller contract \
       requires — the id's removal fence answered {fence}, the reserved terminal. That value is a \
       GLOBAL verdict that the lineage was absorbed away; a local removal has no standing to write \
       it, and every replica reading it discharges a thaw obligation that is still owed"
    ),
  );
}

/// The highest generation an id can legitimately hold: the product reserves the TOP TWO values, so
/// a working generation stops one below [`HIGHEST_WORKING_GENERATION`] and its removal ceiling's
/// `+ 1` lands exactly ON that boundary — representable, strictly fencing, never the
/// [`MERGED_FLOOR`] sentinel.
///
/// Every removal-ceiling fixture below is expressed relative to it. At generations 2, 4 and 8 any
/// arithmetic answers correctly; the reservation only bites here.
const TOP_WORKING_GENERATION: u64 = HIGHEST_WORKING_GENERATION - 1;

/// Every check the engine suite must reach whatever the subject's durability tier.
const REQUIRED_ALWAYS: &[&str] = &[
  "engine/admission-is-explicit",
  "engine/barrier-releases-every-group-at-once",
  "engine/barrier-releases-every-groups-completions",
  "engine/barrier-releases-nothing-early",
  "engine/batch-metrics-count-every-barrier",
  "engine/boot-epoch-refused-for-an-unhosted-id",
  "engine/boot-epoch-strictly-increases",
  "engine/durable-index-covers-a-released-append",
  "engine/durable-snapshot-advances-at-the-barrier",
  "engine/durable-snapshot-is-never-the-visible-slot",
  "engine/fresh-subject",
  "engine/hard-state-advances-at-the-barrier",
  "engine/hard-state-is-last-durable",
  "engine/has-staged-reports-owed-barriers",
  "engine/hosted-ids-lend-stores",
  "engine/lineage-fold-rides-the-data-barrier",
  "engine/lineage-is-monotone",
  "engine/lineage-outlives-remove-group",
  "engine/lineage-reads-freshest",
  "engine/lineage-record-rejects-the-reserved-band",
  "engine/log-read-view-is-immediate",
  "engine/poll-no-spurious-error",
  "engine/re-admission-does-not-clear-the-fence",
  "engine/re-admission-lends-empty-stores",
  "engine/removal-ceiling-folds-a-shape-entry",
  "engine/removal-ceiling-folds-the-snapshot-meta",
  "engine/removal-ceiling-is-zero-for-an-unreshaped-id",
  "engine/removal-ceiling-never-reaches-the-terminal",
  "engine/removal-ceiling-retracts-a-truncated-shape-entry",
  "engine/removal-ceiling-retracts-with-the-slot",
  "engine/removal-reports-absence",
  "engine/reopen-manufactures-no-completions",
  "engine/reopened-durable-hard-state-agrees",
  "engine/reopened-durable-index-never-over-answers",
  "engine/snapshot-is-visible-at-submit",
  "engine/staging-cap-refuses-an-oversized-transfer",
  "engine/staging-cap-still-admits-what-fits",
  "engine/staged-work-is-invisible-to-has-pending",
  "engine/terminal-floor-folds-and-admits-nothing",
];

/// The crash half asks OPPOSITE questions of the two tiers, so each tier owes its own set. A
/// volatile subject that quietly stopped reaching the survival checks would otherwise read as
/// fully covered.
const REQUIRED_VOLATILE: &[&str] = &["engine/volatile-engine-keeps-nothing"];

/// What a durable subject owes on top of [`REQUIRED_ALWAYS`].
const REQUIRED_DURABLE: &[&str] = &[
  "engine/a-clean-drop-keeps-what-the-barriers-covered",
  "engine/a-reopened-log-is-resident-and-readable",
  "engine/an-append-acknowledged-before-a-barrier-survives",
  "engine/an-issued-epoch-survives-an-unflushed-crash",
  "engine/barrier-is-all-or-nothing-across-a-crash",
  "engine/boot-epoch-never-repeats-across-a-reopen",
  "engine/durability-precedes-the-barriers-return",
  "engine/exactly-flush-covered-state-survives",
  "engine/exactly-the-maximal-valid-prefix-survives",
];

/// The one check only a VOLATILE subject may leave unasked: nothing it hosted comes back, so no
/// completion queue survives the reopen to inspect. A durable subject owes an answer.
const SKIPPABLE_VOLATILE: &[&str] = &["engine/reopen-manufactures-no-completions"];

/// Checks a subject may legitimately leave unasked, each for a reason the report states: the
/// optional durable probes, the legs an at-submit engine answers before the question is posed, and
/// the torn-tail legs a subject that will not name its medium's boundaries cannot settle.
const SKIPPABLE: &[&str] = &[
  "engine/an-append-acknowledged-before-a-barrier-survives",
  "engine/barrier-is-all-or-nothing-across-a-crash",
  "engine/barrier-releases-nothing-early",
  "engine/durability-precedes-the-barriers-return",
  "engine/durable-index-covers-a-released-append",
  "engine/durable-snapshot-is-never-the-visible-slot",
  "engine/exactly-the-maximal-valid-prefix-survives",
  "engine/hard-state-is-last-durable",
  "engine/removal-ceiling-folds-a-shape-entry",
  "engine/removal-ceiling-retracts-a-truncated-shape-entry",
  "engine/reopened-durable-hard-state-agrees",
  "engine/reopened-durable-index-never-over-answers",
];

/// Check a [`MultiEngine`] against the barrier, lineage, and crash contracts.
pub fn engine<S>(subject: &mut S) -> Report
where
  S: EngineSubject,
  <EngineLogOf<S> as LogStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::NodeId: PartialEq,
  S::NodeId: Clone,
{
  let mut report = Report::new();
  live_half(subject, &mut report);
  crash_half(subject, &mut report);
  report.absorb(super::restore_admission(subject));
  let mut required: Vec<&'static str> = REQUIRED_ALWAYS.to_vec();
  let mut skippable: Vec<&'static str> = SKIPPABLE.to_vec();
  match subject.durability() {
    Durability::Volatile => {
      required.extend(REQUIRED_VOLATILE);
      skippable.extend(SKIPPABLE_VOLATILE);
    }
    Durability::Durable => required.extend(REQUIRED_DURABLE),
  }
  report.require_coverage(&required, &skippable);
  report
}

/// Everything checkable without crashing: the visibility matrix, the barrier, the lineage records.
fn live_half<S>(subject: &mut S, report: &mut Report)
where
  S: EngineSubject,
  <EngineLogOf<S> as LogStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::Error: core::fmt::Debug,
  S::NodeId: Clone,
{
  let mut engine = subject.open();
  let a = subject.group(1);
  let b = subject.group(2);
  let c = subject.group(3);
  let node = subject.node(1);

  // EVERY ID THIS SUITE WILL USE, on every reader. Sampling one left a stale terminal floor on an
  // id the suite admits later indistinguishable from one folded during the run — and a boot epoch
  // carried over from a previous incarnation likewise invisible.
  let fresh = engine.group_ids().next().is_none()
    && !engine.has_staged()
    && (1..=8).all(|n| {
      let gid = subject.group(n);
      FloorStore::floor(&engine, &gid) == 0
        && FloorStore::lineage(&engine, &gid) == 0
        && engine.removal_floor(&gid) == 0
        && engine.next_boot_epoch(&gid).is_none()
    });
  report.require(
    "engine/fresh-subject",
    fresh,
    "a freshly opened engine hosts no group, holds no staged work, and remembers no lineage",
  );

  report.require(
    "engine/admission-is-explicit",
    engine.add_group(a.clone()) && !engine.add_group(a.clone()) && engine.contains_group(&a),
    "add_group admits once and reports false — storage untouched — on a repeat",
  );
  engine.add_group(b.clone());
  engine.add_group(c.clone());
  report.require(
    "engine/admission-is-explicit",
    !engine.contains_group(&subject.group(99)) && engine.stores(&subject.group(99)).is_none(),
    "an unadmitted group resolves to no stores — the deliberate unhosted-drop path",
  );
  report.require(
    "engine/removal-reports-absence",
    !engine.remove_group(&subject.group(99)),
    "removing a group that is not hosted reports false",
  );
  engine.flush();

  // THE VISIBILITY MATRIX. Each read has its own rule, and the point of driving them together is
  // that a barrier moves several of them at once, in one step, and none of them before.
  // RICH ON EVERY AXIS. The two checks these feed say in as many words that an identity match
  // would accept a slot whose lease windows, read mode or provenance were rebuilt from defaults —
  // and with the fixture AT those defaults, that failure was undetectable.
  let meta = SnapshotMeta::new(
    Index::new(2),
    Term::new(1),
    ConfState::from_voters([node.clone()]),
  )
  .with_shape_gen(TOP_WORKING_GENERATION - 8)
  .with_max_lease_window(4_321)
  .with_max_wall_plus_window(8_765)
  .with_max_unwalled_lease_window(2_109)
  .with_read_only(ReadOnlyOption::LeaseGuard)
  .with_fork_id(ForkId::new(
    Bytes::from_static(b"engine-parent"),
    11,
    Index::new(1),
    Term::new(1),
    Bytes::from_static(b"engine-child"),
    12,
  ));
  let written = HardState::initial()
    .with_term(Term::new(1))
    .with_vote(Some(node.clone()))
    .with_commit(Index::new(1))
    .with_lease_support(LeaseSupport::Recorded(Some(
      core::time::Duration::from_millis(750),
    )))
    .with_lineage(Some(ForkId::new(
      Bytes::from_static(b"engine-hs-parent"),
      13,
      Index::new(2),
      Term::new(1),
      Bytes::from_static(b"engine-hs-child"),
      14,
    )))
    .with_founding_gen(19);
  {
    let (log, stable) = engine.stores(&a).expect("group a is hosted");
    log.submit_append(OpId::new(1), &run(1, 1, 3));
    // THE WHOLE READ VIEW. The core reads `term` on every stale `prev_log_index` and `entries` on
    // every replication crank, so a store that moves the tip and serves neither is one the tip
    // alone certifies.
    let resident = match log.entries(Index::new(1)..Index::new(4), u64::MAX) {
      Ok(EntriesRead::Ready(view)) => view.to_vec(),
      _ => Vec::new(),
    };
    report.require(
      "engine/log-read-view-is-immediate",
      log.last_index() == Index::new(3)
        && log.first_index() == Index::new(1)
        && log.term(Index::new(3)).ok() == Some(Term::new(1))
        && resident == run(1, 1, 3),
      std::format!(
        "a submitted append is visible to the log's read view before it is durable: expected \
         1..=3 with term(3)==1 and the entries verbatim, got {:?}..={:?} term {:?} entries \
         {resident:?}",
        log.first_index(),
        log.last_index(),
        log.term(Index::new(3)).ok()
      ),
    );
    stable.submit_write(OpId::new(2), written.clone());
    stable.submit_snapshot(OpId::new(3), meta.clone(), Bytes::from_static(b"blob"));
    report.require(
      "engine/snapshot-is-visible-at-submit",
      stable.snapshot() == Some((meta.clone(), Bytes::from_static(b"blob"))),
      std::format!(
        "a submitted snapshot is readable for serving before it is durable, and it is the meta \
         and bytes submitted VERBATIM — `is_some` accepted a slot holding another boundary \
         entirely. Got {:?}",
        stable.snapshot()
      ),
    );
  }
  report.require(
    "engine/has-staged-reports-owed-barriers",
    engine.has_staged(),
    "work submitted since the last barrier means another barrier is owed",
  );
  // ALWAYS DRAIN, and judge whatever comes out where the claim is made. An engine that releases
  // immediately is not exempt from the durability rule — it is the one the rule is about.
  let staging = {
    let (log, stable) = engine.stores(&a).expect("group a is hosted");
    !log.has_pending() && !stable.has_pending()
  };
  let (a_log_early, a_stable_early) = drain_validating::<S>(
    &mut engine,
    &a,
    report,
    &[(OpId::new(1), Index::new(3))],
    &[(OpId::new(2), written.clone())],
    &[(OpId::new(3), meta.clone())],
  );
  {
    let (_, stable) = engine.stores(&a).expect("group a is hosted");
    // Per class, exactly as the stable suite: absent a completion, the reader must still show what
    // a crash would leave.
    if !a_stable_early.contains(&StableDone::Wrote(OpId::new(2))) {
      report.require(
        "engine/hard-state-is-last-durable",
        stable.hard_state() == HardState::initial(),
        format!(
          "hard_state() moved to {:?} before the barrier that makes it durable",
          stable.hard_state()
        ),
      );
    }
    if !a_stable_early.contains(&StableDone::SnapshotWritten(OpId::new(3))) {
      report.require(
        "engine/durable-snapshot-is-never-the-visible-slot",
        stable.durable_snapshot().is_none(),
        "durable_snapshot() must not serve a blob the barrier has not covered",
      );
    }
  }
  // has_pending() is the driver's ONLY signal that polling is worth doing, so BOTH engine shapes
  // owe the same agreement: whatever it reported before the drain is what the drain then produced.
  // Requiring a literal `true` here recorded a pass no store could ever lose.
  report.require(
    "engine/staged-work-is-invisible-to-has-pending",
    staging == (a_log_early.is_empty() && a_stable_early.is_empty()),
    format!(
      "has_pending() read {} before the barrier, yet the drain that followed produced {} log and \
       {} stable completion(s). Reading false with completions queued strands them until unrelated \
       work happens to wake the driver; reading true with nothing to deliver spins it",
      !staging,
      a_log_early.len(),
      a_stable_early.len()
    ),
  );
  if staging {
    report.require(
      "engine/barrier-releases-nothing-early",
      a_log_early.is_empty() && a_stable_early.is_empty(),
      "no completion may be released before the barrier that makes its write durable",
    );
  } else {
    report.skip(
      "engine/barrier-releases-nothing-early",
      "the engine completes at submit; what it releases early is judged at consumption instead",
    );
  }

  // ATOMICITY. Work staged across three groups plus a lineage record is ONE barrier: a single
  // flush releases all of it, and nothing is owed afterwards.
  {
    let (log, _) = engine.stores(&b).expect("group b is hosted");
    log.submit_append(OpId::new(4), &run(1, 1, 1));
  }
  {
    let (_, stable) = engine.stores(&c).expect("group c is hosted");
    stable.submit_write(OpId::new(5), written.clone());
  }
  engine.set_group_floor(&a, 5);
  report.require(
    "engine/lineage-reads-freshest",
    FloorStore::floor(&engine, &a) == 5,
    format!(
      "a floor written this crank must fence before its barrier lands, got {}",
      FloorStore::floor(&engine, &a)
    ),
  );
  // THE GENERATION TOO. Only the floor was read while staged, so an engine that fences on the
  // freshest floor and the last DURABLE generation admitted a restore the staged record forbids.
  // Written on a group whose removal ceiling nothing else asserts, so the read does not move the
  // fixture the ceiling checks are built on.
  set_gen_checked::<S>(&mut engine, &c, 9, report);
  report.require(
    "engine/lineage-reads-freshest",
    FloorStore::lineage(&engine, &c) == 9,
    format!(
      "a generation written this crank must read back before its barrier lands, got {}",
      FloorStore::lineage(&engine, &c)
    ),
  );
  let released = engine.flush();
  report.require(
    "engine/barrier-releases-every-group-at-once",
    released >= 5,
    format!(
      "one barrier must release every group's staged work and the lineage record with it; it \
       reported {released} completions for 5 staged writes"
    ),
  );
  report.require(
    "engine/has-staged-reports-owed-barriers",
    !engine.has_staged(),
    "after a barrier nothing is staged and no further barrier is owed",
  );
  // A LINEAGE-ONLY STAGE. Every earlier stage carried data beside the record, so an engine that
  // tracks "work owed" by its store queues alone answered correctly throughout — and then let a
  // bare floor write sit unflushed with nothing to tell the driver a barrier was owed.
  {
    let barriers_before = engine.barriers();
    let ops_before = engine.ops_batched();
    engine.set_group_floor(&c, 4);
    report.require(
      "engine/has-staged-reports-owed-barriers",
      engine.has_staged(),
      "a lineage record written with no data beside it still owes a barrier",
    );
    let released = engine.flush();
    // THE BATCH METRIC, which nothing else in this suite reads: a constant zero satisfies both
    // `#[must_use]` methods, and the driver sizes its batches on them.
    report.require(
      "engine/batch-metrics-count-every-barrier",
      engine.barriers() == barriers_before + 1
        && engine.ops_batched() == ops_before + released as u64,
      std::format!(
        "one flush advances barriers() by exactly one and ops_batched() by the completions it \
         reported: barriers {} → {} and ops {} → {} for {released} released",
        barriers_before,
        engine.barriers(),
        ops_before,
        engine.ops_batched()
      ),
    );
  }
  // EVERY GROUP'S HALF, not just the first one's. The claim this check makes is atomicity ACROSS
  // groups, so an oracle that reads one group's completions and infers the rest certifies exactly
  // the engine it is meant to catch: one that barriers the group it happens to visit first and
  // leaves another's staged work behind.
  // Completions consumed BEFORE the barrier count here: an engine settling at submit owes the same
  // one completion a staging engine owes at its barrier, and folding them in is what keeps this
  // accounting honest for both shapes.
  let (mut a_log_done, mut a_stable_done) = (a_log_early, a_stable_early);
  {
    // Validated at consumption, exactly as the pre-barrier drain was. An engine that stages until
    // its barrier makes ALL of its claims here, so leaving this drain unjudged left the release
    // path that actually happens — and with it the log's only durable reader — unexercised.
    let (log_late, stable_late) = drain_validating::<S>(
      &mut engine,
      &a,
      report,
      &[(OpId::new(1), Index::new(3))],
      &[(OpId::new(2), written.clone())],
      &[(OpId::new(3), meta.clone())],
    );
    a_log_done.extend(log_late);
    a_stable_done.extend(stable_late);
  }
  let (b_log_done, b_stable_done) = drain_both::<S>(&mut engine, &b, report);
  let (c_log_done, c_stable_done) = drain_both::<S>(&mut engine, &c, report);
  // EXACT MULTISETS, not membership. `contains` accepted a duplicate — the core folds each
  // completion into a durability watermark — and accepted an extra completion for an operation
  // that was never staged.
  {
    let once = |done: &[LogDone], want: LogDone| {
      done.len() == 1 && done.iter().filter(|d| **d == want).count() == 1
    };
    let stable_once = |done: &[StableDone], want: &[StableDone]| {
      done.len() == want.len()
        && want
          .iter()
          .all(|w| done.iter().filter(|d| *d == w).count() == 1)
    };
    let exact = once(&a_log_done, LogDone::Appended(OpId::new(1)))
      && stable_once(
        &a_stable_done,
        &[
          StableDone::Wrote(OpId::new(2)),
          StableDone::SnapshotWritten(OpId::new(3)),
        ],
      )
      && once(&b_log_done, LogDone::Appended(OpId::new(4)))
      && stable_once(&c_stable_done, &[StableDone::Wrote(OpId::new(5))])
      && b_stable_done.is_empty()
      && c_log_done.is_empty();
    report.require(
      "engine/barrier-releases-every-groups-completions",
      exact,
      std::format!(
        "one barrier releases each staged operation exactly once and nothing else. Got \
         a={a_log_done:?}/{a_stable_done:?}, b={b_log_done:?}/{b_stable_done:?}, \
         c={c_log_done:?}/{c_stable_done:?}"
      ),
    );
  }
  let mut missing: Vec<&str> = Vec::new();
  if !a_log_done.contains(&LogDone::Appended(OpId::new(1))) {
    missing.push("group a's append");
  }
  if !a_stable_done.contains(&StableDone::Wrote(OpId::new(2))) {
    missing.push("group a's hard-state write");
  }
  if !a_stable_done.contains(&StableDone::SnapshotWritten(OpId::new(3))) {
    missing.push("group a's snapshot");
  }
  if !b_log_done.contains(&LogDone::Appended(OpId::new(4))) {
    missing.push("group b's append");
  }
  if !c_stable_done.contains(&StableDone::Wrote(OpId::new(5))) {
    missing.push("group c's hard-state write");
  }
  report.require(
    "engine/barrier-releases-every-groups-completions",
    missing.is_empty(),
    format!(
      "one barrier spans every hosted group, and these halves of it never released: {missing:?}.        Got a={a_log_done:?}/{a_stable_done:?}, b={b_log_done:?}/{b_stable_done:?},        c={c_log_done:?}/{c_stable_done:?}"
    ),
  );
  {
    let (_, stable) = engine.stores(&a).expect("group a is hosted");
    report.require(
      "engine/hard-state-advances-at-the-barrier",
      stable.hard_state() == written,
      "the barrier advances the durable hard state and the completions together",
    );
    report.require(
      "engine/durable-snapshot-advances-at-the-barrier",
      stable.durable_snapshot().as_ref() == Some(&meta),
      "the barrier advances the durable snapshot slot, and the meta it advances onto must be the \
       submitted one VERBATIM — an identity match alone would accept a slot whose lease windows, \
       read mode or provenance were rebuilt from defaults",
    );
  }

  // The removal ceiling is EXACT: it folds what SURVIVES, and retracts with it. Rounding up costs
  // a never-reshaped id its rejoin at one end and forges the terminal floor at the other.
  report.require(
    "engine/removal-ceiling-is-zero-for-an-unreshaped-id",
    engine.removal_floor(&b) == 0,
    format!(
      "an id that never reshaped must floor at 0 so its gen-0 rejoin survives, got {}",
      engine.removal_floor(&b)
    ),
  );
  report.require(
    "engine/removal-ceiling-folds-the-snapshot-meta",
    engine.removal_floor(&a) == TOP_WORKING_GENERATION - 7,
    format!(
      "a snapshot meta claiming generation {} must floor a removal one past it, got {}",
      TOP_WORKING_GENERATION - 8,
      engine.removal_floor(&a)
    ),
  );
  {
    let (_, stable) = engine.stores(&a).expect("group a is hosted");
    stable.submit_snapshot(
      OpId::new(6),
      SnapshotMeta::new(
        Index::new(3),
        Term::new(1),
        ConfState::from_voters([node.clone()]),
      )
      .with_shape_gen(TOP_WORKING_GENERATION - 10),
      Bytes::from_static(b"blob2"),
    );
  }
  engine.flush();
  {
    let (log, stable) = engine.stores(&a).expect("group a is hosted");
    drain_log(log, report);
    drain_stable(stable, report);
  }
  report.require(
    "engine/removal-ceiling-retracts-with-the-slot",
    engine.removal_floor(&a) == TOP_WORKING_GENERATION - 9,
    format!(
      "the snapshot slot now claims generation {}, so the ceiling must be {}; it answered {}. The \
       meta leg is REPLACED with the slot rather than accumulated — a displaced meta stops \
       counting exactly when it stops being what a reader would find. Rounding up is not a safe \
       shortcut: at the top of the working range the saturating +1 would forge the reserved \
       terminal floor",
      TOP_WORKING_GENERATION - 10,
      TOP_WORKING_GENERATION - 9,
      engine.removal_floor(&a)
    ),
  );

  match subject.shape_entry(Term::new(1), Index::new(4), TOP_WORKING_GENERATION - 4) {
    Some(shape) => {
      {
        let (log, _) = engine.stores(&a).expect("group a is hosted");
        log.submit_append(OpId::new(7), core::slice::from_ref(&shape));
      }
      let with_shape = engine.removal_floor(&a);
      {
        let (log, _) = engine.stores(&a).expect("group a is hosted");
        log.submit_append(OpId::new(8), &run(2, 4, 4));
      }
      let after_truncation = engine.removal_floor(&a);
      engine.flush();
      report.require(
        "engine/removal-ceiling-folds-a-shape-entry",
        with_shape == TOP_WORKING_GENERATION - 3,
        format!(
          "a shape entry naming generation {} must floor a removal at {}, got {with_shape}",
          TOP_WORKING_GENERATION - 4,
          TOP_WORKING_GENERATION - 3
        ),
      );
      report.require(
        "engine/removal-ceiling-retracts-a-truncated-shape-entry",
        after_truncation == TOP_WORKING_GENERATION - 9,
        format!(
          "a shape entry truncated away never survived, so the ceiling falls back to what the \
           snapshot slot claims — generation {}, so a ceiling of {}. It answered \
           {after_truncation}. `< {with_shape}` accepted any retraction at all, including one past \
           the slot",
          TOP_WORKING_GENERATION - 10,
          TOP_WORKING_GENERATION - 9
        ),
      );
    }
    None => {
      report.skip(
        "engine/removal-ceiling-folds-a-shape-entry",
        "the subject cannot mint a lineage-bearing log entry, so the shape-entry leg of the \
         ceiling is unproven for it",
      );
      report.skip(
        "engine/removal-ceiling-retracts-a-truncated-shape-entry",
        "the subject cannot mint a lineage-bearing log entry",
      );
    }
  }

  // THE TOP OF THE WORKING RANGE, on an id of its own so the fixture chain above is untouched.
  // Every ceiling leg above is exact but none of them is near the boundary, and the boundary is
  // where the fold's arithmetic stops being free: a fence at the highest generation an id can hold
  // must land ON `HIGHEST_WORKING_GENERATION` and never round up to `MERGED_FLOOR`. That sentinel
  // is read as a GLOBAL proof that the lineage was absorbed away, so an ordinary LOCAL removal
  // forging one would clear a live thaw obligation on every replica and strand a still-frozen
  // source forever.
  //
  // Carried by a snapshot meta rather than the optional shape entry: the meta is a carrier every
  // subject has, and a check that skips on the seam nobody implements would prove nothing here of
  // all places. The shape entry rides along wherever a subject offers one.
  {
    let peak = subject.group(8);
    engine.add_group(peak.clone());
    {
      let (log, stable) = engine.stores(&peak).expect("just admitted");
      stable.submit_snapshot(
        OpId::new(11),
        SnapshotMeta::new(
          Index::new(1),
          Term::new(1),
          ConfState::from_voters([node.clone()]),
        )
        .with_shape_gen(TOP_WORKING_GENERATION),
        Bytes::from_static(b"peak"),
      );
      if let Some(shape) = subject.shape_entry(Term::new(1), Index::new(1), TOP_WORKING_GENERATION)
      {
        log.submit_append(OpId::new(12), core::slice::from_ref(&shape));
      }
    }
    engine.flush();
    drain_group::<S>(&mut engine, &peak, report);
    let hosted_ceiling = engine.removal_floor(&peak);
    engine.remove_group(&peak);
    engine.flush();
    let ceiling = engine.removal_floor(&peak);
    report.require(
      "engine/removal-ceiling-never-reaches-the-terminal",
      ceiling == HIGHEST_WORKING_GENERATION
        && ceiling != MERGED_FLOOR
        && hosted_ceiling == HIGHEST_WORKING_GENERATION,
      format!(
        "an id at the highest working generation ({}) must fence its removal at {} — it answered \
         {ceiling} after the removal and {hosted_ceiling} while still hosted. {} is the reserved \
         terminal: a floor there is a cluster-wide verdict that the lineage was absorbed away, not \
         something a local removal may write",
        TOP_WORKING_GENERATION, HIGHEST_WORKING_GENERATION, MERGED_FLOOR
      ),
    );
  }

  // A fence exists to outlive the group it fences.
  set_gen_checked::<S>(&mut engine, &a, TOP_WORKING_GENERATION - 6, report);
  engine.flush();
  let floor_before = FloorStore::floor(&engine, &a);
  let lineage_before = FloorStore::lineage(&engine, &a);
  let ceiling_before = engine.removal_floor(&a);
  report.require(
    "engine/removal-reports-absence",
    engine.remove_group(&a),
    "removing a hosted group reports true",
  );
  engine.flush();
  report.require(
    "engine/lineage-outlives-remove-group",
    !engine.contains_group(&a)
      && FloorStore::floor(&engine, &a) == floor_before
      && FloorStore::lineage(&engine, &a) == lineage_before
      && engine.removal_floor(&a) == ceiling_before
      && engine.removal_floor(&a) < sailing_proto::MERGED_FLOOR,
    format!(
      "after remove_group the id's fence must stand: floor {} (was {floor_before}), lineage {} \
       (was {lineage_before}), ceiling {} (was {ceiling_before})",
      FloorStore::floor(&engine, &a),
      FloorStore::lineage(&engine, &a),
      engine.removal_floor(&a)
    ),
  );
  engine.add_group(a.clone());
  engine.flush();
  // ALL THREE FENCE READERS, and the stores themselves. Only the floor was re-read, so an
  // add_group that re-attached the removed id's old log and stable state — the shape a
  // directory-keyed engine falls into — passed while serving a recreated incarnation the previous
  // one's entries.
  let (reborn_log, reborn_stable) = match engine.stores(&a) {
    Some((log, stable)) => (
      (log.first_index(), log.last_index()),
      (stable.hard_state(), stable.snapshot().is_some()),
    ),
    None => ((Index::new(1), Index::ZERO), (HardState::initial(), false)),
  };
  report.require(
    "engine/re-admission-does-not-clear-the-fence",
    FloorStore::floor(&engine, &a) == floor_before
      && FloorStore::lineage(&engine, &a) == lineage_before
      && engine.removal_floor(&a) == ceiling_before,
    std::format!(
      "re-creating storage for a removed id must not reset the fence it was removed under: floor \
       {} (was {floor_before}), lineage {} (was {lineage_before}), ceiling {} (was \
       {ceiling_before})",
      FloorStore::floor(&engine, &a),
      FloorStore::lineage(&engine, &a),
      engine.removal_floor(&a)
    ),
  );
  report.require(
    "engine/re-admission-lends-empty-stores",
    reborn_log == (Index::new(1), Index::ZERO) && reborn_stable == (HardState::initial(), false),
    std::format!(
      "the re-admitted id came back holding log {reborn_log:?} and stable state \
       {reborn_stable:?}. remove_group destroyed that storage; handing it back to the next \
       incarnation serves a recreated group the entries of the one it replaced"
    ),
  );

  // Monotonicity: a belated lower write can never soften a fence, which is what makes the freshest
  // read safe.
  engine.set_group_floor(&a, 1);
  set_gen_checked::<S>(&mut engine, &a, 1, report);
  engine.flush();
  report.require(
    "engine/lineage-is-monotone",
    FloorStore::floor(&engine, &a) == floor_before
      && FloorStore::lineage(&engine, &a) == lineage_before,
    format!(
      "a lower write must not lower the fence: floor {} lineage {}",
      FloorStore::floor(&engine, &a),
      FloorStore::lineage(&engine, &a)
    ),
  );

  // Barrier survival of the record BESIDE the data it describes.
  let d = subject.group(4);
  engine.add_group(d.clone());
  {
    let (log, _) = engine.stores(&d).expect("group d is hosted");
    log.submit_append(OpId::new(9), &run(1, 1, 2));
  }
  engine.set_group_floor(&d, 3);
  let before_floor = FloorStore::floor(&engine, &d);
  engine.flush();
  report.require(
    "engine/lineage-fold-rides-the-data-barrier",
    FloorStore::floor(&engine, &d) == before_floor && before_floor == 3,
    format!(
      "the barrier that made the data durable must have folded the record describing it; the \
       floor read {before_floor} before the barrier and {} after",
      FloorStore::floor(&engine, &d)
    ),
  );

  // The TERMINAL floor is the durable tombstone: `MERGED_FLOOR` fences every generation, the
  // sentinel itself included, and it must fold and read back exactly like any other floor.
  let terminal = subject.group(5);
  engine.add_group(terminal.clone());
  engine.set_group_floor(&terminal, sailing_proto::MERGED_FLOOR);
  engine.flush();
  let fence = FloorStore::floor(&engine, &terminal);
  report.require(
    "engine/terminal-floor-folds-and-admits-nothing",
    fence == sailing_proto::MERGED_FLOOR
      && !sailing_proto::floor_admits(fence, 0)
      && !sailing_proto::floor_admits(fence, 7)
      && !sailing_proto::floor_admits(fence, sailing_proto::MERGED_FLOOR),
    format!(
      "a merged-away id's terminal floor read {fence} and must fence every generation, the \
       reserved sentinel included"
    ),
  );

  // The completion-fault battery, driven through the engine's OWN lent stores: a channel that
  // reorders, duplicates, delays, loses, or replays a dead incarnation's acknowledgment must leave
  // the durability probes exactly where the barriers put them.
  let battery = subject.group(6);
  engine.add_group(battery.clone());
  engine.flush();
  {
    let mut store_subject = LentStores::<S> {
      engine: &mut engine,
      gid: battery.clone(),
      node: node.clone(),
    };
    report.absorb(super::completion_faults_log(&mut store_subject));
  }
  {
    let mut store_subject = LentStores::<S> {
      engine: &mut engine,
      gid: battery,
      node,
    };
    report.absorb(super::completion_faults_stable(&mut store_subject));
  }

  // Boot epochs: strictly increasing, refused for an unhosted id, never wrapping.
  let mut epochs = Vec::new();
  for _ in 0..3 {
    epochs.push(engine.next_boot_epoch(&d));
  }
  report.require(
    "engine/boot-epoch-strictly-increases",
    epochs == std::vec![Some(1), Some(2), Some(3)],
    format!("the per-group boot epoch starts at 1 and strictly increases, got {epochs:?}"),
  );
  report.require(
    "engine/boot-epoch-refused-for-an-unhosted-id",
    engine.next_boot_epoch(&subject.group(99)).is_none(),
    "an unhosted group has no boot-epoch counter to advance",
  );

  // THE STAGING CAP IS A PROMISE ABOUT ALLOCATION. An engine that accepts the call and ignores it
  // leaves the embedder believing a hostile `total_len` is bounded, and the next transfer
  // declaring terabytes allocates them. Judged over a group admitted AFTER the call as well as
  // one admitted before, because the contract says every current AND future group.
  {
    const CAP: usize = 64;
    let late = subject.group(7);
    engine.set_snapshot_staging_cap(CAP);
    engine.add_group(late.clone());
    let boundary = SnapshotMeta::new(
      Index::new(9),
      Term::new(3),
      ConfState::from_voters([subject.node(1)]),
    );
    let chunk = Bytes::from_static(b"chunk");
    for (label, gid) in [
      ("a group admitted before the cap", &a),
      ("one admitted after", &late),
    ] {
      let Some((_, stable)) = engine.stores(gid) else {
        continue;
      };
      // One byte over — comfortably allocatable, so only the CAP can refuse it. A hostile
      // declaration the allocator would reject anyway proves nothing about the cap.
      let oversized = stable.accept_snapshot_chunk(&boundary, CAP as u64 + 1, 0, &chunk);
      let allowed = stable.accept_snapshot_chunk(&boundary, CAP as u64, 0, &chunk);
      report.require(
        "engine/staging-cap-refuses-an-oversized-transfer",
        oversized.is_err(),
        std::format!(
          "[{label}] a transfer declaring {} bytes was accepted under a {CAP}-byte staging cap. \
           The declaration comes from a PEER, so a cap that is not enforced is an allocation a \
           remote node chooses",
          CAP + 1
        ),
      );
      report.require(
        "engine/staging-cap-still-admits-what-fits",
        allowed.is_ok(),
        std::format!(
          "[{label}] a transfer declaring exactly the {CAP}-byte cap was refused. The check above \
           must not be passing by refusing everything"
        ),
      );
      stable.discard_snapshot_staging();
    }
    engine.remove_group(&late);
    engine.set_snapshot_staging_cap(usize::MAX);
    engine.flush();
  }

  // Posed only where an `Appended` is consumed AND the lent log offers the probe; naming the
  // absence keeps a store that answers neither from reading as covered.
  report.skip_if_unreached(
    "engine/durable-index-covers-a-released-append",
    "the lent log does not offer the optional durable_index probe, so a released `Appended` \
     cannot be measured against the engine's own durable reader at the moment it is consumed",
  );

  subject.crash(engine, CrashClass::Clean);
}

/// The crash half. Which law applies depends on what the engine CLAIMS: a durable engine must give
/// back exactly its barrier-covered state, a volatile one must give back nothing.
///
/// # The leg-by-gate matrix
///
/// What this loop grades depends on three things at once, and a name graded in one combination
/// while silently absent in another reads as covered without being asked. The combinations are
/// therefore enumerated rather than left to be derived: a column is one (crash class × boundary
/// knowledge × durability tier), and a cell says what happens to that name in that column.
///
/// | name | V | D-CL | D-TB | D-TN | D-TU |
/// |---|---|---|---|---|---|
/// | `hosted-ids-lend-stores`                  | G   | G      | G | G | G  |
/// | `a-reopened-log-is-resident-and-readable` | G   | G      | G | G | G  |
/// | `reopen-manufactures-no-completions`      | S¹  | G      | G | G | G  |
/// | `reopened-durable-index-never-over-answers` | S² | G/S²  | G/S² | G/S² | G/S² |
/// | `reopened-durable-hard-state-agrees`      | S²  | G/S²   | G/S² | G/S² | G/S² |
/// | `boot-epoch-never-repeats-across-a-reopen` | U  | G      | G | G | G³ |
/// | `volatile-engine-keeps-nothing`           | G   | U      | U | U | U  |
/// | `exactly-the-maximal-valid-prefix-survives` | U | G      | G | G | D  |
/// | `a-clean-drop-keeps-what-the-barriers-covered` | U | G⁴ | U | U | U  |
/// | `exactly-flush-covered-state-survives`    | U   | G⁵     | U | U | U  |
/// | `barrier-is-all-or-nothing-across-a-crash` | U  | U      | G | G | G⁷ |
/// | `durability-precedes-the-barriers-return` | U   | G⁶     | G⁶ | G⁶ | D |
///
/// The D-TB / D-TU split is resolved PER LEG, from the boundaries that leg's own incarnation
/// reported — never from a capability sampled once before the sweep. A subject may not know its
/// medium's length until it has one to measure, so a single up-front `tail_len` answered `None`
/// and put every later leg in D-TU whatever it went on to report. The same run's numbers now
/// choose the column, aim the cut, and grade the image, so a subject can move between D-TB and
/// D-TU leg by leg and each leg is judged where it actually belongs.
///
/// Columns. **V** — a volatile subject, any class, any boundary. **D-CL** — durable, `Clean` or
/// `LoseUnsyncedWrites`; the boundary is irrelevant because no cut is made. **D-TB** — durable,
/// `TornTail`, with a boundary pair that strictly orders the two barriers. **D-TN** — durable,
/// `TornTail { keep_bytes: u64::MAX }`, the cut that reaches past the end and therefore removes
/// nothing; knowable WITHOUT a boundary, which is the whole reason it exists. **D-TU** — durable,
/// `TornTail` at a real offset with no usable boundary.
///
/// Cells. **G** graded. **S** skipped and allow-listed. **D** skip-DOMINATING: the name spans every
/// class, so a column that cannot ask it withdraws the pass a sibling column recorded — and a
/// FAILURE outranks the skip, which is what lets D-TN catch a defect whose name D-TU then reports
/// as uncovered. **U** structurally unreachable, and absent from that tier's `REQUIRED` list.
///
/// 1. A volatile reopen hosts nothing, so there is no completion queue to inspect
///    (`SKIPPABLE_VOLATILE`).
/// 2. Needs the matching optional probe; recorded once, after every leg, by `skip_if_unreached`.
/// 3. Graded wherever the reopen kept the group — at `keep_bytes` small enough to lose it there is
///    no id to ask about, and D-CL covers the name for every durable subject regardless.
/// 4. `Clean` only. 5. `LoseUnsyncedWrites` only. 6. `DrainPolicy::NoDrain` legs only.
/// 7. Graded WITHOUT the boundary: a barrier spans every hosted group at once, so whatever a crash
///    left is one of three COMPLETE states — nothing, everything through the first barrier,
///    everything through the second — and a mixture is none of them however the cut fell. Only
///    WHICH of the three it should be needs the layout, which is why the other two rows still
///    skip-dominate here.
///
/// The readability row is G everywhere on purpose: whether a hosted reopen can serve its own log
/// needs no image and no boundary, so it is graded before `expected_image` is consulted at all.
/// Folding an unreadable read into "no entries" put it inside the image instead, where the D-TU
/// column never looks.
///
/// Two durable legs sit OUTSIDE this sweep because the sweep's own shape hides what they ask.
/// `an-append-acknowledged-before-a-barrier-survives` needs a crash while a completion is
/// outstanding and no barrier has run; `an-issued-epoch-survives-an-unflushed-crash` needs one
/// after epochs are taken and before any flush — and every leg of the sweep reaches a flush after
/// taking its own. Both are graded once, for the durable tier, in every boundary column.
///
/// THE INVARIANT: every name on the tier's `REQUIRED` manifest is G, S or D in at least one column
/// that tier reaches — never U everywhere. `Report::require_coverage` enforces it per report, and
/// `the_crash_matrix_columns_are_each_covered` runs one subject per column to prove each column is
/// actually reachable rather than merely tabulated.
fn crash_half<S>(subject: &mut S, report: &mut Report)
where
  S: EngineSubject,
  <EngineLogOf<S> as LogStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::Error: core::fmt::Debug,
  S::NodeId: Clone,
{
  let a = subject.group(1);
  let b = subject.group(2);
  let absent = (GroupImage::<S::NodeId>::absent(), GroupImage::absent());
  let mut unsettled_legs = false;

  // WHERE EACH CUT LANDS IS RESOLVED PER LEG, against the medium THAT leg's own incarnation
  // wrote — and so is WHICH KIND of cut it is. Learning the boundaries once from a separate probe
  // engine assumed byte offsets are stable across opens; asking `tail_len` once before the loop
  // then assumed the SHAPE of its answer is stable too. Nothing promises either. A subject that
  // does not know its medium's length until it has one to measure answered `None` to that single
  // question and was swept with fixed offsets for the rest of the run, so every cut landed before
  // the first barrier or past the last, and a barrier surviving in halves between them was never
  // aimed at.
  //
  // One list of landmarks now, each carrying its own fallback offset for a leg whose boundaries
  // really are unusable. The leg's own numbers decide which it gets.
  let cuts = [
    Cut::Zero,
    Cut::HalfOfFirst,
    Cut::JustBeforeFirst,
    Cut::AtFirst,
    Cut::BetweenBarriers,
    Cut::JustBeforeSecond,
    Cut::AtSecond,
    Cut::JustPastSecond,
    // INSIDE THE UN-BARRIERED TAIL: the ordinary shape of a crash, and every landmark above stops
    // at the last barrier. Its fallback is the cut that reaches past the end and removes NOTHING —
    // the one torn leg whose survivors are knowable without a boundary at all.
    Cut::WellPastSecond,
  ];
  let mut legs = std::vec![Leg::Clean, Leg::Unsynced];
  legs.extend(cuts.into_iter().map(Leg::Torn));

  for drain in [
    DrainPolicy::NoDrain,
    DrainPolicy::Partial,
    DrainPolicy::Full,
  ] {
    for leg in legs.clone() {
      let mut engine = subject.open();
      let (len1, len2, issued_before) = write_scenario(subject, &mut engine, drain, report);
      // A pair that does not strictly ORDER the two barriers cannot aim anything: equal lengths,
      // or a first barrier at offset zero, put every cut at or above both, so every torn leg would
      // be graded "keep everything" — the one answer no engine can fail. Such a pair takes the
      // unknown-boundary path, which states in the report which legs it cannot settle.
      let (len1, len2) = match (len1, len2) {
        (Some(one), Some(two)) if two <= one || one == 0 => (None, None),
        pair => pair,
      };
      let class = leg.class(len1, len2);
      let mut reopened = subject.crash(engine, class);
      let tag = std::format!("{class:?}/{}", drain.label());

      // A reopened store owes acknowledgments to nobody: the incarnation that submitted this work
      // is gone, and so is every op id it minted.
      check_no_manufactured_completions::<S>(&mut reopened, subject, report, &tag);
      check_reopened_probes::<S>(&mut reopened, subject, report, &tag);

      let (image_a, read_a) = read_image::<S>(&mut reopened, &a, report, &tag);
      let (image_b, read_b) = read_image::<S>(&mut reopened, &b, report, &tag);
      // GRADED ON ITS OWN, before any image expectation. A hosted group whose log will not answer
      // is a replica the core poisons or wedges on, and that is knowable without knowing which
      // barriers survived — so it must not ride on an image the boundary may make unknowable.
      for (n, read) in [(1u64, read_a), (2, read_b)] {
        report.require(
          "engine/a-reopened-log-is-resident-and-readable",
          matches!(read, LogRead::Absent | LogRead::Resident),
          std::format!(
            "[{tag}] group {n} came back hosted with a log that answered {read:?}. A reopen hands \
             the core a replica it drives immediately; one whose entries it cannot read is a \
             poisoned endpoint, not a recovery"
          ),
        );
      }
      let seen = (image_a, image_b);
      // BEFORE ANY IMAGE EXPECTATION, and on EVERY leg. Which barriers survive a torn cut is
      // unknowable without the medium's boundary; which epoch the reopen hands out is not. Judged
      // inside the image arm, the legs that cannot settle an image would skip this too — and an
      // engine that rolls its counter back only after a torn crash would go unasked while the clean
      // and unsynced-loss legs left the name reading as passed. Such an engine reissues an op-id
      // epoch, and a prior incarnation's retained completion then aliases a live write and releases
      // the gate fencing it.
      //
      // `read_image` already took the reopen's FIRST epoch, above; taking another here would
      // compare the second one and miss a counter that resets and then advances.
      if subject.durability() == Durability::Durable && reopened.contains_group(&a) {
        let after = reopened.next_boot_epoch(&a);
        report.require(
          "engine/boot-epoch-never-repeats-across-a-reopen",
          matches!((issued_before, after), (Some(x), Some(y)) if y > x),
          std::format!(
            "[{tag}] the last epoch the crashed incarnation handed out was {issued_before:?} and \
             the first the reopen hands out is {after:?}; an epoch handed out twice folds two \
             incarnations onto one identity for every gen-keyed observer"
          ),
        );
      }
      // AGAIN, after the reads and a barrier of its own. A single drain at the moment of reopen
      // passes for an engine that reconstructs its queues lazily — on the first read, or on the
      // first flush — and hands the new incarnation acknowledgments the crashed one's op ids
      // belong to.
      check_no_manufactured_completions::<S>(&mut reopened, subject, report, &tag);
      reopened.flush();
      check_no_manufactured_completions::<S>(&mut reopened, subject, report, &tag);
      // The two membership readers must agree: an id `contains_group` claims is hosted and
      // `stores` will not lend is one the core drives with no storage at all.
      for n in 1..=2u64 {
        let gid = subject.group(n);
        let hosted = reopened.contains_group(&gid);
        let lends = reopened.stores(&gid).is_some();
        report.require(
          "engine/hosted-ids-lend-stores",
          hosted == lends,
          std::format!(
            "[{tag}] group {n} reopened with contains_group() {hosted} and stores() {lends}. The \
             core admits an id on the first and drives it through the second"
          ),
        );
      }
      match subject.durability() {
        Durability::Volatile => report.require(
          "engine/volatile-engine-keeps-nothing",
          seen == absent,
          std::format!(
            "[{tag}] the reopened engine held {seen:?}. An engine whose state dies with it must \
             come back empty; appearing to recover state would report durability it does not have"
          ),
        ),
        Durability::Durable => {
          let Some((expected, name)) = expected_image::<S>(subject, class, drain, len1, len2)
          else {
            // ATOMICITY IS STILL DECIDABLE. Which barriers a cut left behind needs the boundary;
            // whether what survived is a WHOLE barrier does not. A barrier spans every hosted
            // group at once, so a crash leaves one of exactly three complete states — nothing,
            // everything through the first, everything through the second — and a mixture is none
            // of them however the cut landed. Discarding the entire image here threw that away
            // along with the part that really does need the offset.
            let allowed = [
              (absent.clone(), "pre-first-barrier"),
              (after_first::<S>(subject), "first-barrier"),
              (after_second::<S>(subject), "second-barrier"),
            ];
            report.require(
              "engine/barrier-is-all-or-nothing-across-a-crash",
              allowed.iter().any(|(image, _)| *image == seen),
              std::format!(
                "[{tag}] the reopened engine held\n  {seen:?}\nwhich is none of the three \
                 COMPLETE states a crash can leave: nothing, everything the first barrier covered, \
                 or everything the second did. A barrier spans every hosted group in one atomic \
                 unit, so half of one is a state no barrier ever produced — and that is decidable \
                 without knowing where the cut fell"
              ),
            );
            // NOTED, NOT RECORDED YET. Whether these names are askable at all is only known once
            // every leg has run: a later one may settle them. Skipping here, per leg, made the
            // same name both passed and skipped for a subject whose other legs did settle it —
            // the report then reads as covered while some legs were never asked.
            unsettled_legs = true;
            subject.crash(reopened, CrashClass::Clean);
            continue;
          };
          let holds = seen == expected;
          let detail = std::format!(
            "[{tag}] the reopened engine held\n  {seen:?}\nwhere the {name} state is\n  {expected:?}\n\
             A crash leaves the MAXIMAL VALID PREFIX of the barriers and nothing else: a lesser \
             image throws away work the engine acknowledged, a greater one resurrects work it \
             never made durable, and a MIXED one is half a barrier — the record and the data it \
             describes must reach the medium in one atomic unit, across every group the barrier \
             spans"
          );
          report.require(
            "engine/exactly-the-maximal-valid-prefix-survives",
            holds,
            &detail,
          );
          // The same comparison settles the claim each crash class exists to make, reported under
          // its own name so a failure says WHICH property broke.
          match class {
            CrashClass::LoseUnsyncedWrites => {
              report.require(
                "engine/exactly-flush-covered-state-survives",
                holds,
                &detail,
              );
            }
            CrashClass::TornTail { .. } => {
              report.require(
                "engine/barrier-is-all-or-nothing-across-a-crash",
                holds,
                &detail,
              );
            }
            CrashClass::Clean => {
              report.require(
                "engine/a-clean-drop-keeps-what-the-barriers-covered",
                holds,
                &detail,
              );
            }
          }
          if drain == DrainPolicy::NoDrain {
            // Durability precedes a barrier's RETURN, not its drain: nothing was polled here.
            report.require(
              "engine/durability-precedes-the-barriers-return",
              holds,
              &detail,
            );
          }
        }
      }

      subject.crash(reopened, CrashClass::Clean);
    }
  }

  // AN ISSUED EPOCH IS DURABLE WHEN IT IS ISSUED, not when the next barrier happens to arrive. The
  // column sweep above always reaches a flush after taking its epochs, so an engine that advances
  // the counter in memory and journals it with the next batch passes every leg of it — and then a
  // real crash in that window forgets the epoch and the reopen hands the same number out again,
  // which is the aliasing the counter exists to prevent. The admission is made durable first so the
  // group is still there to ask; the EPOCHS are what this crash is aimed at.
  if subject.durability() == Durability::Durable {
    let mut engine = subject.open();
    engine.add_group(a.clone());
    engine.flush();
    let mut highest = None;
    for _ in 0..3 {
      highest = engine.next_boot_epoch(&a).or(highest);
    }
    assert!(
      highest.is_some(),
      "the suite must actually take an epoch here, or the check below asks nothing"
    );
    // NO FLUSH. This is the window the sweep never leaves open.
    let mut reopened = subject.crash(engine, CrashClass::LoseUnsyncedWrites);
    let after = reopened.next_boot_epoch(&a);
    report.require(
      "engine/an-issued-epoch-survives-an-unflushed-crash",
      matches!((highest, after), (Some(x), Some(y)) if y > x),
      std::format!(
        "the engine handed out epochs through {highest:?}, then crashed with no barrier after \
         them, and the reopen hands out {after:?}. An epoch is used the moment it is issued, so it \
         owes a durable home of its own rather than the next batch's — one a crash forgets is one \
         handed out twice, and a dead incarnation's retained completion then sorts at an id a live \
         write mints"
      ),
    );
    subject.crash(reopened, CrashClass::Clean);
  }

  // AN ACKNOWLEDGMENT MADE BEFORE ANY BARRIER has no in-process auditor: the log's only durable
  // reader is the optional `durable_index` probe, and an engine that declines to offer one makes
  // a claim nothing can check while the process lives. The crash checks it instead — consume
  // whatever is released before the first barrier, then lose the unsynced writes and require
  // exactly what was acknowledged to have survived.
  if subject.durability() == Durability::Durable {
    let mut engine = subject.open();
    engine.add_group(a.clone());
    let entries = a_entries(1);
    let mut released = Vec::new();
    {
      let (log, _) = engine.stores(&a).expect("just admitted");
      log.submit_append(OpId::new(1), &entries);
      // THREE ARMS. `while let Some(Ok(_))` ends on an error exactly as it ends on `None`, so a
      // trailing one-shot fault terminates the drain invisibly and a later clean drain records the
      // pass under the same name.
      loop {
        match log.poll() {
          Some(Ok(done)) => released.push(done),
          Some(Err(_)) => {
            report.require(
              NO_SPURIOUS_ERROR,
              false,
              "the log reported a store fault while draining an early acknowledgment; nothing \
               here injects one",
            );
            break;
          }
          None => break,
        }
      }
    }
    if released.contains(&LogDone::Appended(OpId::new(1))) {
      let mut reopened = subject.crash(engine, CrashClass::LoseUnsyncedWrites);
      let survived = reopened.stores(&a).map(|(log, _)| {
        match log.entries(log.first_index()..log.last_index().next(), u64::MAX) {
          Ok(EntriesRead::Ready(view)) => view.to_vec(),
          _ => Vec::new(),
        }
      });
      report.require(
        "engine/an-append-acknowledged-before-a-barrier-survives",
        survived.as_deref() == Some(entries.as_slice()),
        std::format!(
          "the engine handed over an `Appended` before any barrier, then lost its unsynced writes \
           and came back holding {survived:?} where the acknowledged entries are {entries:?}. A \
           completion is the engine's OWN claim that the write is durable; released ahead of the \
           medium it releases every gate the core fences on it"
        ),
      );
      subject.crash(reopened, CrashClass::Clean);
    } else {
      report.skip(
        "engine/an-append-acknowledged-before-a-barrier-survives",
        "the engine released nothing before its first barrier, so it claimed no durability here",
      );
      subject.crash(engine, CrashClass::Clean);
    }
  }

  // AN UNASKABLE LEG DOMINATES. These three names each claim something about EVERY crash class,
  // and the clean and unsynced-loss legs settle them long before a torn one is attempted — so
  // suppressing the skip because a sibling leg passed laundered a pass for a property never asked
  // under any torn crash at all, which is the crash class the names exist for. Splitting them per
  // class is not the answer: each already HAS a per-class sibling
  // (`exactly-flush-covered-state-survives`, `a-clean-drop-keeps-what-the-barriers-covered`,
  // `barrier-is-all-or-nothing-across-a-crash`), and these are deliberately the broad names that
  // span the classes. A broad name is covered only when every class it spans was asked.
  if unsettled_legs {
    for check in UNVERIFIABLE_WITHOUT_A_BOUNDARY {
      report.skip_dominating(
        check,
        "the subject does not report the length of the device a torn tail cuts, so which barriers \
         survive a cut at that offset is unknowable to the suite — and a property claimed across \
         every crash class is not covered by the classes that could be asked. Implement \
         EngineSubject::tail_len to have these legs run",
      );
    }
  }

  // Each of these is posed per reopened group, so whether it can be posed at all is only known
  // once every leg has run. Naming the reason keeps the gap in the report instead of leaving the
  // check silently absent.
  report.skip_if_unreached(
    "engine/reopen-manufactures-no-completions",
    "no reopen hosted a group, so no completion queue survived to inspect",
  );
  report.skip_if_unreached(
    "engine/reopened-durable-index-never-over-answers",
    "no reopened group offered the optional durable_index probe",
  );
  report.skip_if_unreached(
    "engine/reopened-durable-hard-state-agrees",
    "no reopened group offered the optional durable_hard_state probe",
  );
}

/// WHERE a torn cut lands, named by the landmark it is aimed at rather than by a byte offset.
///
/// A byte offset is only meaningful against the medium it was measured on. Naming the landmark lets
/// every leg resolve its own cut against its OWN incarnation's boundaries, which is what a subject
/// with variable framing needs and what a subject with stable framing gets for free.
#[derive(Debug, Clone, Copy)]
enum Cut {
  Zero,
  HalfOfFirst,
  JustBeforeFirst,
  AtFirst,
  BetweenBarriers,
  JustBeforeSecond,
  AtSecond,
  JustPastSecond,
  WellPastSecond,
}

impl Cut {
  /// The offset this landmark names on a medium whose barriers ended at `one` and `two`.
  fn resolve(self, one: u64, two: u64) -> u64 {
    match self {
      Self::Zero => 0,
      Self::HalfOfFirst => one / 2,
      Self::JustBeforeFirst => one.saturating_sub(1),
      Self::AtFirst => one,
      Self::BetweenBarriers => one + (two - one) / 2,
      Self::JustBeforeSecond => two.saturating_sub(1),
      Self::AtSecond => two,
      Self::JustPastSecond => two + 1,
      Self::WellPastSecond => two + (two - one),
    }
  }

  /// The offset to use when a leg's own boundaries cannot order its two barriers. Each landmark
  /// gets a DISTINCT one, so a subject the suite can never measure still gets a spread rather than
  /// nine cuts at the same place — and the last of them reaches past every possible end, which is
  /// the only torn outcome knowable without a boundary.
  const fn fallback(self) -> u64 {
    match self {
      Self::Zero => 0,
      Self::HalfOfFirst => 1,
      Self::JustBeforeFirst => 16,
      Self::AtFirst => 64,
      Self::BetweenBarriers => 256,
      Self::JustBeforeSecond => 1_024,
      Self::AtSecond => 4_096,
      Self::JustPastSecond => 65_536,
      Self::WellPastSecond => u64::MAX,
    }
  }
}

/// One leg of the crash sweep: a class, with a torn cut still to be resolved against the run.
#[derive(Debug, Clone, Copy)]
enum Leg {
  Clean,
  Unsynced,
  Torn(Cut),
}

impl Leg {
  /// The crash this leg makes of a run whose barriers ended at `len1` and `len2`.
  fn class(self, len1: Option<u64>, len2: Option<u64>) -> CrashClass {
    match self {
      Self::Clean => CrashClass::Clean,
      Self::Unsynced => CrashClass::LoseUnsyncedWrites,
      Self::Torn(cut) => CrashClass::TornTail {
        // THE LEG'S OWN NUMBERS DECIDE. A pair that orders this run's barriers resolves the
        // landmark against it; one that does not falls back to this landmark's fixed offset.
        keep_bytes: match (len1, len2) {
          (Some(one), Some(two)) => cut.resolve(one, two),
          _ => cut.fallback(),
        },
      },
    }
  }
}

/// Both groups as the FIRST barrier left them — one of the three complete states a crash can leave.
fn after_first<S>(subject: &S) -> HostImage<S>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  (first_image::<S>(subject, 1), first_image::<S>(subject, 2))
}

/// Both groups as the SECOND barrier left them.
fn after_second<S>(subject: &S) -> HostImage<S>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  (second_image::<S>(subject, 1), second_image::<S>(subject, 2))
}

/// The checks a torn-tail leg can only settle once the subject names its medium's boundaries.
/// `barrier-is-all-or-nothing-across-a-crash` is deliberately NOT here: whether what survived is a
/// whole barrier is decidable from the image alone. These two are the claims that genuinely need
/// the cut's location — WHICH barrier survived, and whether an un-drained one did.
const UNVERIFIABLE_WITHOUT_A_BOUNDARY: [&str; 2] = [
  "engine/exactly-the-maximal-valid-prefix-survives",
  "engine/durability-precedes-the-barriers-return",
];

/// Both groups' images, as one crash's outcome — the unit every comparison is made in, because a
/// barrier spans them together.
type HostImage<S> = (
  GroupImage<<S as EngineSubject>::NodeId>,
  GroupImage<<S as EngineSubject>::NodeId>,
);

/// Which whole state a given crash must leave behind — the MAXIMAL VALID PREFIX of the barriers
/// the medium still holds, named for the report.
fn expected_image<S>(
  subject: &S,
  class: CrashClass,
  drain: DrainPolicy,
  len1: Option<u64>,
  len2: Option<u64>,
) -> Option<(HostImage<S>, &'static str)>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  // A clean drop and an unsynced-write loss both keep every barrier: the tail written PAST the last
  // one never reached the medium at all, because the engine had not been asked to put it there.
  let after_second = after_second::<S>(subject);
  let after_first = after_first::<S>(subject);
  let absent = (GroupImage::absent(), GroupImage::absent());
  let _ = drain;
  match class {
    CrashClass::Clean | CrashClass::LoseUnsyncedWrites => Some((after_second, "second-barrier")),
    // A CUT THAT REACHES PAST EVERY POSSIBLE OFFSET CUTS NOTHING, whatever the medium's length
    // turns out to be. Its survivors are knowable BY DEFINITION — everything both barriers wrote —
    // so this one torn leg is graded even for a subject that will not name its boundary. Ungraded
    // with the rest, it is where an engine can drop a hosted group and pass: no image is compared,
    // and the two membership readers agree with each other when both say "not hosted".
    CrashClass::TornTail {
      keep_bytes: u64::MAX,
    } => Some((after_second, "second-barrier")),
    CrashClass::TornTail { keep_bytes } => match (len1, len2) {
      (Some(one), Some(two)) => Some(if keep_bytes >= two {
        (after_second, "second-barrier")
      } else if keep_bytes >= one {
        (after_first, "first-barrier")
      } else {
        (absent, "pre-first-barrier")
      }),
      // NO EXPECTATION EXISTS. Which barriers a cut at `keep_bytes` leaves behind is a fact about
      // WHERE the records sit on the medium, and a subject that cannot report its boundaries has
      // not told the suite that. Guessing "empty" here would record a PASS for whichever engines
      // happen to lose everything and a FAILURE for the correct ones — an unverifiable seam
      // answered by assumption. It skips instead, which is the kit's own rule.
      _ => None,
    },
  }
}

/// Two groups' work interleaved across two barriers, then a tail written past the second — the
/// shape every crash check reads back. Returns the medium's length after each barrier.
///
/// Two groups, because a barrier spans EVERY group the engine hosts: an engine that frames its
/// record per group leaves one group at the new barrier and another at the old, and a single-group
/// scenario cannot see that at all.
///
/// # Why every value here is deliberately un-default
///
/// The reopened comparison is whole-value equality, so it discriminates only over fields that
/// actually DIFFER from what a lossy store would invent. Zero timestamps, a default
/// `lease_support`, an absent lineage token, default lease windows and read mode — each of those
/// is exactly what a store that drops the field produces, so a scenario built from them cannot
/// tell a faithful store from one that persists `(index, term, kind, bytes)` and nothing else.
/// Every field family below therefore carries a distinct non-default value, and a DIFFERENT one in
/// each barrier.
fn write_scenario<S>(
  subject: &S,
  engine: &mut S::Engine,
  drain: DrainPolicy,
  report: &mut Report,
) -> (Option<u64>, Option<u64>, Option<u64>)
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  let a = subject.group(1);
  let b = subject.group(2);
  engine.add_group(a.clone());
  engine.add_group(b.clone());
  // THE EPOCH THIS INCARNATION IS BORN WITH, taken here and nowhere else. An epoch is handed out
  // and used immediately, so an engine owes it a durable home of its own rather than the next
  // barrier's — which means the record for this one sits BENEATH every barrier the scenario then
  // writes, and any cut that keeps a barrier keeps it too. Taken after the barriers instead, a
  // torn cut could drop it and an honest reopen would reissue the same number.
  // TWO of them, and the LAST is what the reopen must exceed. An engine that rolls its durable
  // counter back to any value above the FIRST one reissues a number this incarnation already handed
  // out — after issuing 1 and 2, a reopen answering 2 is an ALIAS, not an advance. Taking the
  // maximum also keeps the rule independent of how many epochs the scenario happens to consume.
  let mut issued_before = None;
  for _ in 0..2 {
    issued_before = engine.next_boot_epoch(&a).or(issued_before);
  }

  {
    let (log, stable) = engine.stores(&a).expect("just admitted");
    log.submit_append(OpId::new(1), &a_entries(1));
    stable.submit_write(OpId::new(2), a_hard_state::<S>(subject, 1));
  }
  {
    let (log, stable) = engine.stores(&b).expect("just admitted");
    log.submit_append(OpId::new(3), &b_entries(1));
    let (meta, blob) = b_snapshot::<S>(subject, 1);
    stable.submit_snapshot(OpId::new(4), meta, blob);
  }
  engine.set_group_floor(&a, 5);
  engine.flush();
  let len1 = subject.tail_len();
  if drain != DrainPolicy::NoDrain {
    drain_group::<S>(engine, &a, report);
    drain_group::<S>(engine, &b, report);
  }

  {
    let (log, stable) = engine.stores(&a).expect("hosted");
    log.submit_append(OpId::new(5), &a_entries(2));
    stable.submit_write(OpId::new(6), a_hard_state::<S>(subject, 2));
  }
  {
    let (log, stable) = engine.stores(&b).expect("hosted");
    log.submit_append(OpId::new(7), &b_entries(2));
    let (meta, blob) = b_snapshot::<S>(subject, 2);
    stable.submit_snapshot(OpId::new(8), meta, blob);
  }
  set_gen_checked::<S>(engine, &a, 9, report);
  engine.set_group_floor(&b, 2);
  engine.flush();
  let len2 = subject.tail_len();
  if drain == DrainPolicy::Full {
    drain_group::<S>(engine, &a, report);
    drain_group::<S>(engine, &b, report);
  }

  // Past the last barrier: visible to this process, crash-losable by contract.
  {
    let (log, stable) = engine.stores(&a).expect("hosted");
    log.submit_append(
      OpId::new(9),
      &[rich_entry(3, 7, EntryKind::Normal, b"tail", 700)],
    );
    stable.submit_write(OpId::new(10), HardState::initial().with_term(Term::new(3)));
  }
  engine.set_group_floor(&a, 12);
  (len1, len2, issued_before)
}

/// An entry with every self-describing field set to a distinct non-zero value derived from `salt`.
fn rich_entry(term: u64, index: u64, kind: EntryKind, payload: &'static [u8], salt: u64) -> Entry {
  Entry::new(
    Term::new(term),
    Index::new(index),
    kind,
    Bytes::from_static(payload),
  )
  .with_timestamp(salt)
  .with_lease_window(salt * 3 + 1)
  .with_wall_timestamp(salt * 7 + 2)
}

/// Group A's entries for `barrier`. Distinct KINDS as well as distinct payloads and lease fields —
/// the kind byte is as droppable as any other, and a store that normalises everything to `Normal`
/// reopens with a log the core will replay differently.
///
/// The lineage-bearing kinds (`Split`, the merge trio) are deliberately absent: their payloads can
/// only be minted through a codec `sailing-proto` keeps crate-private, so an out-of-tree scenario
/// cannot build an honest one. [`EngineSubject::shape_entry`] is the seam for those.
fn a_entries(barrier: u8) -> Vec<Entry> {
  if barrier == 1 {
    std::vec![
      rich_entry(1, 1, EntryKind::Normal, b"a-one", 11),
      rich_entry(1, 2, EntryKind::Empty, b"", 23),
      rich_entry(1, 3, EntryKind::ConfChange, b"a-conf", 37),
    ]
  } else {
    std::vec![
      rich_entry(2, 4, EntryKind::SetReadMode, b"a-mode", 41),
      rich_entry(2, 5, EntryKind::Normal, b"a-two", 53),
      rich_entry(2, 6, EntryKind::ConfChange, b"a-conf-2", 67),
    ]
  }
}

/// Group B's entries for `barrier`.
fn b_entries(barrier: u8) -> Vec<Entry> {
  if barrier == 1 {
    std::vec![
      rich_entry(1, 1, EntryKind::Normal, b"b-one", 71),
      rich_entry(1, 2, EntryKind::ConfChange, b"b-conf", 83),
    ]
  } else {
    std::vec![rich_entry(2, 3, EntryKind::Empty, b"", 97)]
  }
}

/// A lineage token distinct per `salt` — the field whose loss makes a snapshot compare unequal to
/// itself and a restart read as a lineage mismatch.
fn token(salt: u64) -> ForkId {
  ForkId::new(
    Bytes::from_static(b"conformance-parent"),
    salt,
    Index::new(salt * 2),
    Term::new(salt + 1),
    Bytes::from_static(b"conformance-child"),
    salt + 3,
  )
}

/// Group A's hard state for `barrier`: a real vote, a RECORDED promise with a real floor, a lineage
/// token, and a nonzero FOUNDING GENERATION — the fields a structural-only persistence silently
/// drops.
///
/// The founding generation differs between the barriers even though production writes it as a
/// per-incarnation CONSTANT. That is deliberate and it is about the STORE, not the field: a store
/// that persists the value once and then serves the cached copy on every later write — an easy
/// optimisation to reach for, given the field never moves — is indistinguishable from a faithful
/// one unless the fixture moves it. What the store owes is to round-trip whatever it was handed.
fn a_hard_state<S>(subject: &S, barrier: u8) -> HardState<S::NodeId>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  if barrier == 1 {
    HardState::initial()
      .with_term(Term::new(1))
      .with_vote(Some(subject.node(2)))
      .with_commit(Index::new(1))
      .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(700))))
      .with_lineage(Some(token(5)))
      .with_founding_gen(43)
  } else {
    HardState::initial()
      .with_term(Term::new(2))
      .with_vote(Some(subject.node(3)))
      .with_commit(Index::new(3))
      .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(1_500))))
      .with_lineage(Some(token(9)))
      .with_founding_gen(61)
  }
}

/// Group B's snapshot for `barrier`: a JOINT configuration with every set populated, non-default
/// lease windows, an explicit read mode, a shape generation, and fork provenance.
fn b_snapshot<S>(subject: &S, barrier: u8) -> (SnapshotMeta<S::NodeId>, Bytes)
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  // `learners_next` is an OUTGOING-ONLY staged demotion, so node 5 must sit in the outgoing half
  // and out of the incoming one. A fixture that breaks that is not a configuration any cluster
  // could install, and a validating store would be falsely rejected by it.
  let joint = ConfState::new(
    [subject.node(1), subject.node(2)],
    [subject.node(3)],
    [subject.node(4), subject.node(5)],
    [subject.node(5)],
    true,
  );
  assert!(
    joint.is_valid(),
    "the joint fixture must be an installable configuration: {joint:?}"
  );
  if barrier == 1 {
    (
      SnapshotMeta::new(Index::new(1), Term::new(1), joint)
        .with_max_lease_window(1_234)
        .with_max_wall_plus_window(5_678)
        .with_max_unwalled_lease_window(9_012)
        .with_read_only(ReadOnlyOption::LeaseGuard)
        .with_shape_gen(4)
        .with_fork_id(token(13)),
      Bytes::from_static(b"b-snapshot-barrier-one"),
    )
  } else {
    (
      SnapshotMeta::new(Index::new(2), Term::new(2), joint)
        .with_max_lease_window(2_468)
        .with_max_wall_plus_window(1_357)
        .with_max_unwalled_lease_window(8_642)
        .with_read_only(ReadOnlyOption::LeaseBased)
        .with_shape_gen(6)
        .with_fork_id(token(17)),
      Bytes::from_static(b"b-snapshot-barrier-two"),
    )
  }
}

/// Drain a group's completions and validate each ONE AT THE MOMENT IT IS CONSUMED against what its
/// operation claimed.
///
/// Gating the pre-barrier checks on "both queues are empty" asks nothing at all of an engine that
/// releases completions immediately: it skips the block, makes its writes durable during the flush,
/// and every later drain finds exactly the ids expected. A completion is the engine's own claim that
/// its write is durable, so the matching reader must already agree the moment it is handed over —
/// the same discipline the store suites carry, applied where the engine lends the stores.
fn drain_validating<S>(
  engine: &mut S::Engine,
  gid: &S::Group,
  report: &mut Report,
  appended: &[(OpId, Index)],
  wrote: &[(OpId, HardState<S::NodeId>)],
  snapshots: &[(OpId, SnapshotMeta<S::NodeId>)],
) -> (Vec<LogDone>, Vec<StableDone>)
where
  S: EngineSubject,
{
  let mut log_done = Vec::new();
  let mut stable_done = Vec::new();
  let Some((log, stable)) = engine.stores(gid) else {
    return (log_done, stable_done);
  };
  loop {
    let done = match log.poll() {
      Some(Ok(done)) => done,
      Some(Err(_)) => {
        report.require(
          NO_SPURIOUS_ERROR,
          false,
          "the log reported a store fault during a conforming sequence; nothing injects one",
        );
        break;
      }
      None => break,
    };
    {
      if let LogDone::Appended(id) = done
        && let Some((_, upto)) = appended.iter().find(|(op, _)| *op == id)
        && let Some(answer) = log.durable_index()
      {
        report.require(
          "engine/durable-index-covers-a-released-append",
          answer >= *upto,
          std::format!(
            "an `Appended` for an append reaching {upto:?} claims the whole prefix through it is \
           durable, yet at the moment it was consumed durable_index() answered {answer:?}"
          ),
        );
      }
      log_done.push(done);
    }
  }
  loop {
    let done = match stable.poll() {
      Some(Ok(done)) => done,
      Some(Err(_)) => {
        report.require(
          NO_SPURIOUS_ERROR,
          false,
          "the stable store reported a fault during a conforming sequence; nothing injects one",
        );
        break;
      }
      None => break,
    };
    {
      match done {
        StableDone::Wrote(id) => {
          if let Some((_, expected)) = wrote.iter().find(|(op, _)| *op == id) {
            report.require(
              "engine/hard-state-is-last-durable",
              stable.hard_state() == *expected,
              std::format!(
                "a `Wrote` completion claims this write is DURABLE, yet at the moment it was \
               consumed hard_state() read {:?}",
                stable.hard_state()
              ),
            );
          }
        }
        StableDone::SnapshotWritten(id) => {
          if let Some((_, expected)) = snapshots.iter().find(|(op, _)| *op == id) {
            report.require(
              "engine/durable-snapshot-is-never-the-visible-slot",
              stable.durable_snapshot().as_ref() == Some(expected),
              std::format!(
                "a `SnapshotWritten` completion claims this blob is DURABLE, yet at the moment it \
               was consumed durable_snapshot() read {:?}",
                stable.durable_snapshot()
              ),
            );
          }
        }
        _ => {}
      }
      stable_done.push(done);
    }
  }
  // Recorded on the CLEAN path too, in THIS phase: a name that only ever appears when it fails
  // cannot tell "no fault" from "never asked".
  report.require(NO_SPURIOUS_ERROR, true, "");
  (log_done, stable_done)
}

/// Drain BOTH of a group's completion queues, returning what each released.
fn drain_both<S>(
  engine: &mut S::Engine,
  gid: &S::Group,
  report: &mut Report,
) -> (Vec<LogDone>, Vec<StableDone>)
where
  S: EngineSubject,
{
  match engine.stores(gid) {
    Some((log, stable)) => (drain_log(log, report), drain_stable(stable, report)),
    None => (Vec::new(), Vec::new()),
  }
}

fn drain_group<S>(engine: &mut S::Engine, gid: &S::Group, report: &mut Report)
where
  S: EngineSubject,
{
  if let Some((log, stable)) = engine.stores(gid) {
    drain_log(log, report);
    drain_stable(stable, report);
  }
}

/// Group `n`'s state after the FIRST barrier.
fn first_image<S>(subject: &S, n: u64) -> GroupImage<S::NodeId>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  if n == 1 {
    GroupImage {
      hosted: true,
      first_index: Index::new(1),
      last_index: Index::new(3),
      entries: a_entries(1),
      hard_state: a_hard_state::<S>(subject, 1),
      visible_snapshot: None,
      durable_snapshot: None,
      floor: 5,
      lineage: 0,
      removal_floor: 0,
    }
  } else {
    let (meta, blob) = b_snapshot::<S>(subject, 1);
    GroupImage {
      hosted: true,
      first_index: Index::new(1),
      last_index: Index::new(2),
      entries: b_entries(1),
      hard_state: HardState::initial(),
      visible_snapshot: Some((meta.clone(), blob)),
      durable_snapshot: Some(meta),
      floor: 0,
      lineage: 0,
      removal_floor: 5,
    }
  }
}

/// Group `n`'s state after the SECOND barrier.
fn second_image<S>(subject: &S, n: u64) -> GroupImage<S::NodeId>
where
  S: EngineSubject,
  S::NodeId: Clone,
{
  let mut image = first_image::<S>(subject, n);
  if n == 1 {
    image.last_index = Index::new(6);
    image.entries = a_entries(1).into_iter().chain(a_entries(2)).collect();
    image.hard_state = a_hard_state::<S>(subject, 2);
    image.lineage = 9;
    image.removal_floor = 10;
  } else {
    let (meta, blob) = b_snapshot::<S>(subject, 2);
    image.last_index = Index::new(3);
    image.entries = b_entries(1).into_iter().chain(b_entries(2)).collect();
    image.visible_snapshot = Some((meta.clone(), blob));
    image.durable_snapshot = Some(meta);
    image.floor = 2;
    // The meta leg is REPLACED with the slot, so the ceiling follows the new generation.
    image.removal_floor = 7;
  }
  image
}

/// Read a group's COMPLETE state back out of a reopened engine.
/// How a reopened group's log answered the read its image is built from.
///
/// Folding every answer into "no entries" made an unreadable log indistinguishable from an empty
/// one — and on the legs where the image itself is unknowable, the image comparison was the only
/// reader, so a hosted group whose log faulted recorded NOTHING. A real restart poisons that
/// replica or wedges on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogRead {
  /// The id is not hosted, so there is no log to read.
  Absent,
  /// Resident: the read answered the COMPLETE run the log claims to retain.
  Resident,
  /// `Ready`, but not with what the claimed range promises — an empty answer to a non-empty
  /// claimed range, a short one, or a run that is not the aligned contiguous span from
  /// `first_index`.
  ContradictsItsOwnRange,
  /// The bounds themselves are impossible: `first_index` above `last_index.next()`.
  OrphanedRange,
  /// The restart-time read came back COLD. Production treats that as fail-stop, so the suite does
  /// too — see the classification below.
  ColdAtRestart,
  /// The read faulted.
  Faulted,
}

fn read_image<S>(
  engine: &mut S::Engine,
  gid: &S::Group,
  report: &mut Report,
  tag: &str,
) -> (GroupImage<S::NodeId>, LogRead)
where
  S: EngineSubject,
{
  let hosted = engine.contains_group(gid);
  let floor = FloorStore::floor(engine, gid);
  let lineage = FloorStore::lineage(engine, gid);
  let removal_floor = engine.removal_floor(gid);
  let mut image = GroupImage::absent();
  image.hosted = hosted;
  image.floor = floor;
  image.lineage = lineage;
  image.removal_floor = removal_floor;
  let mut read = LogRead::Absent;
  if let Some((log, stable)) = engine.stores(gid) {
    // SAMPLED ONCE. Reading the bounds twice let a store answer a different pair to the image than
    // to the read, so the run was graded against a claim nobody made.
    let first = log.first_index();
    let last = log.last_index();
    image.first_index = first;
    image.last_index = last;
    let range = first..last.next();
    // ONE READ, and a cold answer is FATAL — not something to re-drive until it lands. The core's
    // restart scans are resident-only: `endpoint/restart.rs` treats a cold (`Pending`), empty or
    // faulted in-range read during the synchronous lease-floor scan as unretryable and poisons
    // with `PoisonReason::LogRead`, because retrying it would under-size the floor. A suite that
    // re-drove to an eventual `Ready` certified exactly the store production fail-stops on.
    //
    // (The bounded re-drive was this suite's own idea, not the contract's. Production says
    // otherwise, so production wins.)
    // REFUSED BEFORE IT IS READ. A contiguous log satisfies `first_index <= last_index + 1` —
    // equal when empty or freshly baselined — so a wider gap is a shape no writer produces: the
    // residue of a partially-persisted re-baseline that advanced the front past a tail it never
    // wrote. `reconcile_restart_log` poisons with `PoisonReason::OrphanedLog` on exactly that
    // pair. Saturating the span to zero instead made an empty answer satisfy a vacuous contiguity
    // test, so the impossible shape read as resident.
    read = if first > last.next() {
      LogRead::OrphanedRange
    } else {
      match log.entries(range.clone(), u64::MAX) {
        // MEASURED AGAINST WHAT THE LOG ITSELF CLAIMS. `Ready` alone says only that the store
        // answered; the answer must be the complete aligned run from `first_index` through
        // `last_index`, because the budget here is `u64::MAX` and nothing may shorten it. An empty
        // answer to a non-empty claimed range is fatal by the same rule above.
        Ok(EntriesRead::Ready(view)) => {
          // Checked, not saturating: the bound above already established the range is well-formed,
          // and an arithmetic fallback here would quietly re-admit the shape it refuses.
          let claimed = range
            .end
            .get()
            .checked_sub(range.start.get())
            .expect("the orphaned-range bound above makes end >= start");
          let contiguous_from_start = view
            .iter()
            .enumerate()
            .all(|(n, entry)| entry.index().get() == range.start.get().saturating_add(n as u64));
          let resident = view.len() as u64 == claimed && contiguous_from_start;
          image.entries = view.to_vec();
          if resident {
            LogRead::Resident
          } else {
            LogRead::ContradictsItsOwnRange
          }
        }
        Ok(EntriesRead::Pending) => {
          // The re-drive is CONSUMED and graded even though the read is already fatal: a reopen
          // that comes alive on its first read can queue a dead incarnation's acknowledgment there,
          // and this poll is the only reader positioned to see it.
          match log.poll() {
            Some(Ok(done)) => report.require(
              "engine/reopen-manufactures-no-completions",
              false,
              std::format!(
                "[{tag}] re-driving a cold read produced {done:?}. Replay folds into DURABLE state \
               directly: an acknowledgment handed to the new incarnation belongs to op ids the \
               crashed one minted, which no pending map holds and no boot epoch can fence"
              ),
            ),
            Some(Err(_)) => report.require(
              NO_SPURIOUS_ERROR,
              false,
              std::format!("[{tag}] re-driving a cold read reported a store fault"),
            ),
            None => {}
          }
          LogRead::ColdAtRestart
        }
        Err(_) => LogRead::Faulted,
      }
    };
    image.hard_state = stable.hard_state();
    // BOTH slots, each verbatim. Rebuilding the visible entry out of the durable meta let a
    // reopened store keep a snapshot's shape and invent its lease bounds, its read mode and its
    // shape generation; requiring the durable answer to be present at all let a store fabricate a
    // servable slot with nothing behind it.
    image.visible_snapshot = stable.snapshot();
    image.durable_snapshot = stable.durable_snapshot();
  }
  // The boot epoch is DELIBERATELY not here. Handing one out is a mutation, and folding it into a
  // value the image compares tied the epoch's own rule to whether the image was knowable at all —
  // so a torn leg with no medium boundary skipped the epoch too. It has its own name and its own
  // comparison, taken on every leg.
  (image, read)
}

/// The optional durability PROBES are standing evidence the core reads instead of waiting for a
/// completion, so a reopen is exactly where a fabricated one does its damage — and the image
/// comparison cannot see them at all: an engine can come back with `durable_index` at the top of
/// the index space, or a `durable_hard_state` naming a term it never held, and every field the
/// image compares still matches.
fn check_reopened_probes<S>(engine: &mut S::Engine, subject: &S, report: &mut Report, tag: &str)
where
  S: EngineSubject,
{
  for n in 1..=2u64 {
    let gid = subject.group(n);
    let Some((log, stable)) = engine.stores(&gid) else {
      continue;
    };
    if let Some(answer) = log.durable_index() {
      report.require(
        "engine/reopened-durable-index-never-over-answers",
        answer <= log.last_index(),
        std::format!(
          "[{tag}] group {n} reopened answering durable_index() {answer:?} above its own visible \
           tip {:?}. The core folds this into the persist-before-ack watermark without waiting for \
           a completion, so an over-answer here acks a match the crash-surviving log does not back",
          log.last_index()
        ),
      );
    }
    if let Some(probe) = stable.durable_hard_state() {
      report.require(
        "engine/reopened-durable-hard-state-agrees",
        probe == stable.hard_state(),
        std::format!(
          "[{tag}] group {n} reopened with durable_hard_state() {probe:?} while hard_state() — the \
           other durable reader — says {:?}. Both describe the state a crash right now would \
           leave, and each gate the core releases reads whichever it was given",
          stable.hard_state()
        ),
      );
    }
  }
}

/// A reopened engine must hold NO pollable completion: replay reconstructs durable state, and the
/// incarnation those acknowledgments belonged to is gone.
fn check_no_manufactured_completions<S>(
  engine: &mut S::Engine,
  subject: &S,
  report: &mut Report,
  tag: &str,
) where
  S: EngineSubject,
  <EngineLogOf<S> as LogStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::Error: core::fmt::Debug,
{
  for n in 1..=2u64 {
    let gid = subject.group(n);
    if let Some((log, stable)) = engine.stores(&gid) {
      let log_pending = log.has_pending();
      let log_done = log.poll();
      let stable_pending = stable.has_pending();
      let stable_done = stable.poll();
      report.require(
        "engine/reopen-manufactures-no-completions",
        !log_pending && log_done.is_none() && !stable_pending && stable_done.is_none(),
        std::format!(
          "[{tag}] group {n} came back with completions queued (log {log_done:?}, stable \
           {stable_done:?}). Replay must fold into DURABLE state directly: an acknowledgment \
           handed to the new incarnation belongs to op ids the crashed one minted, which no \
           pending map holds and no boot epoch can fence"
        ),
      );
    }
  }
}
