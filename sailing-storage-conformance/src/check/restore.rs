//! The restore-admission suite: what an engine's DURABLE lineage record entitles a restore to.
//!
//! The record is the only reading of an id's incarnation that outlives the process that wrote it,
//! so the rules it carries are only meaningful against a record that came back from a REOPEN. That
//! is what this suite drives: write the record, crash, reopen, and judge a restore against what
//! survived.

use super::{Durability, EngineSubject, Report};
use crate::fault::CrashClass;
use bytes::Bytes;
use sailing_proto::{
  CreateGroupError, Entry, EntryKind, FloorStore, GroupStores, HardState, Index, LogStore,
  MultiEngine, OpId, StableStore, Term, validate_restore,
};
use std::format;

/// The engine's per-group log handle type.
type EngineLogOf<S> = <<S as EngineSubject>::Engine as MultiEngine<
  <S as EngineSubject>::Group,
  <S as EngineSubject>::NodeId,
>>::Log;
/// The engine's per-group stable handle type.
type EngineStableOf<S> = <<S as EngineSubject>::Engine as MultiEngine<
  <S as EngineSubject>::Group,
  <S as EngineSubject>::NodeId,
>>::Stable;

/// Judge the restore rules against an engine's record, `label`ling which reading of it was used.
fn judge<S>(engine: &mut S::Engine, subject: &S, report: &mut Report, label: &str)
where
  S: EngineSubject,
  <EngineStableOf<S> as StableStore>::NodeId: PartialEq,
{
  let stateful = subject.group(1);
  let blank = subject.group(2);
  let husk = subject.group(3);

  let record = FloorStore::lineage(engine, &stateful);
  let floor = FloorStore::floor(engine, &stateful);
  report.require(
    "restore/lineage-record-survives-the-medium",
    record == 7,
    format!("[{label}] the id's lineage record read {record} where 7 was flushed for it"),
  );

  let Some((log, stable)) = engine.stores(&stateful) else {
    report.require(
      "restore/lineage-record-survives-the-medium",
      false,
      format!("[{label}] the group's stores did not come back at all"),
    );
    return;
  };
  let below = validate_restore(record, floor, record.saturating_sub(1), log, stable);
  report.require(
    "restore/below-the-record-refuses-typed",
    matches!(below, Err(CreateGroupError::BelowLineageRecord { record: r }) if r == record),
    format!(
      "[{label}] a restore one generation BELOW the durable record {record} answered {below:?}. \
       It must refuse typed: silently folding it up by max hides a catalog that has rolled back at \
       exactly the moment the durable record is the better-informed reading"
    ),
  );
  let at = validate_restore(record, floor, record, log, stable);
  let above = validate_restore(record, floor, record.saturating_add(2), log, stable);
  report.require(
    "restore/at-or-above-the-record-admits",
    at.is_ok() && above.is_ok(),
    format!(
      "[{label}] a restore at the record answered {at:?} and one above it {above:?}; both agree \
       with the record on the lineage's direction and must be admitted"
    ),
  );

  let blank_record = FloorStore::lineage(engine, &blank);
  let blank_floor = FloorStore::floor(engine, &blank);
  if let Some((log, stable)) = engine.stores(&blank) {
    let verdict = validate_restore(blank_record, blank_floor, blank_record, log, stable);
    report.require(
      "restore/a-known-id-over-empty-stores-refuses",
      matches!(verdict, Err(CreateGroupError::NoStoredState)),
      format!(
        "[{label}] an id the lineage knows (record {blank_record}) over stores holding no hard \
         state, no snapshot and no log answered {verdict:?}. There is nothing to recover, so a \
         restore must refuse rather than present a blank term-0 endpoint as recovered state"
      ),
    );
  } else {
    // A VIOLATION, not a skip. The sibling arm above gets this right: an id the engine reports as
    // hosted and then cannot lend stores for is a broken engine, not an absent capability.
    report.require(
      "restore/a-known-id-over-empty-stores-refuses",
      false,
      format!("[{label}] the engine holds a record for this id but lent no stores for it"),
    );
  }

  // LOG CONTENT BESIDE AN INITIAL HARD STATE. The two stores carry no cross-store durability
  // ordering, so this is the one shape where the founding generation's loss is both possible and
  // undetectable afterwards: the counter would rebuild at zero here while peers stand at the
  // founding value.
  let husk_record = FloorStore::lineage(engine, &husk);
  let husk_floor = FloorStore::floor(engine, &husk);
  // THE FLOOR, read back. It is passed to every `validate_restore` above and its term in the
  // stored-state rule is dead in each of them, so a garbage floor rode through the whole suite.
  report.require(
    "restore/lineage-record-survives-the-medium",
    husk_floor == 4,
    format!("[{label}] the id's floor read {husk_floor} where 4 was flushed for it"),
  );
  if let Some((log, stable)) = engine.stores(&husk) {
    let verdict = validate_restore(husk_record, husk_floor, husk_record, log, stable);
    report.require(
      "restore/an-unrecoverable-incarnation-refuses-typed",
      matches!(
        verdict,
        Err(CreateGroupError::IncarnationUnrecoverable { record: r }) if r == husk_record
      ),
      format!(
        "[{label}] an id the lineage knows (record {husk_record}) over surviving log content and \
         an INITIAL hard state answered {verdict:?}. The founding generation lived only in that \
         hard state, so nothing that survived can rebuild the incarnation; admitting it seats a \
         replica whose counter restarts at zero"
      ),
    );
  } else {
    report.require(
      "restore/an-unrecoverable-incarnation-refuses-typed",
      false,
      format!("[{label}] the engine holds a record for this id but lent no stores for it"),
    );
  }
}

/// Every check this suite is responsible for reaching.
const REQUIRED: &[&str] = &[
  "restore/a-known-id-over-empty-stores-refuses",
  "restore/an-unrecoverable-incarnation-refuses-typed",
  "restore/at-or-above-the-record-admits",
  "restore/below-the-record-refuses-typed",
  "restore/judged-against-a-reopened-record",
  "restore/lineage-record-survives-the-medium",
];

/// Only the reopened-record leg is optional, and only for an engine whose state dies with it.
const SKIPPABLE: &[&str] = &["restore/judged-against-a-reopened-record"];

/// Check the restore-admission rules against an engine's lineage record, before and after a crash.
pub fn restore_admission<S>(subject: &mut S) -> Report
where
  S: EngineSubject,
  <EngineLogOf<S> as LogStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::Error: core::fmt::Debug,
  <EngineStableOf<S> as StableStore>::NodeId: PartialEq,
{
  let mut report = Report::new();
  let mut engine = subject.open();
  let stateful = subject.group(1);
  let blank = subject.group(2);
  let husk = subject.group(3);

  engine.add_group(stateful.clone());
  engine.add_group(blank.clone());
  engine.add_group(husk.clone());
  {
    let (_, stable) = engine.stores(&stateful).expect("just admitted");
    stable.submit_write(OpId::new(1), HardState::initial().with_term(Term::new(3)));
  }
  engine.set_group_gen(&stateful, 7);
  // The blank id gets a record and NO state — the shape a removal, a cleared tombstone, and a
  // hopeful restore leave behind.
  engine.set_group_gen(&blank, 3);
  // The husk gets a record and log content, but its hard state never leaves the initial value.
  {
    let (log, _) = engine.stores(&husk).expect("just admitted");
    log.submit_append(
      OpId::new(2),
      &[Entry::new(
        Term::new(4),
        Index::new(1),
        EntryKind::Normal,
        Bytes::from_static(b"husk"),
      )],
    );
  }
  engine.set_group_gen(&husk, 5);
  engine.set_group_floor(&husk, 4);
  engine.flush();

  judge::<S>(&mut engine, subject, &mut report, "live");

  match subject.durability() {
    Durability::Durable => {
      let mut reopened = subject.crash(engine, CrashClass::LoseUnsyncedWrites);
      judge::<S>(&mut reopened, subject, &mut report, "reopened");
      report.require(
        "restore/judged-against-a-reopened-record",
        !report.failed("restore/lineage-record-survives-the-medium"),
        "the rules must hold against a record that came back from the medium, not only against \
         the live one",
      );
      subject.crash(reopened, CrashClass::Clean);
    }
    Durability::Volatile => {
      report.skip(
        "restore/judged-against-a-reopened-record",
        "the engine's record dies with it, so there is no reopened record to judge a restore \
         against — the embedder's catalog remains the cross-restart authority",
      );
      subject.crash(engine, CrashClass::Clean);
    }
  }
  report.require_coverage(REQUIRED, SKIPPABLE);
  report
}
