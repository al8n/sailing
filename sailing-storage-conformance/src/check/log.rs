//! The [`LogStore`] suite.

use super::{LogSubject, Report};
use bytes::Bytes;
use sailing_proto::{EntriesRead, Entry, EntryKind, Index, LogDone, LogStore, OpId, Term};
use std::{
  collections::{BTreeMap, BTreeSet},
  format,
  string::String,
  vec::Vec,
};

/// One entry, with every field distinct from every other entry's.
///
/// Identical entries make a resident-range oracle blind: comparing a run of entries that differ
/// only by index cannot see a store that swapped two payloads, normalised every kind, or dropped
/// the self-describing lease fields. Kind cycles, payload carries the coordinates, and the three
/// lease fields are salted apart, so verbatim comparison discriminates over all of them.
fn entry(term: u64, index: u64) -> Entry {
  let kind = match index % 3 {
    0 => EntryKind::Normal,
    1 => EntryKind::ConfChange,
    _ => EntryKind::SetReadMode,
  };
  let salt = term * 101 + index * 7;
  Entry::new(
    Term::new(term),
    Index::new(index),
    kind,
    Bytes::from(std::format!("t{term}i{index}")),
  )
  .with_timestamp(salt)
  .with_lease_window(salt * 3 + 1)
  .with_wall_timestamp(salt * 5 + 2)
}

fn run(term: u64, from: u64, through: u64) -> Vec<Entry> {
  (from..=through).map(|i| entry(term, i)).collect()
}

/// Read a resident range back and compare it to `expected` VERBATIM.
///
/// Index and count alone certify a store that hands back the right SHAPE and the wrong CONTENT —
/// stripped payloads, normalised kinds, a term from the wrong entry. The core replays whatever
/// comes back from here, so every field of it is load-bearing.
fn require_resident_run<L>(
  log: &L,
  report: &mut Report,
  check: &'static str,
  range: core::ops::Range<Index>,
  expected: &[Entry],
  what: &str,
) where
  L: LogStore,
  L::Error: core::fmt::Debug,
{
  match log.entries(range.clone(), u64::MAX) {
    Ok(EntriesRead::Ready(view)) => report.require(
      check,
      &*view == expected,
      format!(
        "{what}: entries({:?}..{:?}) must come back VERBATIM — every payload byte, kind, term and \
         self-describing field. Expected {expected:?}, got {:?}",
        range.start, range.end, &*view
      ),
    ),
    Ok(EntriesRead::Pending) => report.require(
      check,
      false,
      format!("{what}: a resident range is never Pending"),
    ),
    Err(e) => report.require(check, false, format!("{what}: entries() faulted: {e:?}")),
  }
}

/// Drain every ready completion, checking [`LogStore::has_pending`] against what the very next
/// [`poll`](LogStore::poll) actually yields.
///
/// This is the only exact reading of that contract: `has_pending` is defined as "`poll` would
/// return `Some`", and the two are compared at the one instant where the answer is decidable.
///
/// The operations the log has been handed SO FAR, in the order they were handed over. Kept beside
/// the extents because a completion names an operation of either kind, and because an id that is
/// only submitted later must be rejected the moment it is delivered.
#[derive(Debug, Default)]
struct Accepted {
  appends: BTreeSet<OpId>,
  compactions: BTreeSet<Index>,
}

impl Accepted {
  fn append(&mut self, id: OpId) {
    self.appends.insert(id);
  }

  fn compaction(&mut self, up_to: Index) {
    self.compactions.insert(up_to);
  }

  fn holds(&self, done: &LogDone) -> bool {
    match done {
      LogDone::Appended(id) => self.appends.contains(id),
      LogDone::Compacted(up_to) => self.compactions.contains(up_to),
      // A completion kind this build of the kit does not know is not one the log was given.
      _ => false,
    }
  }
}

/// Drain every ready completion, checking [`LogStore::has_pending`] exactly, and validating each
/// `Appended` AT THE MOMENT IT IS CONSUMED against the extent it was submitted for.
///
/// The log-side twin of the stable suite's rule, and the same reason: an `Appended` for an append
/// reaching index `N` is a claim that the whole prefix through `N` is durable, so a store that
/// answers the `durable_index` probe must ALREADY cover `N` when it hands the completion over.
/// Asking that only at fixed points lets a store release the completion first and move the probe
/// afterwards, which is exactly the phantom-durable-replica window the probe exists to close.
fn drain_validating<L>(
  log: &mut L,
  report: &mut Report,
  extents: &BTreeMap<OpId, Index>,
  accepted: &Accepted,
) -> Vec<LogDone>
where
  L: LogStore,
  L::Error: core::fmt::Debug,
{
  let mut out = Vec::new();
  // Recorded on the CLEAN path too: a check that exists only when it fails never runs against a
  // conforming subject, so nothing in the report can tell "no fault" from "never asked".
  let mut faulted: Option<String> = None;
  loop {
    let claimed = log.has_pending();
    match log.poll() {
      Some(Ok(done)) => {
        report.require(
          "log/has-pending-exact",
          claimed,
          format!("has_pending() was false, yet the next poll() yielded {done:?}"),
        );
        // A COMPLETION NAMES AN OPERATION THE LOG WAS ACTUALLY GIVEN, and one given by NOW. An id
        // it never held — or holds only later in the run — is an acknowledgment for work that does
        // not exist, and membership over the FINAL set cannot see it: by the end every submitted
        // id has a matching completion either way.
        report.require(
          "log/completion-names-an-accepted-operation",
          accepted.holds(&done),
          format!(
            "the log released {done:?}, which names no operation it had been given by then. \
             Accepted so far: {accepted:?}"
          ),
        );
        // RIGHT HERE, while the claim is being made.
        if let LogDone::Appended(id) = done
          && let Some(&upto) = extents.get(&id)
          && let Some(answer) = log.durable_index()
        {
          report.require(
            "log/durable-index-covers-a-released-append",
            answer >= upto,
            format!(
              "an `Appended` for an append reaching {upto:?} is a claim that the whole prefix \
               through it is durable, yet at the moment it was consumed durable_index() answered \
               {answer:?}. A completion released ahead of the probe it should already have moved \
               lets the core ack a match the crash-surviving log does not back"
            ),
          );
        }
        out.push(done);
      }
      Some(Err(e)) => {
        faulted = Some(format!("{e:?}"));
        break;
      }
      None => {
        report.require(
          "log/has-pending-exact",
          !claimed,
          "has_pending() was true, yet the next poll() yielded None",
        );
        break;
      }
    }
  }
  report.require(
    "log/poll-no-spurious-error",
    faulted.is_none(),
    format!(
      "poll() reported a store fault during a conforming sequence: {}",
      faulted.clone().unwrap_or_default()
    ),
  );
  out
}

/// The term at `index`, or `None` when the read faulted — a fault is itself a violation the caller
/// reports through the comparison that follows.
fn term_of<L>(log: &L, index: u64) -> Option<u64>
where
  L: LogStore,
{
  log.term(Index::new(index)).ok().map(Term::get)
}

fn appended(done: &[LogDone]) -> Vec<OpId> {
  done
    .iter()
    .filter_map(|d| match d {
      LogDone::Appended(id) => Some(*id),
      _ => None,
    })
    .collect()
}

/// Every check this suite is responsible for reaching.
const REQUIRED: &[&str] = &[
  "log/compact-moves-the-boundary",
  "log/compaction-completes",
  "log/completion-exactly-once",
  "log/completion-names-an-accepted-operation",
  "log/durable-index-clamp",
  "log/durable-index-covers-a-released-append",
  "log/durable-index-never-past-the-view",
  "log/entries-aligned-and-contiguous",
  "log/entries-capped-prefix",
  "log/entries-out-of-view-is-empty",
  "log/fresh-subject",
  "log/has-pending-exact",
  "log/poll-no-spurious-error",
  "log/read-view-immediate",
  "log/restore-drops-stale-completions",
  "log/restore-rebaselines",
  "log/superseded-append-never-completes",
  "log/survivor-completes-exactly-once",
  "log/term-domain-never-errs",
  "log/truncated-append-does-not-complete",
  "log/truncation-rewrites-the-view",
];

/// Checks a store may legitimately leave unasked: the three that need the optional `durable_index`
/// probe, and the superseded-completion rule, which a store completing at submit answers before the
/// truncation can supersede anything.
const SKIPPABLE: &[&str] = &[
  "log/durable-index-clamp",
  "log/superseded-append-never-completes",
  "log/durable-index-covers-a-released-append",
  "log/durable-index-never-past-the-view",
  "log/truncated-append-does-not-complete",
];

/// Check a [`LogStore`] against the read-view, completion, and durability-probe contracts.
///
/// The subject must hand back a FRESH log; the suite says so and stops if it does not.
pub fn log_store<S>(subject: &mut S) -> Report
where
  S: LogSubject,
  <S::Log as LogStore>::Error: core::fmt::Debug,
{
  let mut report = Report::new();
  let log = subject.log();
  let fresh = log.last_index() == Index::ZERO && log.first_index() == Index::new(1);
  report.require(
    "log/fresh-subject",
    fresh,
    format!(
      "a fresh log must read first_index()==1 and last_index()==0, got {:?}..={:?}",
      log.first_index(),
      log.last_index()
    ),
  );
  if !fresh {
    return report;
  }
  report.require(
    "log/has-pending-exact",
    !log.has_pending(),
    "a fresh log has nothing to poll",
  );

  // The read view moves at submit, ahead of durability: the core allocates the next index from
  // `last_index()` synchronously, so a store whose view lagged its barrier would hand two
  // proposals the same index.
  // What each accepted append CLAIMS is durable when its completion is released — the input the
  // at-consumption probe check is judged against.
  let mut extents: BTreeMap<OpId, Index> = BTreeMap::new();
  let mut accepted = Accepted::default();
  let op_a = OpId::new(1);
  accepted.append(op_a);
  log.submit_append(op_a, &run(1, 1, 3));
  extents.insert(op_a, Index::new(3));
  report.require(
    "log/read-view-immediate",
    log.last_index() == Index::new(3) && log.first_index() == Index::new(1),
    format!(
      "after submit_append(1..=3) the view must read 1..=3, got {:?}..={:?}",
      log.first_index(),
      log.last_index()
    ),
  );
  report.require(
    "log/read-view-immediate",
    term_of(log, 3) == Some(1),
    "term() must see a submitted-but-undurable entry",
  );
  require_resident_run(
    log,
    &mut report,
    "log/entries-aligned-and-contiguous",
    Index::new(1)..Index::new(4),
    &run(1, 1, 3),
    "the just-submitted run",
  );
  // ENDING STRICTLY INSIDE THE VIEW. Every read above stops at last_index + 1, which a store that
  // ignores `range.end` and returns everything it holds answers correctly by accident.
  require_resident_run(
    log,
    &mut report,
    "log/entries-aligned-and-contiguous",
    Index::new(1)..Index::new(3),
    &run(1, 1, 2),
    "a read whose end falls inside the retained range",
  );

  // A byte cap may shorten the answer but never empties an in-view range, and never returns a
  // suffix: a caller advances by the last index it saw.
  match log.entries(Index::new(2)..Index::new(4), 1) {
    Ok(EntriesRead::Ready(view)) => {
      let expected = run(1, 2, 3);
      report.require(
        "log/entries-capped-prefix",
        !view.is_empty() && view.len() <= expected.len() && *view == expected[..view.len()],
        format!(
          "a byte-capped read must still return a VERBATIM prefix of the range starting at 2 — a \
           cap shortens the answer, it does not change the entries in it. Expected a prefix of \
           {expected:?}, got {:?}",
          &*view
        ),
      );
    }
    Ok(EntriesRead::Pending) => report.require(
      "log/entries-capped-prefix",
      false,
      "a resident range is never Pending, byte cap or not",
    ),
    Err(e) => report.require(
      "log/entries-capped-prefix",
      false,
      format!("a byte-capped in-view read faulted: {e:?}"),
    ),
  }
  // A ZERO cap is the degenerate case of the same rule: it may shorten the answer to one entry,
  // never to none. A store that treats zero as "return nothing" stalls replication outright, since
  // the caller advances by the last index it saw and there is none.
  match log.entries(Index::new(2)..Index::new(4), 0) {
    Ok(EntriesRead::Ready(view)) => report.require(
      "log/entries-capped-prefix",
      view.first() == run(1, 2, 3).first(),
      format!(
        "entries(2..4, 0) must still return at least the first entry of the range; got {:?}",
        &*view
      ),
    ),
    other => report.require(
      "log/entries-capped-prefix",
      false,
      format!(
        "entries(2..4, 0) over a resident range must be Ready, got {}",
        match other {
          Ok(EntriesRead::Pending) => "Pending",
          _ => "an error",
        }
      ),
    ),
  }
  match log.entries(Index::new(4)..Index::new(6), u64::MAX) {
    Ok(EntriesRead::Ready(view)) => report.require(
      "log/entries-out-of-view-is-empty",
      view.is_empty(),
      format!("a range above last_index() holds nothing, got {view:?}"),
    ),
    _ => report.require(
      "log/entries-out-of-view-is-empty",
      false,
      "a range above last_index() must answer Ready(empty), not Pending or Err",
    ),
  }

  // Every index outside the retained range is a routine probe the core makes with peer-controlled
  // values: it answers Term::ZERO, never Err, or ordinary traffic poisons the node.
  for probe in [0u64, 4, 99] {
    report.require(
      "log/term-domain-never-errs",
      term_of(log, probe) == Some(0),
      format!("term({probe}) outside the retained range must be Ok(Term::ZERO)"),
    );
  }

  let before_barrier = drain_validating(subject.log(), &mut report, &extents, &accepted);
  let released_early = !appended(&before_barrier).is_empty();
  subject.barrier();
  let after_barrier = drain_validating(subject.log(), &mut report, &extents, &accepted);
  let mut completions = before_barrier;
  completions.extend(after_barrier);
  report.require(
    "log/completion-exactly-once",
    appended(&completions) == std::vec![op_a],
    format!("submit_append must complete exactly once for its own id; got {completions:?}"),
  );

  // A conflicting append REWRITES visible content the durable bytes no longer match. Everything
  // below turns on that: the superseded completion must not fire, and the durability probe must
  // cap at the last index where durable and visible still agree.
  let op_b = OpId::new(2);
  let log = subject.log();
  accepted.append(op_b);
  log.submit_append(op_b, &run(2, 2, 4));
  extents.insert(op_b, Index::new(4));
  report.require(
    "log/truncation-rewrites-the-view",
    log.last_index() == Index::new(4) && term_of(log, 2) == Some(2),
    "a conflicting append must truncate the superseded suffix and take its place in the view",
  );
  {
    // The whole surviving view, verbatim: the kept prefix of the old run followed by the new one.
    let mut surviving = run(1, 1, 1);
    surviving.extend(run(2, 2, 4));
    require_resident_run(
      log,
      &mut report,
      "log/truncation-rewrites-the-view",
      Index::new(1)..Index::new(5),
      &surviving,
      "after a conflicting append",
    );
  }
  // A SAFE PREFIX IS ALWAYS A VALID ANSWER. `durable_index` may under-answer — that is what makes
  // it safe to fold into a watermark without waiting for a completion — and `submit_append` never
  // promised its slice reaches the medium atomically: durability is PREFIX-ORDERED, and that holds
  // WITHIN one append's entries as much as between appends. So a store that persisted 2..=4 at
  // submit answers 4, a store that staged the whole thing answers 1 or less, and a store that
  // persisted part of the run — or simply lags its own medium conservatively — answers 2 or 3.
  // All three are honest.
  //
  // The regime cannot be inferred from the completion queue: `poll` and `has_pending` describe
  // COMPLETION-QUEUE readiness and nothing else, so a store that persists at submit and
  // acknowledges at the barrier looks staged to a drain. Nor may a store that claims past 1 be held
  // to the full extent, which refuses the honest prefix. What is left is the rule that needs no
  // regime at all: never past what the log actually holds. The teeth for a store that INVENTS
  // durability live where the answer is knowable independently — the disjoint re-baseline below,
  // where the visible log shares no index with the medium and zero is the only honest answer for
  // any store.
  let log = subject.log();
  if let Some(answer) = log.durable_index() {
    report.require(
      "log/durable-index-clamp",
      answer <= log.last_index(),
      format!(
        "durable_index() answered {answer:?} past the visible tip {:?}. A durable prefix stops \
         where the log's own content stops; answering beyond it manufactures a phantom durable \
         replica out of indices the store does not hold",
        log.last_index()
      ),
    );
  } else {
    report.skip(
      "log/durable-index-clamp",
      "the store does not offer the durable_index probe",
    );
  }

  let op_c = OpId::new(3);
  let log = subject.log();
  accepted.append(op_c);
  log.submit_append(op_c, &run(2, 5, 5));
  extents.insert(op_c, Index::new(5));
  subject.barrier();
  let log = subject.log();
  let done = drain_validating(log, &mut report, &extents, &accepted);
  let ids = appended(&done);
  // Exactly ONE outcome is recorded. Recording the pass AND the skip made the name read as
  // covered on a store that never had the property tested.
  if released_early {
    report.skip(
      "log/truncated-append-does-not-complete",
      "the store completes at submit, so the superseded append was already durable when it was \
       truncated",
    );
  } else {
    report.require(
      "log/truncated-append-does-not-complete",
      !ids.contains(&op_a),
      format!(
        "the append at 1..=3 was superseded before any barrier released it, so completing it \
         would claim a durable prefix through an index the log no longer holds; got {ids:?}"
      ),
    );
  }
  // COUNTED. Membership let a store release the same completion twice under the very name that
  // says "exactly once", and the core folds each one into a durability watermark.
  let (b_count, c_count) = (
    ids.iter().filter(|id| **id == op_b).count(),
    ids.iter().filter(|id| **id == op_c).count(),
  );
  report.require(
    "log/completion-exactly-once",
    b_count == 1 && c_count == 1,
    format!(
      "each surviving append completes exactly once; the append at 2..=4 completed {b_count} \
       time(s) and the one at 5..=5 {c_count} time(s). Got {ids:?}"
    ),
  );

  let log = subject.log();
  if log.durable_index().is_none() {
    for check in [
      "log/durable-index-never-past-the-view",
      "log/durable-index-covers-a-released-append",
    ] {
      report.skip(check, "the store does not offer the durable_index probe");
    }
  }
  if let Some(answer) = log.durable_index() {
    report.require(
      "log/durable-index-never-past-the-view",
      answer <= log.last_index(),
      format!(
        "durable_index() answered {answer:?} above last_index() {:?}: no index outside the visible \
         log can have a durable visible prefix",
        log.last_index()
      ),
    );
  }

  // Compaction moves the front boundary and keeps the boundary term readable — the snapshot
  // boundary the core probes on every stale `prev_log_index`.
  accepted.compaction(Index::new(3));
  log.compact(Index::new(3));
  report.require(
    "log/compact-moves-the-boundary",
    log.first_index() == Index::new(4)
      && term_of(log, 3) == Some(2)
      && log.last_index() == Index::new(5),
    format!(
      "after compact(3) the view is 4..=5 with term(3) the boundary term, got {:?}..={:?}",
      log.first_index(),
      log.last_index()
    ),
  );
  {
    let mut surviving = run(2, 4, 4);
    surviving.extend(run(2, 5, 5));
    require_resident_run(
      log,
      &mut report,
      "log/compact-moves-the-boundary",
      Index::new(4)..Index::new(6),
      &surviving,
      "after compact(3)",
    );
  }
  // BELOW THE BOUNDARY, not merely outside the original range. A compacted index is the shape a
  // stale `prev_log_index` takes on every reconnect, and the answer is the same Term::ZERO.
  for probe in [1u64, 2] {
    report.require(
      "log/term-domain-never-errs",
      term_of(log, probe) == Some(0),
      format!(
        "term({probe}) below the compaction boundary must be Ok(Term::ZERO); a store that errs \
         there poisons the node on ordinary catch-up traffic"
      ),
    );
  }
  subject.barrier();
  let done = drain_validating(subject.log(), &mut report, &extents, &accepted);
  // EXACTLY ONE, and no other boundary. `any` accepted a duplicate under the name that promises a
  // single completion, and nothing else in this suite inspects a `Compacted` at all — `appended`
  // discards them — so a spurious one for an index never compacted had no reader either.
  let compactions: Vec<Index> = done
    .iter()
    .filter_map(|d| match d {
      LogDone::Compacted(i) => Some(*i),
      _ => None,
    })
    .collect();
  report.require(
    "log/compaction-completes",
    compactions == [Index::new(3)],
    format!("compact(3) must surface exactly one completion, for index 3; got {compactions:?}"),
  );

  // A restore re-baselines the whole log synchronously. Every queued completion for a discarded
  // index must go with it: a stale `Appended` would ack entries the log no longer stores.
  let log = subject.log();
  log.restore(Index::new(20), Term::new(7));
  report.require(
    "log/restore-rebaselines",
    log.first_index() == Index::new(21)
      && log.last_index() == Index::new(20)
      && term_of(log, 20) == Some(7),
    format!(
      "after restore(20, 7) the view is 21..=20 with term(20)==7, got {:?}..={:?} term {:?}",
      log.first_index(),
      log.last_index(),
      term_of(log, 20)
    ),
  );
  require_resident_run(
    log,
    &mut report,
    "log/restore-rebaselines",
    Index::new(21)..Index::new(22),
    &[],
    "after restore(20, 7) no entry survives the re-baseline",
  );
  if let Some(answer) = log.durable_index() {
    // ZERO, not "at or below the new tip". The re-baseline to 20 is DISJOINT from the 1..=5 the
    // medium holds, so there is no index at which the durable bytes and the visible log agree —
    // and a crash right now returns the log to the content the medium still holds. A clamp phrased
    // against the new tip would let a store follow its own view across a restore and report a
    // durable prefix nothing wrote.
    report.require(
      "log/durable-index-clamp",
      answer == Index::ZERO,
      format!(
        "durable_index() answered {answer:?} after a staged re-baseline to 20 over content the \
         medium holds at 1..=5. The two share no index, so the honest answer is zero until the \
         snapshot behind the re-baseline is itself durable"
      ),
    );
  }
  let op_d = OpId::new(4);
  accepted.append(op_d);
  log.submit_append(op_d, &run(7, 21, 22));
  log.restore(Index::new(30), Term::new(8));
  // The SECOND re-baseline's own effect. Only the first was ever asserted, so a store that
  // discards the queued completion and then ignores the re-baseline itself passed the check below
  // while serving a boundary two generations stale.
  report.require(
    "log/restore-rebaselines",
    log.first_index() == Index::new(31)
      && log.last_index() == Index::new(30)
      && term_of(log, 30) == Some(8),
    format!(
      "after restore(30, 8) the view is 31..=30 with term(30)==8, got {:?}..={:?} term {:?}",
      log.first_index(),
      log.last_index(),
      term_of(log, 30)
    ),
  );
  subject.barrier();
  let done = drain_validating(subject.log(), &mut report, &extents, &accepted);
  report.require(
    "log/restore-drops-stale-completions",
    !appended(&done).contains(&op_d),
    format!(
      "the append at 21..=22 was discarded by a re-baseline to 30, so its completion must never \
       fire; got {done:?}"
    ),
  );

  // TRUE STAGED SUPERSESSION: the conflict lands while BOTH appends are still behind the barrier,
  // which is the only window where a store gets to choose. Counted, not merely checked for
  // membership: a store that emits the survivor's completion twice satisfies "the survivor
  // completed" and still double-counts a durability watermark.
  // The suite's OWN expectation, not the subject's answer. Reading the tip back and using it as
  // the oracle for what follows let a store choose the coordinates it would be judged at — and a
  // subject reporting a tip near the top of the index space overflowed the arithmetic below.
  let base = 30u64;
  report.require(
    "log/restore-rebaselines",
    subject.log().last_index() == Index::new(base),
    format!(
      "the re-baseline to {base} must leave the tip there; got {:?}",
      subject.log().last_index()
    ),
  );
  let doomed = OpId::new(10);
  let survivor = OpId::new(11);
  let log = subject.log();
  accepted.append(doomed);
  extents.insert(doomed, Index::new(base + 3));
  log.submit_append(doomed, &run(9, base + 1, base + 3));
  // CLASSIFIED BEFORE THE CONFLICT EXISTS. A store whose barrier is a no-op settles this append
  // here, while it is still the only one — there is no staged window in which it could choose to
  // withhold the completion, so the supersession rule below has nothing to ask it. Draining now is
  // the only moment that difference is visible.
  let doomed_early = drain_validating(subject.log(), &mut report, &extents, &accepted);
  let doomed_settled_at_submit = appended(&doomed_early).contains(&doomed);
  let log = subject.log();
  accepted.append(survivor);
  log.submit_append(survivor, &run(10, base + 2, base + 4));
  extents.insert(survivor, Index::new(base + 4));
  report.require(
    "log/truncation-rewrites-the-view",
    log.last_index() == Index::new(base + 4) && term_of(log, base + 2) == Some(10),
    "the conflicting append takes the superseded suffix's place in the view",
  );
  {
    // THE PAYLOADS, not the tip and one term. A store that rewrites the boundary metadata and
    // keeps the superseded BYTES serves a peer entries from a term that lost.
    let mut expected = run(9, base + 1, base + 1);
    expected.extend(run(10, base + 2, base + 4));
    require_resident_run(
      log,
      &mut report,
      "log/truncation-rewrites-the-view",
      Index::new(base + 1)..Index::new(base + 5),
      &expected,
      "after the conflicting append supersedes the tail",
    );
  }
  let early = drain_validating(subject.log(), &mut report, &extents, &accepted);
  subject.barrier();
  let mut settled = doomed_early;
  settled.extend(early);
  settled.extend(drain_validating(
    subject.log(),
    &mut report,
    &extents,
    &accepted,
  ));
  let ids = appended(&settled);
  let doomed_count = ids.iter().filter(|id| **id == doomed).count();
  let survivor_count = ids.iter().filter(|id| **id == survivor).count();
  if doomed_settled_at_submit {
    report.skip(
      "log/superseded-append-never-completes",
      "the store settled the append before the conflicting one arrived, so it was already durable \
       when it was superseded — there was never a staged window in which withholding the \
       completion was the store's to choose",
    );
  } else {
    report.require(
      "log/superseded-append-never-completes",
      doomed_count == 0,
      format!(
        "the append at {}..={} was superseded before any barrier released it, yet it completed \
       {doomed_count} time(s). Releasing it would claim a durable prefix through an index the log \
       no longer holds",
        base + 1,
        base + 3
      ),
    );
  }
  report.require(
    "log/survivor-completes-exactly-once",
    survivor_count == 1,
    format!(
      "the surviving append completed {survivor_count} time(s), not once. The core folds each \
       completion into a durability watermark, so a duplicate is not a harmless repeat"
    ),
  );

  report.require_coverage(REQUIRED, SKIPPABLE);
  report
}
