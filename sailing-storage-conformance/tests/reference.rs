//! The kit against the in-tree reference implementation. Every suite must pass here: the reference
//! engine IS the contract's first subject, so a violation is either a regression in it or a bug in
//! the check.

use sailing_storage_conformance::{
  check,
  fault::{ReferenceCodec, ReferenceLogSubject, ReferenceStableSubject},
};

#[test]
fn the_reference_log_store_conforms() {
  let report = check::log_store(&mut ReferenceLogSubject::new());
  report.assert_conformant();
  assert!(
    report.passed_check("log/durable-index-clamp") || !report.skipped().is_empty(),
    "the probe checks must either run or be reported as skipped"
  );
}

#[test]
fn the_reference_stable_store_conforms() {
  check::stable_store(&mut ReferenceStableSubject::new()).assert_conformant();
}

#[test]
fn the_reference_codec_conforms() {
  let report = check::serialization(&ReferenceCodec);
  report.assert_conformant();
  for check in [
    "serde/legacy-decodes-unrecorded",
    "serde/legacy-founds-at-zero",
    "serde/founding-gen-verbatim",
    "serde/truncated-input-never-decodes",
    "serde/complete-input-still-decodes",
    "serde/meta-configuration-verbatim",
    "serde/meta-simple-configuration-verbatim",
  ] {
    assert!(
      report.passed_check(check),
      "{check} must actually run against the reference codec"
    );
  }
}

#[test]
fn the_reference_probing_log_conforms() {
  use sailing_storage_conformance::fault::ProbingLogSubject;
  let report = check::log_store(&mut ProbingLogSubject::default());
  report.assert_conformant();
  assert!(
    report.passed_check("log/durable-index-clamp"),
    "the probing log answers durable_index, so the clamp check must actually run"
  );
}

#[test]
fn the_reference_probing_stable_conforms() {
  use sailing_storage_conformance::fault::ProbingStableSubject;
  let report = check::stable_store(&mut ProbingStableSubject::default());
  report.assert_conformant();
  assert!(
    report.passed_check("stable/durable-hard-state-agrees-with-the-durable-reader"),
    "the probing store answers durable_hard_state, so that check must actually run"
  );
}

#[test]
fn the_reference_stores_survive_every_completion_fault_class() {
  use sailing_storage_conformance::fault::{ProbingLogSubject, ProbingStableSubject};
  let log = check::completion_faults_log(&mut ProbingLogSubject::default());
  log.assert_conformant();
  let stable = check::completion_faults_stable(&mut ProbingStableSubject::default());
  stable.assert_conformant();
  // Each class must have OBSERVED its own fault, not merely failed to observe an impossible one.
  for report in [&log, &stable] {
    for check in [
      "completion/reorder-is-observed",
      "completion/duplication-is-observed",
      "completion/loss-is-observed",
      "completion/delay-is-observed",
      "completion/stale-delivery-is-observed",
    ] {
      assert!(
        report.passed_check(check),
        "{check} must actually run against the probing stores"
      );
    }
  }
  // Each battery answers for ITS OWN probe. `durable_index` and `durable_hard_state` are
  // independent capabilities, so one name cannot stand for both.
  for check in [
    "completion/loss-heals-through-the-log-probe",
    "completion/log-probe-never-over-answers-under-faults",
  ] {
    assert!(
      log.passed_check(check),
      "{check} must run against the probing log"
    );
  }
  for check in [
    "completion/loss-heals-through-the-hard-state-probe",
    "completion/hard-state-probe-never-over-answers-under-faults",
  ] {
    assert!(
      stable.passed_check(check),
      "{check} must run against the probing stable store"
    );
  }
}

#[test]
fn the_in_memory_reference_engine_conforms() {
  use sailing_storage_conformance::fault::ReferenceEngineSubject;
  let report = check::engine(&mut ReferenceEngineSubject::in_memory());
  report.assert_conformant();
  assert!(
    report.passed_check("engine/volatile-engine-keeps-nothing"),
    "the in-memory engine's tier is volatile, and the suite must check that law directly"
  );
  assert!(
    report.passed_check("engine/terminal-floor-folds-and-admits-nothing"),
    "the durable tombstone is checkable on any tier"
  );
  assert!(
    report.passed_check("engine/lineage-record-rejects-the-reserved-band"),
    "the caller's half of the set_group_gen contract is answerable on either tier"
  );
  for check in [
    "engine/removal-ceiling-folds-a-shape-entry",
    "engine/removal-ceiling-retracts-a-truncated-shape-entry",
    "engine/removal-ceiling-caps-on-an-invalid-shape-entry",
    "engine/boot-epoch-survives-a-removal",
  ] {
    assert!(
      report.passed_check(check),
      "the log leg of the removal ceiling is answerable by every engine now, so {check} must \
       GRADE rather than skip"
    );
  }
  assert!(
    report.passed_check("engine/removal-ceiling-never-reaches-the-terminal"),
    "the fold is engine-level state, so the boundary is answerable on either tier — which is why \
     the check sits on the manifest both of them owe"
  );
  // ONE OUTCOME PER NAME, over the whole report. This is the shape that hid two defects: a name
  // recorded as passed on one leg and skipped on another reads as covered to `passed_check` while
  // the leg that mattered was never asked. `Report` now debug-asserts it where the outcome is
  // recorded; this pins it over a real subject's whole report as well, in release too.
  for name in report.passed() {
    assert!(
      !report.skipped().iter().any(|s| s.check == *name),
      "{name} is recorded as BOTH passed and skipped"
    );
  }
  // The store offers no `durable_hard_state`, so the property that needs it must not read as
  // covered — the snapshot slot agreeing says nothing about whether a lost `Wrote` heals.
  assert!(
    !report.passed_check("completion/loss-heals-through-the-hard-state-probe"),
    "with no hard-state probe a lost Wrote stays wedged until a restart; the report must not \
     claim otherwise"
  );
  assert!(
    report.passed_check("completion/loss-heals-through-the-snapshot-slot"),
    "the snapshot slot is a durable reader every store has, so its own recovery leg still runs"
  );
  assert!(
    report
      .skipped()
      .iter()
      .any(|s| s.check == "completion/loss-heals-through-the-hard-state-probe"),
    "the in-tree engine's stores do not offer the durability probes, so the heal must be reported \
     as skipped rather than silently passing"
  );
}

#[test]
fn the_journalling_reference_engine_conforms() {
  use sailing_storage_conformance::fault::JournalEngineSubject;
  let report = check::engine(&mut JournalEngineSubject::new());
  report.assert_conformant();
  for check in [
    "engine/exactly-flush-covered-state-survives",
    "engine/exactly-the-maximal-valid-prefix-survives",
    "engine/barrier-is-all-or-nothing-across-a-crash",
    "engine/durability-precedes-the-barriers-return",
    "engine/reopen-manufactures-no-completions",
    "engine/boot-epoch-never-repeats-across-a-reopen",
    "engine/terminal-floor-folds-and-admits-nothing",
    "engine/durable-index-covers-a-released-append",
    "engine/lineage-record-rejects-the-reserved-band",
    "engine/removal-ceiling-never-reaches-the-terminal",
    "engine/removal-ceiling-folds-a-shape-entry",
    "engine/removal-ceiling-retracts-a-truncated-shape-entry",
    "engine/removal-ceiling-caps-on-an-invalid-shape-entry",
    "engine/boot-epoch-survives-a-removal",
    "restore/an-unrecoverable-incarnation-refuses-typed",
    "completion/loss-heals-through-the-log-probe",
    "completion/loss-heals-through-the-hard-state-probe",
    "completion/loss-heals-through-the-snapshot-slot",
    "completion/log-probe-never-over-answers-under-faults",
    "completion/hard-state-probe-never-over-answers-under-faults",
    "restore/below-the-record-refuses-typed",
  ] {
    assert!(
      report.passed_check(check),
      "the durable subject must actually exercise {check}"
    );
  }
  // The torn-tail legs SKIP for a subject that hides its medium's boundary, so the reference — the
  // one subject that DOES report it — is where they have to actually run. A suite whose sharpest
  // legs are skipped everywhere proves nothing.
  for check in [
    "engine/barrier-is-all-or-nothing-across-a-crash",
    "engine/exactly-the-maximal-valid-prefix-survives",
  ] {
    assert!(
      !report.skipped().iter().any(|s| s.check == check),
      "{check} must RUN against the reference, never be skipped away"
    );
  }
}

#[test]
fn the_durable_record_is_judged_after_a_reopen() {
  use sailing_storage_conformance::fault::JournalEngineSubject;
  let report = check::restore_admission(&mut JournalEngineSubject::new());
  report.assert_conformant();
  assert!(
    report.passed_check("restore/below-the-record-refuses-typed")
      && report.passed_check("restore/a-known-id-over-empty-stores-refuses")
      && report.passed_check("restore/lineage-record-survives-the-medium"),
    "the durable subject must judge the rules against a record that came back from a reopen"
  );
}

/// AN ASYMMETRIC-PROBE ENGINE. `durable_index` and `durable_hard_state` are independent optional
/// capabilities, so a perfectly ordinary engine may offer one and decline the other. Absorbing the
/// two completion batteries under shared names then put one name in BOTH the passed and the
/// skipped set — each sub-report's own coverage succeeded, the outer coverage judges a different
/// namespace, and `passed_check` certified the gate belonging to the probe that does not exist.
#[test]
fn an_engine_offering_one_probe_certifies_only_that_one() {
  use sailing_storage_conformance::fault::JournalEngineSubject;
  let report = check::engine(&mut JournalEngineSubject::offering_only_the_hard_state_probe());
  for name in report.passed() {
    assert!(
      !report.skipped().iter().any(|s| s.check == *name),
      "{name} is recorded as BOTH passed and skipped"
    );
  }
  assert!(
    !report.passed_check("completion/loss-heals-through-the-log-probe"),
    "the log declines durable_index, so nothing may certify the gate that reads it"
  );
  assert!(
    report
      .skipped()
      .iter()
      .any(|s| s.check == "completion/loss-heals-through-the-log-probe"),
    "the absent log probe must be reported as skipped"
  );
  assert!(
    report.passed_check("completion/loss-heals-through-the-hard-state-probe"),
    "the stable store DOES answer durable_hard_state, so its own gate is genuinely exercised"
  );
  assert!(
    !report
      .violations()
      .iter()
      .any(|v| v.check == "kit/absorbed-outcomes-conflict"),
    "with a name per capability there is nothing for absorb to reconcile: {:?}",
    report.violations()
  );
}

/// THE CRASH MATRIX, reconciled against the loop rather than left as a comment.
///
/// One subject per column of the table above `crash_half`. Each report must be coverage-clean —
/// `require_coverage` fails a manifest name that is neither graded nor skipped — which is the
/// table's invariant stated mechanically: no REQUIRED name may be structurally unreachable in
/// every column a tier reaches. A name present in one column and silently absent in another reads
/// as covered without being asked, so the table is held to the loop mechanically rather than left
/// to be re-derived by inspection.
#[test]
fn the_crash_matrix_columns_are_each_covered() {
  use sailing_storage_conformance::fault::{JournalEngineSubject, ReferenceEngineSubject};

  // Column V: a volatile tier, whose reopen keeps nothing.
  let volatile = check::engine(&mut ReferenceEngineSubject::in_memory());
  // Columns D-CL and D-TB: a durable tier that names its medium's boundary, so every torn offset
  // is aimed and graded.
  let with_boundary = check::engine(&mut JournalEngineSubject::new());
  // Columns D-CL, D-TN and D-TU: a durable tier that will not name it, so the real torn offsets go
  // unsettled and only the cut that removes nothing is knowable.
  let without_boundary = check::engine(&mut JournalEngineSubject::hiding_its_boundary());

  for (column, report) in [
    ("volatile", &volatile),
    ("durable, boundary named", &with_boundary),
    ("durable, boundary hidden", &without_boundary),
  ] {
    report.assert_conformant();
    for name in report.passed() {
      assert!(
        !report.skipped().iter().any(|s| s.check == *name),
        "[{column}] {name} is recorded as BOTH passed and skipped"
      );
    }
  }

  // The cells the table calls G for a named boundary and D for a hidden one — the difference the
  // dominance rule exists to express, and the one the matrix must keep honest.
  // Atomicity is decidable from the image alone, so it is GRADED in both — the D-TU cell that
  // moved when the whole image stopped being discarded there.
  assert!(
    with_boundary.passed_check("engine/barrier-is-all-or-nothing-across-a-crash")
      && without_boundary.passed_check("engine/barrier-is-all-or-nothing-across-a-crash"),
    "a whole-barrier claim needs no layout: it is graded in every durable column"
  );
  for broad in [
    "engine/exactly-the-maximal-valid-prefix-survives",
    "engine/durability-precedes-the-barriers-return",
  ] {
    assert!(
      with_boundary.passed_check(broad),
      "{broad} is graded where every torn offset can be aimed"
    );
    assert!(
      !without_boundary.passed_check(broad)
        && without_boundary.skipped().iter().any(|s| s.check == broad),
      "{broad} spans every crash class, so a column that cannot ask it withdraws the pass"
    );
  }

  // And the cells the table calls G in EVERY durable column, boundary or not: these need no image.
  for always in [
    "engine/boot-epoch-never-repeats-across-a-reopen",
    "engine/hosted-ids-lend-stores",
    "engine/reopen-manufactures-no-completions",
  ] {
    assert!(
      with_boundary.passed_check(always) && without_boundary.passed_check(always),
      "{always} is graded in every durable column: it needs no image and no boundary"
    );
  }
  assert!(
    volatile.passed_check("engine/volatile-engine-keeps-nothing"),
    "the volatile column's own law must be graded there"
  );
}
