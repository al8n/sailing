//! What a suite hands back: a named outcome per check, and the assertion that turns any violation
//! into a test failure.

use std::{
  string::{String, ToString},
  vec::Vec,
};

/// One check that the subject broke.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
  /// The check's stable name (`"log/durable-index-clamp"`). Stable enough to assert on, which is
  /// how the kit's own red-proofs pin each mutant to the check that catches it.
  pub check: &'static str,
  /// What the subject did, in enough detail to act on without re-running.
  pub detail: String,
}

/// One check the subject could not be asked — an optional seam it does not offer.
///
/// A skip is NOT a pass. A report full of them proves little, which is why they are listed
/// separately rather than folded into the passing count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skip {
  /// The check's stable name.
  pub check: &'static str,
  /// Why it could not be asked.
  pub reason: String,
}

/// The outcome of one suite.
#[derive(Debug, Clone, Default)]
pub struct Report {
  passed: Vec<&'static str>,
  violations: Vec<Violation>,
  skipped: Vec<Skip>,
}

impl Report {
  /// An empty report.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// The checks the subject broke.
  #[must_use]
  pub fn violations(&self) -> &[Violation] {
    &self.violations
  }

  /// The checks that could not be asked.
  #[must_use]
  pub fn skipped(&self) -> &[Skip] {
    &self.skipped
  }

  /// The names of the checks the subject satisfied.
  #[must_use]
  pub fn passed(&self) -> &[&'static str] {
    &self.passed
  }

  /// Whether the subject broke nothing. Says nothing about COVERAGE — see
  /// [`skipped`](Self::skipped).
  #[must_use]
  pub fn is_conformant(&self) -> bool {
    self.violations.is_empty()
  }

  /// Whether `check` recorded a violation. The kit's own red-proofs use this to pin a deliberately
  /// broken implementation to the exact check that must catch it.
  #[must_use]
  pub fn failed(&self, check: &str) -> bool {
    self.violations.iter().any(|v| v.check == check)
  }

  /// Whether `check` ran and passed.
  #[must_use]
  pub fn passed_check(&self, check: &str) -> bool {
    self.passed.contains(&check)
  }

  /// Hold the report to its suite's MANIFEST: every `required` name must have been reached, and
  /// every skip must name a check the suite declared genuinely optional.
  ///
  /// Two holes close here, and both are invisible to a violation count. A SKIP was conformant — so
  /// a suite could skip its sharpest checks and still report clean — and a check that never RAN was
  /// neither passed, failed, nor skipped, so an oracle deleted by a refactor, or unreachable behind
  /// a condition that stopped holding, simply vanished from the report. Both now become violations,
  /// which is the only thing `assert_conformant` reads.
  pub fn require_coverage(&mut self, required: &[&'static str], skippable: &[&'static str]) {
    for name in required {
      if !self.reached(name) {
        self.violations.push(Violation {
          check: "kit/required-check-never-ran",
          detail: std::format!(
            "{name} is on this suite's manifest and was never reached — neither passed, failed, \
             nor skipped. A check that does not run proves nothing, and nothing else in the report \
             can tell it apart from one that passed"
          ),
        });
      }
    }
    // Judge only THIS suite's namespace. An absorbed sub-suite already ran its own coverage pass
    // over its own names, and re-judging them here would demand that every caller restate a
    // sub-suite's allow-list.
    let namespace = required
      .first()
      .and_then(|name| name.split_once('/'))
      .map(|(prefix, _)| prefix);
    let unexpected: Vec<&'static str> = self
      .skipped
      .iter()
      .map(|s| s.check)
      .filter(|name| namespace.is_some_and(|prefix| name.starts_with(prefix)))
      .filter(|name| !skippable.contains(name))
      .collect();
    for name in unexpected {
      self.violations.push(Violation {
        check: "kit/skip-is-allow-listed",
        detail: std::format!(
          "{name} was skipped, but this suite does not declare it optional. A skip is an admission \
           that the subject could not be asked; one the manifest did not anticipate is a hole, not \
           a capability"
        ),
      });
    }
  }

  /// Whether a check has been recorded at all, under any outcome.
  #[must_use]
  pub fn reached(&self, check: &str) -> bool {
    self.passed.contains(&check)
      || self.violations.iter().any(|v| v.check == check)
      || self.skipped.iter().any(|s| s.check == check)
  }

  /// Record a skip ONLY if the check was never reached.
  ///
  /// A suite that poses the same question across many legs must not let a leg that could not ask
  /// it contradict a leg that did: a name recorded as both passed and skipped reads as covered
  /// while proving nothing.
  pub fn skip_if_unreached(&mut self, check: &'static str, reason: impl AsRef<str>) {
    if !self.reached(check) {
      self.skip(check, reason);
    }
  }

  /// Record a skip that DOMINATES a sibling leg's pass.
  ///
  /// For a name whose claim spans several legs — every crash class, say — a leg that could not be
  /// asked is not made good by a leg that could. `skip_if_unreached` is the wrong tool there: the
  /// sibling legs have already recorded the pass, so the skip is suppressed and the report claims
  /// a property it never asked under the conditions that matter. This withdraws the pass and
  /// records the skip instead.
  ///
  /// A FAILURE still outranks both: a leg that actually broke the property is stronger evidence
  /// than a leg that could not pose it, so a name already failed keeps its violation and takes no
  /// skip.
  pub fn skip_dominating(&mut self, check: &'static str, reason: impl AsRef<str>) {
    if self.failed(check) {
      return;
    }
    self.passed.retain(|p| *p != check);
    if !self.skipped.iter().any(|s| s.check == check) {
      self.skip(check, reason);
    }
  }

  /// Fold another suite's outcome into this one.
  pub fn absorb(&mut self, other: Self) {
    self.violations.extend(other.violations);
    // RECONCILED, NOT CONCATENATED, and in RELEASE. Two sub-suites can each be internally
    // consistent and still disagree about one name: one asks the property and passes it, the other
    // cannot ask it and skips. Concatenating left the name in BOTH sets, where every sub-report's
    // own coverage had already succeeded, the outer coverage judges another namespace, and
    // `passed_check` certified a gate nobody exercised.
    //
    // The skip DOMINATES: a name partly unasked is not covered, whatever a sibling leg found. A
    // FAILURE dominates in turn — a proven violation is stronger evidence than "not asked" — so a
    // name already broken stays broken and takes no skip. And the disagreement itself is
    // surfaced rather than quietly resolved: two suites recording one name under different
    // askability means the name is standing for two capabilities and wants splitting.
    for skip in other.skipped {
      if self.passed.contains(&skip.check) {
        self.passed.retain(|p| *p != skip.check);
        self.violations.push(Violation {
          check: "kit/absorbed-outcomes-conflict",
          detail: std::format!(
            "{} was passed by one absorbed suite and skipped by another. The two gate on \
             INDEPENDENT capabilities, so one name cannot stand for both: the pass is withdrawn \
             and the name reads as skipped, but the name itself needs splitting",
            skip.check
          ),
        });
      }
      if !self.skipped.iter().any(|s| s.check == skip.check) {
        self.skipped.push(skip);
      }
    }
    for name in other.passed {
      if self.skipped.iter().any(|s| s.check == name) {
        self.violations.push(Violation {
          check: "kit/absorbed-outcomes-conflict",
          detail: std::format!(
            "{name} was skipped by one absorbed suite and passed by another; the pass is not \
             recorded, and the name needs splitting"
          ),
        });
        continue;
      }
      if !self.passed.contains(&name) && !self.failed(name) {
        self.passed.push(name);
      }
    }
  }

  /// Panic naming every violation, or return quietly when there are none — the one line a test
  /// needs.
  ///
  /// # Panics
  /// If the subject broke any check.
  pub fn assert_conformant(&self) {
    assert!(
      self.is_conformant(),
      "{} conformance violation(s) ({} checks passed, {} skipped):\n{}",
      self.violations.len(),
      self.passed.len(),
      self.skipped.len(),
      self
        .violations
        .iter()
        .map(|v| std::format!("  - {}: {}", v.check, v.detail))
        .collect::<Vec<_>>()
        .join("\n")
    );
  }

  /// Record `check` as satisfied iff `holds`, with `detail` describing the breach otherwise.
  pub(crate) fn require(&mut self, check: &'static str, holds: bool, detail: impl AsRef<str>) {
    if holds {
      // ONE OUTCOME PER NAME, enforced where the outcome is recorded rather than left to each
      // suite's discipline. A name that is both passed and skipped reads as covered to
      // `passed_check` while some leg of it was never asked. The shape that produces it is a
      // property recorded under a probe-dependent name, passed on the leg that needs no probe and
      // skipped on the leg that does. Use `skip_if_unreached` where a leg may legitimately be
      // unaskable; split the name where two properties share one.
      debug_assert!(
        !self.skipped.iter().any(|s| s.check == check),
        "{check} was skipped and is now passing: split the name, or record the skip with \
         skip_if_unreached"
      );
      // A check is asserted at several edges under one name. It counts as passed only while no
      // edge has broken it, so `passed_check` and `failed` can never both answer true for a name.
      if !self.passed.contains(&check) && !self.failed(check) {
        self.passed.push(check);
      }
    } else {
      self.passed.retain(|p| *p != check);
      self.violations.push(Violation {
        check,
        detail: detail.as_ref().to_string(),
      });
    }
  }

  /// Record `check` as unaskable.
  pub(crate) fn skip(&mut self, check: &'static str, reason: impl AsRef<str>) {
    debug_assert!(
      !self.passed.contains(&check),
      "{check} passed and is now being skipped: split the name, or record the skip with \
       skip_if_unreached"
    );
    self.skipped.push(Skip {
      check,
      reason: reason.as_ref().to_string(),
    });
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const REQUIRED: &[&str] = &["demo/one", "demo/two"];
  const SKIPPABLE: &[&str] = &["demo/two"];

  #[test]
  fn a_manifest_check_that_never_ran_is_a_violation() {
    let mut report = Report::new();
    report.require("demo/one", true, "");
    report.require_coverage(REQUIRED, SKIPPABLE);
    assert_eq!(
      report
        .violations()
        .iter()
        .map(|v| v.check)
        .collect::<Vec<_>>(),
      std::vec!["kit/required-check-never-ran"]
    );
  }

  #[test]
  fn a_skip_the_manifest_did_not_anticipate_is_a_violation() {
    let mut report = Report::new();
    report.require("demo/one", true, "");
    report.skip("demo/two", "optional seam");
    report.skip("demo/three", "invented");
    report.require_coverage(REQUIRED, SKIPPABLE);
    assert_eq!(
      report
        .violations()
        .iter()
        .map(|v| v.check)
        .collect::<Vec<_>>(),
      std::vec!["kit/skip-is-allow-listed"]
    );
  }

  #[test]
  fn another_suites_skips_are_left_to_that_suite() {
    let mut report = Report::new();
    report.require("demo/one", true, "");
    report.skip("demo/two", "optional seam");
    let mut absorbed = Report::new();
    absorbed.skip("other/thing", "its own suite already judged this");
    report.absorb(absorbed);
    report.require_coverage(REQUIRED, SKIPPABLE);
    assert!(report.violations().is_empty(), "{:?}", report.violations());
  }

  #[test]
  fn a_leg_that_could_not_ask_never_contradicts_one_that_did() {
    let mut report = Report::new();
    report.require("demo/one", true, "");
    report.skip_if_unreached("demo/one", "unaskable on this leg");
    assert!(report.skipped().is_empty());
    report.skip_if_unreached("demo/two", "unaskable everywhere");
    assert_eq!(report.skipped().len(), 1);
  }
}
