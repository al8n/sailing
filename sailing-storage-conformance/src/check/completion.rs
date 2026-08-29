//! The completion-fault battery.
//!
//! A store's delivery channel is allowed to fail; what is NOT allowed is for a durability PROBE to
//! answer differently because of it. The probe reads the store's own durable state, so a completion
//! reordered, duplicated, delayed, lost, or arriving from a dead incarnation must leave it exactly
//! where it was — and, when the completion is LOST, the probe is the only thing that keeps the gated
//! path from wedging until the next restart.
//!
//! Every class asserts the SIGNATURE of its own fault in the delivered trace, not merely that
//! nothing impossible happened. A battery that checks only "no unknown op arrived" is satisfied by
//! a channel that injected nothing at all — and then proves the probe survives a fault that never
//! occurred.

use super::{LogSubject, Report, StableSubject};
use crate::fault::{CompletionFaults, FaultyLog, FaultyStable, prior_incarnation_op_id};
use bytes::Bytes;
use core::time::Duration;
use sailing_proto::{
  ConfState, Entry, EntryKind, HardState, Index, LeaseSupport, LogDone, LogStore, OpId,
  SnapshotMeta, StableDone, StableStore, Term,
};
use std::{format, vec::Vec};

/// How many completions each class's trace carries. Four, because a reorder over fewer than three
/// is not observable as reorder, and a loss RATE needs several to be a rate rather than a coin flip.
const TRACE: u64 = 4;

/// Ids for this battery live in boot epoch 1, so the prior-incarnation id the channel injects
/// (epoch 0) is strictly below every one of them — the ordering the whole stale-delivery defence
/// rests on.
fn battery_op(seq: u64) -> OpId {
  let mut id = OpId::first_of_epoch(1);
  for _ in 0..seq {
    id = id.next();
  }
  id
}

/// Every check the log battery is responsible for reaching.
const REQUIRED_LOG: &[&str] = &[
  "completion/delay-is-observed",
  "completion/delivery-invents-no-op",
  "completion/duplication-is-observed",
  "completion/log-probe-never-over-answers-under-faults",
  "completion/loss-heals-through-the-log-probe",
  "completion/loss-is-observed",
  "completion/poll-no-spurious-error",
  "completion/prior-incarnation-id-sorts-below",
  "completion/reorder-is-observed",
  "completion/stale-delivery-is-observed",
];

/// The stable battery's manifest — the log's, minus its id-ordering leg, over ITS OWN probe, and
/// plus the snapshot slot, which is a durable reader every store has rather than an optional probe.
const REQUIRED_STABLE: &[&str] = &[
  "completion/delay-is-observed",
  "completion/delivery-invents-no-op",
  "completion/duplication-is-observed",
  "completion/hard-state-probe-never-over-answers-under-faults",
  "completion/loss-heals-through-the-hard-state-probe",
  "completion/loss-heals-through-the-snapshot-slot",
  "completion/loss-is-observed",
  "completion/poll-no-spurious-error",
  "completion/reorder-is-observed",
  "completion/stale-delivery-is-observed",
];

/// The probe-dependent legs, one pair per PROBE. `durable_index` and `durable_hard_state` are
/// INDEPENDENT optional capabilities — a store may offer either, both, or neither — so they cannot
/// share a name: an engine absorbing both batteries would record one name as passed by the battery
/// whose probe exists and skipped by the battery whose probe does not, and `passed_check` would
/// then certify a gate nobody exercised. Each pair vanishes with its own probe.
const SKIPPABLE: &[&str] = &[
  "completion/hard-state-probe-never-over-answers-under-faults",
  "completion/log-probe-never-over-answers-under-faults",
  "completion/loss-heals-through-the-hard-state-probe",
  "completion/loss-heals-through-the-log-probe",
];

/// Where a completion channel comes from. The suite asks for a class; the injector decides what is
/// actually interposed — which is how the kit checks ITSELF, by running the whole battery through
/// an injector that interposes nothing and requiring every class to reject it.
pub trait CompletionInjector {
  /// The faults actually applied when the suite asks for `requested`.
  fn applied(&self, requested: CompletionFaults) -> CompletionFaults;
}

/// The injector that applies what it is asked for.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaithfulInjector;

impl CompletionInjector for FaithfulInjector {
  fn applied(&self, requested: CompletionFaults) -> CompletionFaults {
    requested
  }
}

/// Every class, run one at a time, then all together.
fn classes() -> Vec<CompletionFaults> {
  std::vec![
    CompletionFaults::reordering(),
    CompletionFaults::duplicating(),
    CompletionFaults::losing_every(2),
    CompletionFaults::delaying(2),
    CompletionFaults::stale_deliveries(),
    CompletionFaults::all(),
  ]
}

/// Require the fault's OWN signature in the delivered trace.
///
/// The combined class composes every rule at once, so its exact trace is not a fixed shape; the
/// invariants that survive composition are checked by the callers instead.
fn require_signature(
  report: &mut Report,
  faults: CompletionFaults,
  label: &str,
  submitted: &[OpId],
  delivered: &[OpId],
  empty_polls: usize,
  stale: OpId,
) {
  if faults == CompletionFaults::all() {
    return;
  }
  if faults.reorder {
    let reversed: Vec<OpId> = submitted.iter().rev().copied().collect();
    report.require(
      "completion/reorder-is-observed",
      delivered != submitted && delivered == reversed.as_slice(),
      format!(
        "[{label}] the channel delivered {delivered:?} for submissions {submitted:?}. A reorder \
         class that delivers submission order injected NOTHING, so everything it then proves about \
         the probe is proved against a fault that never happened"
      ),
    );
  }
  if faults.duplicate {
    let expected = submitted.len() * 2;
    let each_twice = submitted
      .iter()
      .all(|id| delivered.iter().filter(|d| *d == id).count() == 2);
    report.require(
      "completion/duplication-is-observed",
      delivered.len() == expected && each_twice,
      format!(
        "[{label}] the channel delivered {} completions for {} submissions (each-twice: \
         {each_twice}). A duplicating class that delivers each once injected nothing",
        delivered.len(),
        submitted.len()
      ),
    );
  }
  if faults.lose_every != 0 {
    let dropped = submitted.len().saturating_sub(delivered.len());
    let expected = submitted.len() / faults.lose_every as usize;
    report.require(
      "completion/loss-is-observed",
      dropped == expected && dropped > 0,
      format!(
        "[{label}] {dropped} of {} completions were dropped where {expected} were asked for. A \
         lossy class that delivers everything injected nothing, and the heal it then demonstrates \
         is a heal of nothing",
        submitted.len()
      ),
    );
  }
  if faults.delay_polls != 0 {
    report.require(
      "completion/delay-is-observed",
      empty_polls >= faults.delay_polls as usize,
      format!(
        "[{label}] the first completion arrived after {empty_polls} empty poll(s), where at least \
         {} were asked for. A delaying class that answers immediately injected nothing",
        faults.delay_polls
      ),
    );
  }
  if faults.stale_delivery {
    report.require(
      "completion/stale-delivery-is-observed",
      delivered.contains(&stale),
      format!(
        "[{label}] the prior incarnation's completion {stale:?} never appeared in {delivered:?}. A \
         stale-delivery class that delivers only live ids injected nothing, and the id ordering it \
         then proves is proved about a completion no reader ever saw"
      ),
    );
  }
}

/// Drain a faulted log channel, counting the polls that came back EMPTY before the first delivery.
fn drain_log_trace<L: LogStore>(
  faulty: &mut FaultyLog<'_, L>,
  faulted: &mut bool,
) -> (Vec<LogDone>, usize) {
  let mut out = Vec::new();
  let mut empty_before_first = 0usize;
  for _ in 0..1024 {
    match faulty.poll() {
      Some(Ok(done)) => out.push(done),
      Some(Err(_)) => {
        *faulted = true;
        break;
      }
      None if faulty.is_quiescent() => break,
      None => {
        if out.is_empty() {
          empty_before_first += 1;
        }
      }
    }
  }
  (out, empty_before_first)
}

/// The stable twin of [`drain_log_trace`].
fn drain_stable_trace<S: StableStore>(
  faulty: &mut FaultyStable<'_, S>,
  faulted: &mut bool,
) -> (Vec<StableDone>, usize) {
  let mut out = Vec::new();
  let mut empty_before_first = 0usize;
  for _ in 0..1024 {
    match faulty.poll() {
      Some(Ok(done)) => out.push(done),
      Some(Err(_)) => {
        *faulted = true;
        break;
      }
      None if faulty.is_quiescent() => break,
      None => {
        if out.is_empty() {
          empty_before_first += 1;
        }
      }
    }
  }
  (out, empty_before_first)
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

fn wrote(done: &[StableDone]) -> Vec<OpId> {
  done
    .iter()
    .filter_map(|d| match d {
      StableDone::Wrote(id) | StableDone::SnapshotWritten(id) => Some(*id),
      _ => None,
    })
    .collect()
}

/// Drive a [`LogSubject`] through every completion-fault class, checking that each fault's own
/// signature appears in the delivered trace AND that the durability probe is unmoved by it.
pub fn completion_faults_log<S>(subject: &mut S) -> Report
where
  S: LogSubject,
  <S::Log as LogStore>::Error: core::fmt::Debug,
{
  completion_faults_log_with(subject, &FaithfulInjector)
}

/// [`completion_faults_log`] over a chosen [`CompletionInjector`] — the seam the kit's own
/// red-proof uses to run the battery through a channel that interposes nothing.
pub fn completion_faults_log_with<S, I>(subject: &mut S, injector: &I) -> Report
where
  S: LogSubject,
  I: CompletionInjector,
  <S::Log as LogStore>::Error: core::fmt::Debug,
{
  let mut report = Report::new();
  let mut seq = 0u64;
  for faults in classes() {
    let label = faults.label();
    let mut submitted = Vec::new();
    let base = subject.log().last_index().get();
    for n in 0..TRACE {
      seq += 1;
      let id = battery_op(seq);
      submitted.push(id);
      subject.log().submit_append(
        id,
        &[Entry::new(
          Term::new(9),
          Index::new(base + n + 1),
          EntryKind::Normal,
          Bytes::from_static(b"battery"),
        )],
      );
    }
    subject.barrier();
    let extent = subject.log().last_index();

    let before_probe = subject.log().durable_index();
    let mut faulty = FaultyLog::new(subject.log(), injector.applied(faults));
    let mut faulted = false;
    let (delivered, empty_polls) = drain_log_trace(&mut faulty, &mut faulted);
    let probe = faulty.durable_index();
    let visible = faulty.last_index();
    drop(faulty);

    // A DELIVERY fault is not a STORE fault: the channel misbehaving must never be reported as
    // the store failing, or a driver tears down a healthy replica.
    report.require(
      "completion/poll-no-spurious-error",
      !faulted,
      format!("[{label}] poll() reported a store error while only the channel was faulted"),
    );
    let stale = prior_incarnation_op_id();
    let ids = appended(&delivered);
    require_signature(
      &mut report,
      faults,
      label,
      &submitted,
      &ids,
      empty_polls,
      stale,
    );

    // The stale exemption applies ONLY where a stale id was injected. Unconditional, it accepted a
    // prior incarnation's acknowledgment arriving spontaneously in the four classes that inject
    // none — which is a store inventing exactly the completion the fence exists to reject.
    let unexpected: Vec<OpId> = ids
      .iter()
      .copied()
      .filter(|op| !submitted.contains(op) && !(faults.stale_delivery && *op == stale))
      .collect();
    report.require(
      "completion/delivery-invents-no-op",
      unexpected.is_empty(),
      format!(
        "[{label}] the channel delivered completions for ops never submitted: {unexpected:?}"
      ),
    );
    if faults.stale_delivery {
      // Judged against the ids the STORE handed back: comparing two kit-minted constants could
      // never fail.
      report.require(
        "completion/prior-incarnation-id-sorts-below",
        ids.iter().filter(|op| **op != stale).all(|id| stale < *id),
        format!(
          "[{label}] the channel delivered live completion ids {ids:?}; a prior incarnation's id \
           {stale:?} must sort strictly below every one of them. Op ids are echoed back exactly as \
           submitted, so a store that re-mints them from its own counter lands live work at or \
           below a dead incarnation's id — and that incarnation's acknowledgment then releases a \
           live gate"
        ),
      );
    }

    match probe {
      Some(answer) => {
        // ALSO AGAINST THE READING TAKEN BEFORE THE DRAIN. A single post-drain sample cannot see a
        // probe that MOVED because of what the faulted channel delivered; the probe reads the
        // store's own durable state, which the channel does not touch.
        report.require(
          "completion/log-probe-never-over-answers-under-faults",
          before_probe.is_none_or(|before| answer == before),
          format!(
            "[{label}] durable_index() read {before_probe:?} before the faulted drain and \
             {answer:?} after it. A delivery channel cannot make a write durable, so it cannot \
             move the probe"
          ),
        );
        report.require(
          "completion/log-probe-never-over-answers-under-faults",
          answer <= visible,
          format!(
            "[{label}] durable_index() answered {answer:?} above the visible tip {visible:?}: a \
             faulted delivery channel must not move the probe at all"
          ),
        );
        report.require(
          "completion/loss-heals-through-the-log-probe",
          answer >= extent,
          format!(
            "[{label}] the appends through {extent:?} were made durable by a barrier, yet the \
             probe answers {answer:?}. The probe reads the store's OWN durable state, so it must \
             not depend on completions the channel swallowed — that independence is what heals the \
             stall within the run instead of at the next restart"
          ),
        );
      }
      // BOTH probe legs vanish together; recording only one left the other silently unreached.
      None => {
        for check in [
          "completion/loss-heals-through-the-log-probe",
          "completion/log-probe-never-over-answers-under-faults",
        ] {
          report.skip(
            check,
            "the store does not offer the durable_index probe, so a lost completion keeps its \
             documented stall until a restart",
          );
        }
      }
    }
  }
  report.require_coverage(REQUIRED_LOG, SKIPPABLE);
  report
}

/// The [`StableSubject`] half of the battery.
pub fn completion_faults_stable<S>(subject: &mut S) -> Report
where
  S: StableSubject,
  <S::Stable as StableStore>::Error: core::fmt::Debug,
  <S::Stable as StableStore>::NodeId: Clone,
{
  completion_faults_stable_with(subject, &FaithfulInjector)
}

/// [`completion_faults_stable`] over a chosen [`CompletionInjector`].
pub fn completion_faults_stable_with<S, I>(subject: &mut S, injector: &I) -> Report
where
  S: StableSubject,
  I: CompletionInjector,
  <S::Stable as StableStore>::Error: core::fmt::Debug,
  <S::Stable as StableStore>::NodeId: Clone,
{
  let mut report = Report::new();
  let mut seq = 0u64;
  for faults in classes() {
    let label = faults.label();
    let mut submitted = Vec::new();
    let mut last_written = HardState::initial();
    for n in 0..TRACE {
      seq += 1;
      let id = battery_op(seq);
      submitted.push(id);
      last_written = HardState::initial()
        .with_term(Term::new(20 + seq))
        .with_commit(Index::new(n + 1))
        .with_vote(Some(subject.node_id(1)))
        .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(10))))
        .with_founding_gen(29);
      subject.stable().submit_write(id, last_written.clone());
    }
    // A SNAPSHOT THROUGH THE SAME FAULTED CHANNEL. Only `Wrote` completions ever reached it, so
    // the whole `SnapshotWritten` class — a different completion kind, released from a different
    // slot — was never reordered, duplicated, lost, delayed or staled at all.
    seq += 1;
    let snapshot_id = battery_op(seq);
    submitted.push(snapshot_id);
    let snapshot_meta = SnapshotMeta::new(
      Index::new(TRACE + seq),
      Term::new(20 + seq),
      ConfState::from_voters([subject.node_id(1)]),
    )
    .with_shape_gen(seq);
    subject.stable().submit_snapshot(
      snapshot_id,
      snapshot_meta.clone(),
      Bytes::from_static(b"battery-blob"),
    );
    subject.barrier();

    let before_probe = subject.stable().durable_hard_state();
    let mut faulty = FaultyStable::new(subject.stable(), injector.applied(faults));
    let mut faulted = false;
    let (delivered, empty_polls) = drain_stable_trace(&mut faulty, &mut faulted);
    let probe = faulty.durable_hard_state();
    let durable = faulty.hard_state();
    let snapshot_probe = faulty.durable_snapshot();
    drop(faulty);

    // A DELIVERY fault is not a STORE fault: the channel misbehaving must never be reported as
    // the store failing, or a driver tears down a healthy replica.
    report.require(
      "completion/poll-no-spurious-error",
      !faulted,
      format!("[{label}] poll() reported a store error while only the channel was faulted"),
    );
    let stale = prior_incarnation_op_id();
    let ids = wrote(&delivered);
    require_signature(
      &mut report,
      faults,
      label,
      &submitted,
      &ids,
      empty_polls,
      stale,
    );

    // The stale exemption applies ONLY where a stale id was injected. Unconditional, it accepted a
    // prior incarnation's acknowledgment arriving spontaneously in the four classes that inject
    // none — which is a store inventing exactly the completion the fence exists to reject.
    let unexpected: Vec<OpId> = ids
      .iter()
      .copied()
      .filter(|op| !submitted.contains(op) && !(faults.stale_delivery && *op == stale))
      .collect();
    report.require(
      "completion/delivery-invents-no-op",
      unexpected.is_empty(),
      format!(
        "[{label}] the channel delivered completions for ops never submitted: {unexpected:?}"
      ),
    );

    // The snapshot slot is a durable reader too, and the barrier covered it before the channel
    // ever ran: whatever the channel did or did not deliver, it reads the boundary verbatim.
    //
    // ITS OWN NAME, because it needs no probe. Sharing the probe's name would let this leg pass
    // for a store offering no `durable_hard_state` at all while that same name was skipped for the
    // leg that does need one — and a consumer reading `passed_check` would believe a lost `Wrote`
    // heals in-process when the vote and campaign gates behind it stay wedged until a restart.
    report.require(
      "completion/loss-heals-through-the-snapshot-slot",
      snapshot_probe.as_ref() == Some(&snapshot_meta),
      format!(
        "[{label}] the snapshot was made durable by a barrier, yet durable_snapshot() answers \
         {snapshot_probe:?} instead of {snapshot_meta:?}. The slot reads the store's own durable \
         state, not the completions the channel chose to deliver"
      ),
    );
    match probe {
      Some(answer) => {
        report.require(
          "completion/hard-state-probe-never-over-answers-under-faults",
          before_probe.as_ref().is_none_or(|before| answer == *before),
          format!(
            "[{label}] durable_hard_state() read {before_probe:?} before the faulted drain and \
             {answer:?} after it; a delivery channel cannot make a write durable"
          ),
        );
        report.require(
          "completion/hard-state-probe-never-over-answers-under-faults",
          answer == durable,
          format!(
            "[{label}] durable_hard_state() answered {answer:?} while hard_state() reads \
             {durable:?}: a faulted delivery channel must not move either durable reader"
          ),
        );
        report.require(
          "completion/loss-heals-through-the-hard-state-probe",
          answer == last_written,
          format!(
            "[{label}] the writes were made durable by a barrier, yet the probe answers {answer:?} \
             instead of {last_written:?}; the probe must read the store's own durable state, not \
             the completions the channel delivered"
          ),
        );
      }
      None => {
        for check in [
          "completion/loss-heals-through-the-hard-state-probe",
          "completion/hard-state-probe-never-over-answers-under-faults",
        ] {
          report.skip(
            check,
            "the store does not offer the durable_hard_state probe, so a lost completion keeps \
             its documented stall until a restart",
          );
        }
      }
    }
  }
  report.require_coverage(REQUIRED_STABLE, SKIPPABLE);
  report
}
