//! The [`StableStore`] suite.

use super::{Report, StableSubject};
use bytes::Bytes;
use core::time::Duration;
use sailing_proto::{
  ConfState, HardState, Index, LeaseSupport, OpId, SnapshotChunkRead, SnapshotMeta, StableDone,
  StableStore, Term,
};
use std::{format, string::String, vec::Vec};

/// Drain every ready completion, checking [`StableStore::has_pending`] against what the very next
/// [`poll`](StableStore::poll) yields — the log suite's reading of the same contract.
/// Drain every ready completion, validating each ONE AT THE MOMENT IT IS CONSUMED.
///
/// A completion is the store's own claim that its write is durable, and that claim is only
/// answerable while it is being made. Classifying from a snapshot of ids collected on one earlier
/// drain FREEZES the question at that phase: a completion surfacing on any later drain is never
/// asked at all, so a store that stays silent, then releases while its durable reader still shows
/// the prior value, then advances at the barrier passes every remaining check — the final counts,
/// the membership, and the post-barrier state are all satisfied by then.
///
/// `written` and `meta` are what each class's completion CLAIMS is durable; a `None` meta means no
/// snapshot has been submitted yet, so no such claim is possible.
fn drain_validating<S>(
  store: &mut S,
  report: &mut Report,
  written: &HardState<S::NodeId>,
  meta: Option<&SnapshotMeta<S::NodeId>>,
  submitted: &[StableDone],
) -> Vec<StableDone>
where
  S: StableStore,
  S::Error: core::fmt::Debug,
  S::NodeId: Clone,
{
  let mut out = Vec::new();
  // Recorded on the CLEAN path too: a check that exists only when it fails never runs against a
  // conforming subject, so nothing in the report can tell "no fault" from "never asked".
  let mut faulted: Option<String> = None;
  loop {
    let claimed = store.has_pending();
    match store.poll() {
      Some(Ok(done)) => {
        report.require(
          "stable/has-pending-exact",
          claimed,
          format!("has_pending() was false, yet the next poll() yielded {done:?}"),
        );
        // A COMPLETION NAMES AN OPERATION THAT WAS ACTUALLY ACCEPTED, and one already accepted by
        // now. An id-and-kind the store was never handed — or was handed only LATER — is an
        // acknowledgment for work that does not exist, and membership over the final set cannot see
        // it: by the end every submitted id has a matching completion either way.
        report.require(
          "stable/completion-names-an-accepted-write",
          submitted.contains(&done),
          format!(
            "the store released {done:?}, which names no write it had been given by then. \
             Accepted so far: {submitted:?}"
          ),
        );
        // RIGHT HERE, while the claim is being made.
        match done {
          StableDone::Wrote(_) => report.require(
            "stable/hard-state-is-last-durable",
            store.hard_state() == *written,
            format!(
              "a `Wrote` completion is a claim that this write is DURABLE, yet at the moment it \
               was consumed hard_state() read {:?} rather than the written state. A completion \
               released ahead of the durability it reports releases every gate the core fences on \
               it — and releasing it a drain later hides that from any oracle that classified \
               earlier",
              store.hard_state()
            ),
          ),
          StableDone::SnapshotWritten(_) => {
            if let Some(meta) = meta {
              report.require(
                "stable/durable-snapshot-is-never-the-visible-slot",
                store.durable_snapshot().as_ref() == Some(meta),
                format!(
                  "a `SnapshotWritten` completion is a claim that this blob is DURABLE, yet at the \
                   moment it was consumed durable_snapshot() read {:?}. The core runs the \
                   destructive log re-baseline of a deferred install on this answer",
                  store.durable_snapshot()
                ),
              );
            }
          }
          _ => {}
        }
        out.push(done);
      }
      Some(Err(e)) => {
        faulted = Some(format!("{e:?}"));
        break;
      }
      None => {
        report.require(
          "stable/has-pending-exact",
          !claimed,
          "has_pending() was true, yet the next poll() yielded None",
        );
        break;
      }
    }
  }
  report.require(
    "stable/poll-no-spurious-error",
    faulted.is_none(),
    format!(
      "poll() reported a store fault during a conforming sequence: {}",
      faulted.clone().unwrap_or_default()
    ),
  );
  out
}

/// Exercise the chunked-snapshot staging accumulator: contiguity, idempotence, consumption, and
/// the explicit discard.
fn staging<S>(store: &mut S, report: &mut Report, meta: &SnapshotMeta<S::NodeId>)
where
  S: StableStore,
  S::Error: core::fmt::Debug,
  S::NodeId: Clone,
{
  let blob = Bytes::from_static(b"0123456789");
  let total = blob.len() as u64;
  store.discard_snapshot_staging();
  let first = store.accept_snapshot_chunk(meta, total, 0, &blob.slice(0..4));
  report.require(
    "stable/staging-reports-contiguous",
    matches!(first, Ok(4)),
    format!("staging [0,4) leaves the contiguous watermark at 4, got {first:?}"),
  );
  let gap = store.accept_snapshot_chunk(meta, total, 6, &blob.slice(6..10));
  report.require(
    "stable/staging-reports-contiguous",
    matches!(gap, Ok(4)),
    format!("a gap at [4,6) holds the contiguous watermark at 4, got {gap:?}"),
  );
  report.require(
    "stable/staging-incomplete-is-not-takeable",
    store.take_staged_snapshot(meta).is_none(),
    "a partially staged blob must not be handed back as complete",
  );
  let again = store.accept_snapshot_chunk(meta, total, 0, &blob.slice(0..4));
  report.require(
    "stable/staging-idempotent",
    matches!(again, Ok(4)),
    format!("a re-delivered chunk must not regress the watermark, got {again:?}"),
  );
  let filled = store.accept_snapshot_chunk(meta, total, 4, &blob.slice(4..6));
  report.require(
    "stable/staging-reports-contiguous",
    matches!(filled, Ok(n) if n == total),
    format!("filling the gap completes the run, got {filled:?}"),
  );
  report.require(
    "stable/staging-hands-back-the-blob",
    store.take_staged_snapshot(meta).as_ref() == Some(&blob),
    "a complete staging must hand back exactly the bytes it accumulated",
  );
  report.require(
    "stable/staging-consumed-on-take",
    store.take_staged_snapshot(meta).is_none(),
    "taking a staged blob must consume it, so a second take finds nothing",
  );
  let restaged = store.accept_snapshot_chunk(meta, total, 0, &blob.slice(0..4));
  report.require(
    "stable/staging-reports-contiguous",
    matches!(restaged, Ok(4)),
    format!("a fresh transfer stages from zero, got {restaged:?}"),
  );
  store.discard_snapshot_staging();
  report.require(
    "stable/staging-discard-frees-the-partial",
    store.take_staged_snapshot(meta).is_none(),
    "an explicitly discarded staging must leave nothing behind",
  );

  // KEYED BY ITS BOUNDARY. Every call above names the same meta, which a single global
  // accumulator that ignores the key answers correctly throughout — and then hands one transfer's
  // bytes to a different snapshot's take.
  let elsewhere = SnapshotMeta::new(
    meta.last_index().next().next(),
    meta.last_term(),
    meta.conf().clone(),
  );
  store.discard_snapshot_staging();
  let staged = store.accept_snapshot_chunk(meta, total, 0, &blob);
  report.require(
    "stable/staging-is-keyed-by-its-boundary",
    matches!(staged, Ok(n) if n == total) && store.take_staged_snapshot(&elsewhere).is_none(),
    format!(
      "a COMPLETE staging for one boundary was handed back to a take naming a different one \
       ({staged:?}). A snapshot install would then commit bytes belonging to another boundary \
       entirely"
    ),
  );
  report.require(
    "stable/staging-is-keyed-by-its-boundary",
    store.take_staged_snapshot(meta).as_ref() == Some(&blob),
    "the take naming the RIGHT boundary must still find the staging the foreign take could not",
  );

  // NEVER PAST THE DECLARED LENGTH. The offsets and lengths come from a peer, so a run that
  // reaches beyond `total_len` must be bounded rather than accumulated: the watermark the store
  // reports is what the core answers the sender with.
  store.discard_snapshot_staging();
  let _ = store.accept_snapshot_chunk(meta, total, 0, &blob.slice(0..4));
  for (label, offset, chunk) in [
    (
      "a chunk starting exactly at the declared end",
      total,
      blob.slice(0..4),
    ),
    (
      "a chunk starting past the declared end",
      total + 4,
      blob.slice(0..4),
    ),
    (
      "a chunk running past the declared end",
      total - 2,
      blob.slice(0..6),
    ),
  ] {
    let answer = store.accept_snapshot_chunk(meta, total, offset, &chunk);
    report.require(
      "stable/staging-never-runs-past-the-declared-length",
      matches!(answer, Ok(n) if n <= total),
      format!(
        "{label} answered {answer:?} against a declared length of {total}. A watermark above the \
         declared length is one the sender reads as \"more delivered than exists\""
      ),
    );
  }
  store.discard_snapshot_staging();
}

/// Every check this suite is responsible for reaching.
const REQUIRED: &[&str] = &[
  "stable/completion-exactly-once",
  "stable/completion-names-an-accepted-write",
  "stable/durable-hard-state-agrees-with-the-durable-reader",
  "stable/durable-snapshot-advances-at-the-completion",
  "stable/durable-snapshot-blob-is-verbatim",
  "stable/durable-snapshot-is-never-the-visible-slot",
  "stable/founding-gen-round-trips",
  "stable/fresh-subject",
  "stable/hard-state-advances-at-the-barrier",
  "stable/hard-state-is-a-state-that-was-written",
  "stable/hard-state-is-the-acknowledged-write",
  "stable/hard-state-is-last-durable",
  "stable/hard-state-lineage-round-trips",
  "stable/has-pending-exact",
  "stable/lease-support-round-trips",
  "stable/meta-fidelity",
  "stable/poll-no-spurious-error",
  "stable/snapshot-chunk-absent-is-none",
  "stable/snapshot-chunk-eof-is-empty",
  "stable/snapshot-chunk-is-an-aligned-prefix",
  "stable/snapshot-is-visible-at-submit",
  "stable/staging-consumed-on-take",
  "stable/staging-discard-frees-the-partial",
  "stable/staging-hands-back-the-blob",
  "stable/staging-idempotent",
  "stable/staging-incomplete-is-not-takeable",
  "stable/staging-is-keyed-by-its-boundary",
  "stable/staging-never-runs-past-the-declared-length",
  "stable/staging-reports-contiguous",
];

/// Only the optional `durable_hard_state` probe may go unasked.
const SKIPPABLE: &[&str] = &["stable/durable-hard-state-agrees-with-the-durable-reader"];

/// Check a [`StableStore`] against the durable/visible split, the meta-fidelity contract, and the
/// chunked-read and staging contracts.
///
/// The subject must hand back a FRESH store; the suite says so and stops if it does not.
pub fn stable_store<S>(subject: &mut S) -> Report
where
  S: StableSubject,
  <S::Stable as StableStore>::Error: core::fmt::Debug,
  <S::Stable as StableStore>::NodeId: Clone,
{
  let mut report = Report::new();
  let voter = subject.node_id(1);
  let candidate = subject.node_id(2);
  // The remaining ids the joint configuration below needs, bound before the store borrow.
  let joint_learner = subject.node_id(3);
  // `learners_next` must be an OUTGOING-ONLY voter, so the staged demotion is a member of the
  // outgoing half; a value outside it makes the configuration one no cluster could install.
  let next_learner = subject.node_id(6);
  let outgoing = [subject.node_id(4), subject.node_id(5), next_learner.clone()];
  let store = subject.stable();
  let fresh = store.hard_state() == HardState::initial()
    && store.snapshot().is_none()
    && store.durable_snapshot().is_none();
  report.require(
    "stable/fresh-subject",
    fresh,
    "a fresh store reads the initial hard state and holds neither snapshot slot",
  );
  if !fresh {
    return report;
  }
  report.require(
    "stable/has-pending-exact",
    !store.has_pending(),
    "a fresh store has nothing to poll",
  );
  report.require(
    "stable/snapshot-chunk-absent-is-none",
    store.snapshot_chunk(0, 4).is_none(),
    "with no snapshot, snapshot_chunk must answer None exactly as snapshot() does",
  );

  // hard_state() is the LAST-DURABLE reader. A store that has accepted a write but enqueued no
  // completion has not proven anything durable, so it must still report the prior state — a gate
  // released on a submit-visible read is released on a write a crash erases.
  // Every field family carries a distinct non-default value, the lineage token included: restart
  // reconciliation compares that token against the durable snapshot's, so a store that drops it
  // turns an adopted node's restart into a lineage mismatch — and a fixture built from defaults
  // cannot tell such a store from a faithful one.
  let written = HardState::initial()
    .with_term(Term::new(4))
    .with_vote(Some(candidate.clone()))
    .with_commit(Index::new(2))
    .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(250))))
    .with_lineage(Some(sailing_proto::ForkId::new(
      Bytes::from_static(b"stable-parent"),
      3,
      Index::new(11),
      Term::new(2),
      Bytes::from_static(b"stable-child"),
      4,
    )))
    .with_founding_gen(17);
  store.submit_write(OpId::new(1), written.clone());
  // PER OPERATION, AND ONLY FROM ITS OWN COMPLETION. `has_pending` reports a READY QUEUE, not
  // durability, so a store whose completion becomes pollable before its fsync lands answers `true`
  // here while nothing is durable at all — and skipping the check on that answer certifies exactly
  // that store. A completion is the store's own CLAIM that the write is durable, so the reader must
  // already agree with it; absent one, the reader must still show the state a crash would leave.
  //
  // One boolean for the whole store would also mask a MIXED one — synchronous hard-state writes
  // beside asynchronous snapshots — by letting the first class's answer skip the second's check.
  // What the store has been handed, in submission order — the completions it may release, and the
  // sequence it owes them back in.
  let submitted_write = [StableDone::Wrote(OpId::new(1))];
  let submitted_both = [
    StableDone::Wrote(OpId::new(1)),
    StableDone::SnapshotWritten(OpId::new(2)),
  ];
  let mut consumed = drain_validating(store, &mut report, &written, None, &submitted_write);
  if !consumed.contains(&StableDone::Wrote(OpId::new(1))) {
    report.require(
      "stable/hard-state-is-last-durable",
      store.hard_state() == HardState::initial(),
      format!(
        "hard_state() moved to {:?} on a write no completion has proven durable",
        store.hard_state()
      ),
    );
  }
  if let Some(probe) = store.durable_hard_state() {
    report.require(
      "stable/durable-hard-state-agrees-with-the-durable-reader",
      probe == store.hard_state(),
      format!(
        "durable_hard_state() answered {probe:?} while hard_state() — the other durable reader — \
         reads {:?}; both describe the state a crash right now would leave behind",
        store.hard_state()
      ),
    );
  } else {
    report.skip(
      "stable/durable-hard-state-agrees-with-the-durable-reader",
      "the store does not offer the durable_hard_state probe",
    );
  }

  // The snapshot slots are the sharpest visible/durable split in the trait: `snapshot()` serves
  // immediately, `durable_snapshot()` only once the blob is on stable storage, because the core
  // re-baselines a log against it.
  let meta = SnapshotMeta::new(
    Index::new(9),
    Term::new(3),
    // GENUINELY JOINT: an `auto_leave` flag over empty outgoing and next sets is not a joint
    // configuration, and a store that drops those two fields would round-trip it perfectly.
    // Distinct values from the serialization and engine suites', so a copy-paste cannot mask a drop.
    ConfState::new(
      [voter.clone(), candidate.clone()],
      [joint_learner],
      outgoing,
      [next_learner],
      true,
    ),
  )
  .with_max_lease_window(1_234)
  .with_max_wall_plus_window(5_678)
  .with_max_unwalled_lease_window(9_012)
  .with_read_only(sailing_proto::ReadOnlyOption::LeaseGuard)
  .with_shape_gen(6)
  .with_fork_id(sailing_proto::ForkId::new(
    Bytes::from_static(b"parent"),
    2,
    Index::new(5),
    Term::new(2),
    Bytes::from_static(b"child"),
    6,
  ));
  assert!(
    meta.conf().is_valid(),
    "the joint fixture must be an installable configuration: {:?}",
    meta.conf()
  );
  let blob = Bytes::from_static(b"snapshot-bytes");
  store.submit_snapshot(OpId::new(2), meta.clone(), blob.clone());
  match store.snapshot() {
    Some((seen, data)) => {
      report.require(
        "stable/snapshot-is-visible-at-submit",
        data == blob,
        "the submitted blob must be readable immediately for serving",
      );
      report.require(
        "stable/meta-fidelity",
        seen == meta,
        format!(
          "snapshot() must hand the meta back VERBATIM — an identity match plus a shape_gen check \
           still accepts a meta whose lease windows, read mode or configuration were rebuilt from \
           defaults. Submitted {meta:?}, got {seen:?}"
        ),
      );
    }
    None => report.require(
      "stable/snapshot-is-visible-at-submit",
      false,
      "submit_snapshot must make its blob readable before the write is durable",
    ),
  }
  // The snapshot class is judged on ITS OWN completion, independently of the hard-state class
  // above: a store may be synchronous in one and staged in the other, and each owes the same rule.
  consumed.extend(drain_validating(
    store,
    &mut report,
    &written,
    Some(&meta),
    &submitted_both,
  ));
  if !consumed.contains(&StableDone::SnapshotWritten(OpId::new(2))) {
    report.require(
      "stable/durable-snapshot-is-never-the-visible-slot",
      store.durable_snapshot().is_none(),
      format!(
        "durable_snapshot() answered {:?} for a blob no completion has proven durable; the core \
         re-baselines a log against this answer, so serving the visible slot here lets a crash \
         orphan the log",
        store.durable_snapshot()
      ),
    );
  }

  // A resident store slices its own blob: a prefix at the offset, clamped at the end, and the
  // benign empty tail past it.
  match store.snapshot_chunk(1, 4) {
    Some(Ok((chunk_meta, total, SnapshotChunkRead::Ready(chunk)))) => {
      report.require(
        "stable/snapshot-chunk-is-an-aligned-prefix",
        total == blob.len() as u64 && chunk == blob.slice(1..5),
        format!("snapshot_chunk(1, 4) must be [1,5) of a {total}-byte blob, got {chunk:?}"),
      );
      report.require(
        "stable/meta-fidelity",
        chunk_meta == meta,
        format!("a chunk read carries the same VERBATIM meta as snapshot(); got {chunk_meta:?}"),
      );
    }
    _ => report.require(
      "stable/snapshot-chunk-is-an-aligned-prefix",
      false,
      "a resident chunk read must answer Ready with the blob's total length, never Pending or Err",
    ),
  }
  // A STRADDLING READ. Every read above ends inside the blob, so a store that ignores `len` and
  // returns the whole tail — or one that refuses any range it cannot satisfy in full — was never
  // separated from a conforming one. A peer asks for more than remains on the last chunk of every
  // transfer.
  match store.snapshot_chunk(blob.len() as u64 - 2, 100) {
    Some(Ok((_, total, SnapshotChunkRead::Ready(chunk)))) => report.require(
      "stable/snapshot-chunk-is-an-aligned-prefix",
      total == blob.len() as u64 && chunk == blob.slice(blob.len() - 2..),
      format!(
        "a read of 100 bytes starting 2 from the end must answer exactly those 2 bytes of a \
         {total}-byte blob, got {chunk:?}"
      ),
    ),
    _ => report.require(
      "stable/snapshot-chunk-is-an-aligned-prefix",
      false,
      "a read straddling the end of a resident blob must be Ready with what remains",
    ),
  }

  // At EOF, one past it, and at the top of the offset space. Testing only the exact boundary left
  // every arithmetic mistake beyond it — a subtraction that underflows, a range that inverts —
  // outside the suite, and a chunked transfer walks a cursor a peer supplies.
  for offset in [blob.len() as u64, blob.len() as u64 + 1, u64::MAX] {
    match store.snapshot_chunk(offset, 4) {
      Some(Ok((_, _, SnapshotChunkRead::Ready(chunk)))) => report.require(
        "stable/snapshot-chunk-eof-is-empty",
        chunk.is_empty(),
        format!("snapshot_chunk({offset}, 4) past a {}-byte blob must be the benign empty tail, got {chunk:?}", blob.len()),
      ),
      other => report.require(
        "stable/snapshot-chunk-eof-is-empty",
        false,
        format!(
          "snapshot_chunk({offset}, 4) answered {}. An over-cursor must degenerate to \
           Ready(empty), never Pending, Err or None — the offset comes from a PEER, and a store \
           that faults on it turns a mistimed transfer into a stalled one",
          match other {
            None => "None",
            Some(Err(_)) => "an error",
            Some(Ok(_)) => "Pending",
          }
        ),
      ),
    }
  }

  subject.barrier();
  let store = subject.stable();
  // Completions consumed BEFORE the barrier count once, here: a store settling a class at submit
  // owes exactly the same one completion a staged store owes at its barrier.
  let mut done = consumed;
  done.extend(drain_validating(
    store,
    &mut report,
    &written,
    Some(&meta),
    &submitted_both,
  ));
  report.require(
    "stable/completion-exactly-once",
    done == submitted_both,
    format!(
      "each accepted write completes exactly once and IN SUBMIT ORDER — the contract says the \
       completions are ordered, and an unordered membership check passes {:?} just as happily as \
       the sequence the store actually owes. Expected {submitted_both:?}, got {done:?}",
      [
        StableDone::SnapshotWritten(OpId::new(2)),
        StableDone::Wrote(OpId::new(1))
      ]
    ),
  );
  report.require(
    "stable/hard-state-advances-at-the-barrier",
    store.hard_state() == written,
    format!(
      "after the barrier the durable reader must show the written state, got {:?}",
      store.hard_state()
    ),
  );
  report.require(
    "stable/lease-support-round-trips",
    store.hard_state().lease_support() == LeaseSupport::Recorded(Some(Duration::from_millis(250))),
    "the three-valued lease_support must survive the store verbatim",
  );
  report.require(
    "stable/founding-gen-round-trips",
    store.hard_state().founding_gen() == 17,
    format!(
      "the founding generation must survive the store verbatim: it is the one durable per-replica \
       value a restart recovers a lineage counter from, so a store that drops it leaves a \
       recreated group minting beneath the generation it was admitted at. Got {}",
      store.hard_state().founding_gen()
    ),
  );
  report.require(
    "stable/hard-state-lineage-round-trips",
    store.hard_state().lineage() == written.lineage(),
    format!(
      "the hard state's lineage token must survive the store verbatim: restart reconciliation \
       compares it against the durable snapshot's, so dropping it turns an adopted node's restart \
       into a lineage mismatch. Got {:?}",
      store.hard_state().lineage()
    ),
  );
  match store.durable_snapshot() {
    Some(durable) => report.require(
      "stable/durable-snapshot-advances-at-the-completion",
      durable == meta,
      format!(
        "the durable slot must carry the submitted meta VERBATIM — the core compares it by \
         identity, but a slot that survived with defaulted lease windows or read mode would \
         under-size a successor's commit-wait while still comparing equal. Got {durable:?}"
      ),
    ),
    None => report.require(
      "stable/durable-snapshot-advances-at-the-completion",
      false,
      "durable_snapshot() must advance at the point the SnapshotWritten completion becomes true",
    ),
  }
  // THE BYTES, RE-READ AFTER THE BARRIER. Everything above compares METADATA, and a store that
  // hands back the submitted blob at submit can persist something else beneath the same meta: the
  // slot then looks perfect while the blob a restart decodes, and the blob a peer is served during
  // an install, is not the one anybody wrote. Nothing before this point looks at the bytes again.
  match store.snapshot() {
    Some((slot_meta, slot_blob)) => {
      report.require(
        "stable/durable-snapshot-blob-is-verbatim",
        slot_meta == meta && slot_blob == blob,
        format!(
          "after the barrier the serving slot holds {slot_meta:?} over {slot_blob:?} where the \
           submitted snapshot is {meta:?} over {blob:?}. The meta is what the core compares and \
           the BYTES are what it installs, so a slot that keeps one and replaces the other poisons \
           the next restart on decode or restores a state nobody captured"
        ),
      );
    }
    None => report.require(
      "stable/durable-snapshot-blob-is-verbatim",
      false,
      "the serving slot came back empty after the barrier that made the snapshot durable",
    ),
  }
  // AND THROUGH THE CHUNKED PATH, which is the one a peer is actually served from: the whole blob
  // in one read, a straddling read at the end, and the declared total length.
  let total = blob.len() as u64;
  for (label, offset, len) in [
    ("the whole blob", 0u64, total),
    ("a straddling tail read", total.saturating_sub(2), 64),
  ] {
    match store.snapshot_chunk(offset, len) {
      Some(Ok((chunk_meta, declared, SnapshotChunkRead::Ready(chunk)))) => {
        let start = offset.min(total) as usize;
        let end = offset.saturating_add(len).min(total) as usize;
        report.require(
          "stable/durable-snapshot-blob-is-verbatim",
          chunk_meta == meta && declared == total && chunk == blob.slice(start..end),
          format!(
            "[{label}] the chunked path answered {chunk_meta:?} over {chunk:?} of a declared \
             {declared}-byte blob, where the submitted bytes at [{start}, {end}) are {:?} of \
             {total}. This is the read a peer's InstallSnapshot is served from",
            blob.slice(start..end)
          ),
        );
      }
      other => report.require(
        "stable/durable-snapshot-blob-is-verbatim",
        false,
        format!(
          "[{label}] the chunked path answered {} for a resident durable snapshot",
          match other {
            None => "None",
            Some(Err(_)) => "an error",
            Some(Ok(_)) => "Pending",
          }
        ),
      ),
    }
  }
  if let Some(probe) = store.durable_hard_state() {
    report.require(
      "stable/durable-hard-state-agrees-with-the-durable-reader",
      probe == store.hard_state(),
      format!("durable_hard_state() {probe:?} disagrees with hard_state() after a barrier"),
    );
  }

  // The staged meta carries a FORK TOKEN. It is the field the trait doc says a store that rebuilds
  // a meta rather than keeping it will drop, and it is part of `identity_eq` — so a staging keyed
  // on a rebuilt meta stops matching the transfer that filled it.
  let staged_meta = SnapshotMeta::new(
    Index::new(30),
    Term::new(5),
    ConfState::from_voters([voter]),
  )
  .with_shape_gen(7)
  .with_fork_id(sailing_proto::ForkId::new(
    Bytes::from_static(b"staged-parent"),
    5,
    Index::new(29),
    Term::new(4),
    Bytes::from_static(b"staged-child"),
    6,
  ));
  // TWO OUTSTANDING WRITES. With one in flight, "the last durable state" and "the latest state
  // submitted" are the same value, so a store that MERGES the fields of everything pending —
  // taking the newest term beside an older vote — answers correctly by coincidence. Whatever the
  // reader shows when a completion is consumed, it must be a state somebody actually wrote.
  {
    let first = HardState::initial()
      .with_term(Term::new(9))
      .with_vote(Some(candidate.clone()))
      .with_commit(Index::new(3))
      .with_founding_gen(21);
    let second = HardState::initial()
      .with_term(Term::new(10))
      .with_commit(Index::new(4))
      .with_founding_gen(22);
    let store = subject.stable();
    store.submit_write(OpId::new(5), first.clone());
    store.submit_write(OpId::new(6), second.clone());
    subject.barrier();
    let store = subject.stable();
    let mut delivered = Vec::new();
    let mut faulted: Option<String> = None;
    loop {
      // SAMPLED BEFORE EVERY POLL, both directions. `has_pending` is the driver's only signal that
      // a poll is worth making — `handle_storage` reads it, answers Drained, and sleeps — so a
      // store whose readiness flag tracks one completion class and not another strands the class
      // it forgot until unrelated work happens to wake the driver. The mixed phase above cannot
      // see that: a snapshot pending there keeps the flag true and masks a hard-state completion
      // the flag never counted.
      let claimed = store.has_pending();
      match store.poll() {
        Some(Ok(done)) => {
          report.require(
            "stable/has-pending-exact",
            claimed,
            format!("has_pending() was false, yet the next poll() yielded {done:?}"),
          );
          let reading = store.hard_state();
          report.require(
            "stable/hard-state-is-a-state-that-was-written",
            reading == first || reading == second,
            format!(
              "at the moment {done:?} was consumed hard_state() read {reading:?}, which is \
               neither of the two states submitted ({first:?}, {second:?}). A reader assembled \
               from the fields of several pending writes reports a state no writer ever produced \
               — a term from one and a vote from another is exactly a double vote"
            ),
          );
          // THE SECOND WRITE'S OWN COMPLETION, judged against the SECOND state and nothing else.
          // Accepting either state here let a store persist only the first, acknowledge both, and
          // lose an acknowledged term, vote, commit and founding generation — which after a crash
          // is a node free to vote again in a term it already voted in.
          if done == StableDone::Wrote(OpId::new(6)) {
            report.require(
              "stable/hard-state-is-the-acknowledged-write",
              reading == second,
              format!(
                "a `Wrote` for the LATEST write is a claim that THAT state is durable, yet at the \
                 moment it was consumed hard_state() read {reading:?} instead of {second:?}"
              ),
            );
          }
          delivered.push(done);
        }
        Some(Err(e)) => {
          faulted = Some(format!("{e:?}"));
          break;
        }
        None => {
          report.require(
            "stable/has-pending-exact",
            !claimed,
            "has_pending() was true, yet the next poll() yielded None",
          );
          break;
        }
      }
    }
    report.require(
      "stable/poll-no-spurious-error",
      faulted.is_none(),
      format!(
        "poll() reported a store fault while draining two outstanding writes: {}",
        faulted.clone().unwrap_or_default()
      ),
    );
    // BOTH, IN SUBMISSION ORDER. A drain that stops early leaves an acknowledged write unproven,
    // and a completion order that inverts the writes tells the core the older state is the newer
    // one.
    report.require(
      "stable/completion-exactly-once",
      delivered
        == [
          StableDone::Wrote(OpId::new(5)),
          StableDone::Wrote(OpId::new(6)),
        ],
      format!(
        "two writes behind one barrier owe exactly their two completions in submission order; got \
         {delivered:?}"
      ),
    );
    // AND WHEN THE DUST SETTLES. The reader that every restart and every gate consults must be the
    // last state acknowledged, not merely one of the states submitted.
    let settled = subject.stable().hard_state();
    report.require(
      "stable/hard-state-is-the-acknowledged-write",
      settled == second,
      format!(
        "after both writes completed, hard_state() reads {settled:?} where the last acknowledged \
         write is {second:?}"
      ),
    );
  }

  staging(subject.stable(), &mut report, &staged_meta);

  report.require_coverage(REQUIRED, SKIPPABLE);
  report
}
