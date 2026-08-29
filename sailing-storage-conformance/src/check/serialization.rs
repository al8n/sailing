//! The serialization-fidelity suite.
//!
//! Every check here is about a field whose loss produces NO error: a snapshot that never installs
//! because its restored meta compares unequal to itself, a chunked transfer that restarts on every
//! chunk, a generation floor admitting against a wrong counter, a post-upgrade restart that is
//! quietly less safe than the run before it. A store that keeps these types by value passes for
//! free; one that persists them has to earn it.

use super::{Codec, Report};
use core::time::Duration;
use sailing_proto::{
  ConfState, ForkId, HardState, Index, LeaseSupport, ReadOnlyOption, SnapshotMeta, Term,
};
use std::{format, vec::Vec};

/// Every check this suite is responsible for reaching.
const REQUIRED: &[&str] = &[
  "serde/a-corrupted-record-never-decodes",
  "serde/complete-input-still-decodes",
  "serde/founding-gen-verbatim",
  "serde/hard-state-fields-verbatim",
  "serde/hard-state-lineage-verbatim",
  "serde/lease-support-promise-verbatim",
  "serde/legacy-bytes-are-not-the-current-format",
  "serde/legacy-decodes-unrecorded",
  "serde/legacy-founds-at-zero",
  "serde/legacy-keeps-the-other-fields",
  "serde/meta-boundary-verbatim",
  "serde/meta-configuration-verbatim",
  "serde/meta-fork-id-verbatim",
  "serde/meta-identity-preserved",
  "serde/meta-lease-windows-verbatim",
  "serde/meta-shape-gen-verbatim",
  "serde/meta-simple-configuration-verbatim",
  "serde/recorded-none-stays-recorded",
  "serde/trailing-bytes-never-decode",
  "serde/truncated-input-never-decodes",
  "serde/unrecorded-stays-unrecorded",
];

/// Checks a codec may legitimately leave unasked — all three legacy legs, and only when it cannot
/// produce pre-format bytes at all.
const SKIPPABLE: &[&str] = &[
  "serde/legacy-bytes-are-not-the-current-format",
  "serde/legacy-decodes-unrecorded",
  "serde/legacy-founds-at-zero",
  "serde/legacy-keeps-the-other-fields",
];

/// Check a [`Codec`] against the round-trip contracts on `SnapshotMeta`, `HardState`, and the
/// legacy `lease_support` decode.
pub fn serialization<C>(codec: &C) -> Report
where
  C: Codec,
  C::NodeId: Clone,
{
  let mut report = Report::new();
  let voter = codec.node_id(1);
  let fork = codec.fork_id();
  // A SECOND, DISTINCT token for the hard state. The meta's `fork_id` and the hard state's
  // `lineage` are the same TYPE, and giving both the same value let a codec that memoises one
  // token — or wires the two fields together — satisfy both checks with a single round trip.
  let hard_state_fork = ForkId::new(
    bytes::Bytes::from_static(b"conformance-hard-state-parent"),
    23,
    Index::new(71),
    Term::new(9),
    bytes::Bytes::from_static(b"conformance-hard-state-child"),
    24,
  );

  // A VALID JOINT CONFIGURATION, because the joint fields are the ones a codec drops for free.
  // With `voters_outgoing` and `learners_next` empty and `auto_leave` false, a codec that persists
  // the incoming sets alone satisfies a whole-configuration equality — and a snapshot restored from
  // it during joint consensus discards the OUTGOING quorum, leaving the replica to judge elections
  // and commits under the incoming half by itself. The values are deliberately distinct from the
  // stable and engine suites' joint fixtures, so a copy-paste between suites cannot mask a drop.
  let joint = ConfState::new(
    [voter.clone(), codec.node_id(2), codec.node_id(3)],
    [codec.node_id(4)],
    [codec.node_id(5), codec.node_id(6), codec.node_id(7)],
    [codec.node_id(7)],
    true,
  );
  // AN INSTALLABLE CONFIGURATION, not merely a populated one. `learners_next` are OUTGOING-ONLY
  // staged demotions — a member that leaves on `leave_joint` and becomes a learner — so each must
  // be an outgoing voter and must not be an incoming one. A fixture that breaks the invariant is
  // not a snapshot any cluster could have taken: a validating codec would be falsely rejected by
  // it, and the drop mutants below would be proving fidelity against a shape nothing can install.
  assert!(
    joint.is_valid(),
    "the joint fixture must be an installable configuration: {joint:?}"
  );
  let meta = SnapshotMeta::new(Index::new(41), Term::new(6), joint.clone())
    .with_max_lease_window(1_234)
    .with_max_wall_plus_window(5_678)
    .with_max_unwalled_lease_window(9_012)
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_shape_gen(11)
    .with_fork_id(fork.clone());

  match codec.decode_snapshot_meta(&codec.encode_snapshot_meta(&meta)) {
    Some(back) => {
      report.require(
        "serde/meta-shape-gen-verbatim",
        back.shape_gen() == 11,
        format!(
          "shape_gen came back {} instead of 11: the generation floor then admits against the \
           wrong lineage counter",
          back.shape_gen()
        ),
      );
      report.require(
        "serde/meta-fork-id-verbatim",
        back.fork_id() == Some(&fork),
        format!(
          "the lineage token came back {:?} instead of the one submitted. The token is what makes \
           a meta a snapshot's IDENTITY, so dropping it makes a restored meta compare UNEQUAL to \
           the very snapshot it is: a deferred install stalls and a chunked transfer restarts on \
           every chunk, both silently",
          back.fork_id()
        ),
      );
      report.require(
        "serde/meta-identity-preserved",
        back.identity_eq(&meta),
        "a decoded meta must compare identity-equal to the one encoded",
      );
      report.require(
        "serde/meta-boundary-verbatim",
        back.last_index() == meta.last_index() && back.last_term() == meta.last_term(),
        "the snapshot boundary must round-trip",
      );
      report.require(
        "serde/meta-configuration-verbatim",
        back.conf() == &joint,
        format!(
          "the configuration came back {:?} instead of {joint:?}. All FIVE fields are load-bearing: \
           restoring a snapshot taken during joint consensus without its outgoing voters, its \
           next learners, or its auto-leave flag leaves the replica evaluating elections and \
           commits under the incoming configuration alone — a quorum it was never entitled to \
           decide with",
          back.conf()
        ),
      );
      report.require(
        "serde/meta-lease-windows-verbatim",
        back.max_lease_window() == 1_234
          && back.max_wall_plus_window() == 5_678
          && back.max_unwalled_lease_window() == 9_012
          && back.read_only() == Some(ReadOnlyOption::LeaseGuard),
        "a dropped lease window under-sizes a successor's commit-wait after a restart",
      );
    }
    None => report.require(
      "serde/meta-identity-preserved",
      false,
      "the codec could not decode its own encoding of a snapshot meta",
    ),
  }

  // AND THE PLAIN SHAPE, retained: a codec that fabricates joint fields where none were written is
  // the same defect pointing the other way — a restored replica would wait on an outgoing quorum
  // that does not exist.
  let simple = SnapshotMeta::new(
    Index::new(9),
    Term::new(2),
    ConfState::from_voters([voter.clone(), codec.node_id(2)]),
  );
  match codec.decode_snapshot_meta(&codec.encode_snapshot_meta(&simple)) {
    Some(back) => report.require(
      "serde/meta-simple-configuration-verbatim",
      back.conf() == simple.conf()
        && back.conf().voters_outgoing().is_empty()
        && back.conf().learners_next().is_empty()
        && !back.conf().auto_leave(),
      format!(
        "a single configuration must come back single; got {:?}",
        back.conf()
      ),
    ),
    None => report.require(
      "serde/meta-simple-configuration-verbatim",
      false,
      "the codec could not decode its own encoding of a single-configuration meta",
    ),
  }

  let hs = HardState::initial()
    .with_term(Term::new(13))
    .with_commit(Index::new(8))
    .with_vote(Some(voter))
    .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(750))))
    .with_lineage(Some(hard_state_fork.clone()))
    .with_founding_gen(31);
  match codec.decode_hard_state(&codec.encode_hard_state(&hs)) {
    Some(back) => {
      report.require(
        "serde/hard-state-lineage-verbatim",
        back.lineage() == Some(&hard_state_fork),
        format!(
          "the hard state's lineage came back {:?}: restart reconciliation compares it against the \
           durable snapshot's token, so dropping it turns an adopted node's restart into a lineage \
           mismatch",
          back.lineage()
        ),
      );
      report.require(
        "serde/hard-state-fields-verbatim",
        back.term() == hs.term() && back.commit() == hs.commit() && back.vote() == hs.vote(),
        "term, vote, and commit must round-trip",
      );
      report.require(
        "serde/founding-gen-verbatim",
        back.founding_gen() == 31,
        format!(
          "the founding generation came back {} instead of 31. It is the only durable per-replica \
           value a restart may recover a lineage counter from — a per-incarnation constant, \
           temporally prior to every entry the incarnation holds — so a store that drops it strands \
           a recreated group's counter beneath the generation it was admitted at",
          back.founding_gen()
        ),
      );
      report.require(
        "serde/lease-support-promise-verbatim",
        back.lease_support() == LeaseSupport::Recorded(Some(Duration::from_millis(750))),
        format!(
          "a recorded promise came back {:?}: the post-restart fence must cover exactly what was \
           promised",
          back.lease_support()
        ),
      );
    }
    None => report.require(
      "serde/hard-state-fields-verbatim",
      false,
      "the codec could not decode its own encoding of a hard state",
    ),
  }

  // Recorded(None) is a POSITIVE claim — "a current-format node promised nothing" — and must not
  // collapse into the legacy reading below.
  let promised_nothing = HardState::<C::NodeId>::initial()
    .with_term(Term::new(2))
    .with_lease_support(LeaseSupport::Recorded(None));
  match codec.decode_hard_state(&codec.encode_hard_state(&promised_nothing)) {
    Some(back) => report.require(
      "serde/recorded-none-stays-recorded",
      back.lease_support() == LeaseSupport::Recorded(None),
      format!(
        "a recorded no-promise came back {:?}; decoding it as Unrecorded would fence a node that \
         needs no fence on every restart",
        back.lease_support()
      ),
    ),
    None => report.require(
      "serde/recorded-none-stays-recorded",
      false,
      "the codec could not decode its own encoding of a recorded no-promise",
    ),
  }

  // Unrecorded is the third reading, and the modern encoder must carry it as such. A codec that
  // treats it as "nothing to write" and reads it back as a recorded promise erases the distinction
  // the two checks above exist to keep.
  let unrecorded = HardState::<C::NodeId>::initial()
    .with_term(Term::new(2))
    .with_lease_support(LeaseSupport::Unrecorded);
  match codec.decode_hard_state(&codec.encode_hard_state(&unrecorded)) {
    Some(back) => report.require(
      "serde/unrecorded-stays-unrecorded",
      back.lease_support() == LeaseSupport::Unrecorded,
      format!(
        "an unrecorded promise came back {:?}. A current-format record that says nothing about a \
         promise must keep saying nothing: reading it as Recorded(None) asserts a promise the \
         writer never made",
        back.lease_support()
      ),
    ),
    None => report.require(
      "serde/unrecorded-stays-unrecorded",
      false,
      "the codec could not decode its own encoding of an unrecorded promise",
    ),
  }

  // A TORN record must be REFUSED, not turned into a value built from the fields that survived.
  // Every default a partial decode supplies is a claim nobody made: `lease_support` defaults to a
  // legacy record and re-fences a node that needs no fence, `shape_gen` to generation zero, a
  // lineage token to absent — and an absent token makes a restored meta compare unequal to the very
  // snapshot it is. A record needs a length or checksum of its own for this to be decidable at all:
  // a bare tagged format cannot tell a prefix that ends on a field boundary from a shorter record a
  // writer meant to write.
  let hs_bytes = codec.encode_hard_state(&hs);
  let meta_bytes = codec.encode_snapshot_meta(&meta);
  let mut accepted_hs = Vec::new();
  for cut in 0..hs_bytes.len() {
    if codec.decode_hard_state(&hs_bytes[..cut]).is_some() {
      accepted_hs.push(cut);
    }
  }
  let mut accepted_meta = Vec::new();
  for cut in 0..meta_bytes.len() {
    if codec.decode_snapshot_meta(&meta_bytes[..cut]).is_some() {
      accepted_meta.push(cut);
    }
  }
  report.require(
    "serde/truncated-input-never-decodes",
    accepted_hs.is_empty() && accepted_meta.is_empty(),
    format!(
      "the codec built a value out of a TRUNCATED record — hard-state cuts {accepted_hs:?} of {}        bytes, snapshot-meta cuts {accepted_meta:?} of {} bytes. A torn blob is the ordinary end of        a crashed medium, and every field the cut removed comes back as a default the writer never        wrote",
      hs_bytes.len(),
      meta_bytes.len()
    ),
  );
  // TRAILING BYTES AND INTERIOR CORRUPTION. The prefix sweep proves a record knows where it ENDS;
  // neither of these does. A record read back from a medium can have another record's bytes behind
  // it (a partially overwritten slot) or a flipped byte inside it, and a decoder that ignores the
  // tail or rebuilds a plausible value from corrupted fields turns either into a state nobody
  // wrote — with no length or checksum able to say so afterwards.
  let mut with_tail = hs_bytes.clone();
  with_tail.extend_from_slice(b"\x00leftovers");
  let mut meta_with_tail = meta_bytes.clone();
  meta_with_tail.extend_from_slice(b"\xff\xff\xff\xff");
  report.require(
    "serde/trailing-bytes-never-decode",
    codec.decode_hard_state(&with_tail).is_none()
      && codec.decode_snapshot_meta(&meta_with_tail).is_none(),
    "a complete record followed by bytes that are not part of it must be REFUSED. Ignoring the \
     tail accepts a slot half-overwritten by a later record as though it were the earlier one",
  );
  let mut corrupted_hs = Vec::new();
  let mut corrupted_meta = Vec::new();
  for at in 0..hs_bytes.len() {
    let mut bytes = hs_bytes.clone();
    bytes[at] ^= 0xff;
    if codec
      .decode_hard_state(&bytes)
      .is_some_and(|back| back != hs)
    {
      corrupted_hs.push(at);
    }
  }
  for at in 0..meta_bytes.len() {
    let mut bytes = meta_bytes.clone();
    bytes[at] ^= 0xff;
    if codec
      .decode_snapshot_meta(&bytes)
      .is_some_and(|back| back != meta)
    {
      corrupted_meta.push(at);
    }
  }
  report.require(
    "serde/a-corrupted-record-never-decodes",
    corrupted_hs.is_empty() && corrupted_meta.is_empty(),
    format!(
      "flipping one byte produced a DIFFERENT value the codec accepted — hard-state offsets \
       {corrupted_hs:?}, snapshot-meta offsets {corrupted_meta:?}. A record needs a checksum of \
       its own to tell corruption from content; without one a flipped bit becomes a term, a vote \
       or a boundary nobody wrote"
    ),
  );

  report.require(
    "serde/complete-input-still-decodes",
    codec.decode_hard_state(&hs_bytes).is_some()
      && codec.decode_snapshot_meta(&meta_bytes).is_some(),
    "the sweep above must not be passing by refusing everything: the COMPLETE records still decode",
  );

  match codec.encode_legacy_hard_state(&hs) {
    Some(legacy) => match codec.decode_hard_state(&legacy) {
      Some(back) => {
        // THE BYTES MUST ACTUALLY BE LEGACY. The whole leg is self-attested: a codec that returns
        // its CURRENT encoding here satisfies all three rules trivially, and the pre-format
        // reading — the one restart where the promise and the founding generation are absent — is
        // never exercised at all.
        report.require(
          "serde/legacy-bytes-are-not-the-current-format",
          legacy != codec.encode_hard_state(&hs),
          "encode_legacy_hard_state returned the codec's CURRENT encoding, so the legacy rules \
           below were checked against a record that carries every modern field",
        );
        report.require(
          "serde/legacy-decodes-unrecorded",
          back.lease_support() == LeaseSupport::Unrecorded,
          format!(
            "a pre-lease_support blob decoded as {:?}. It must be Unrecorded: decoding it as \
             Recorded(None) ASSERTS the old node promised nothing, which reopens the \
             disruptive-vote-inside-a-live-lease hole for one post-upgrade restart of a \
             previously-enforcing node",
            back.lease_support()
          ),
        );
        // THE WHOLE TRANSFORMATION, not two fields of it. A legacy decode changes exactly two
        // things — the promise becomes Unrecorded and the founding generation becomes zero — and
        // everything else must survive verbatim. Comparing only term and commit let a decoder drop
        // the VOTE, which hands the same term a second candidate.
        let expected = hs
          .clone()
          .with_lease_support(LeaseSupport::Unrecorded)
          .with_founding_gen(0);
        report.require(
          "serde/legacy-keeps-the-other-fields",
          back == expected,
          format!(
            "a legacy blob decoded as {back:?} where the pre-format reading of it is {expected:?}. \
             Only the promise and the founding generation may differ"
          ),
        );
        report.require(
          "serde/legacy-founds-at-zero",
          back.founding_gen() == 0,
          format!(
            "a pre-founding_gen blob decoded as founded at {}. Zero is EXACT here rather than \
             merely conservative: the storeless create door admits no other generation, so no \
             writer that predates the field could have founded above it",
            back.founding_gen()
          ),
        );
      }
      None => report.require(
        "serde/legacy-decodes-unrecorded",
        false,
        "the codec could not decode the legacy bytes it produced",
      ),
    },
    None => {
      // ALL THREE legacy legs go unasked together; skipping only the first left the other two
      // silently unreached, which the manifest below would (correctly) call a hole.
      for check in [
        "serde/legacy-bytes-are-not-the-current-format",
        "serde/legacy-decodes-unrecorded",
        "serde/legacy-founds-at-zero",
        "serde/legacy-keeps-the-other-fields",
      ] {
        report.skip(
          check,
          "the codec cannot produce pre-format bytes, so the sharpest rules in this suite are \
           unproven for it",
        );
      }
    }
  }

  report.require_coverage(REQUIRED, SKIPPABLE);
  report
}
