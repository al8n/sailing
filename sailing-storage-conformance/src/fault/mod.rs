//! Reference fault types: the crash seam a durable store is tested over, the completion-delivery
//! faults an async store must tolerate, and the reference implementations that show what a
//! conforming answer looks like.

mod completion;
pub use completion::{CompletionFaults, FaultyLog, FaultyStable, prior_incarnation_op_id};

mod codec;
pub use codec::{DecodeFault, ReferenceCodec, crc32};

mod journal;
pub use journal::{
  JournalDefects, JournalEngine, JournalEngineSubject, JournalFraming, JournalLog,
  JournalPersistence, JournalRecovery, JournalStable, JournalStorageError,
};

mod probe;
pub use probe::{
  ProbingLog, ProbingLogSubject, ProbingStable, ProbingStableSubject, StagingUnallocatable,
};

mod reference;
pub use reference::{ReferenceEngineSubject, ReferenceLogSubject, ReferenceStableSubject};

mod vfs;
pub use vfs::{CrashClass, Device, DeviceError, SharedVfs, Vfs, VfsDevice};

/// Mint a log entry whose lineage move names `generation` for its OWN group — what
/// [`EngineSubject::shape_entry`](crate::check::EngineSubject::shape_entry) asks a subject for.
///
/// A SPLIT carrying `parent_gen_after = generation`: of the four shape kinds it is the one whose
/// move belongs to the log that carries it with no second participant to model, and
/// [`shape_entry_move`](sailing_proto::shape_entry_move) — the leg every engine folds — reads
/// exactly that field for this kind. Both reference subjects mint through here so the two tiers are
/// asked the same question.
///
/// The payload encoder is the crate's off-by-default harness seam: minting a shape payload is the
/// consensus core's business everywhere else, because a hand-made one names a lineage move nobody
/// made.
#[must_use]
pub fn mint_shape_entry(
  term: sailing_proto::Term,
  index: sailing_proto::Index,
  generation: u64,
) -> sailing_proto::Entry {
  let payload = sailing_proto::SplitPayload::new(
    bytes::Bytes::from_static(b"child"),
    1,
    generation,
    bytes::Bytes::new(),
  );
  sailing_proto::Entry::new(
    term,
    index,
    sailing_proto::EntryKind::Split,
    sailing_proto::fuzz_internals::shape_payload::split(&payload),
  )
}

/// Which malformed shape entry [`mint_invalid_shape_entry`] builds — one per way an entry of a
/// shape kind fails the apply path's own admission, and so per way the removal ceiling's log leg
/// meets one it must not fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvalidShapeEntry {
  /// A `Split` payload whose child id is EMPTY. Its decoder refuses exactly that (the child id
  /// carries the group-tag wire bound), so the refusal is deterministic and needs no corrupted
  /// bytes to arrange.
  EmptyChildId,
  /// A valid `Split` encode cut short of its last byte — the corruption a torn record or a bad
  /// medium actually produces, rather than one the encoder was asked for.
  TruncatedEncoding,
  /// A well-formed `Split` whose OWN move — `parent_gen_after`, the field this group's ceiling
  /// folds — sits at [`HIGHEST_WORKING_GENERATION`](sailing_proto::HIGHEST_WORKING_GENERATION), in
  /// the reserved band. Folded, the ceiling's `+ 1` lands on the terminal
  /// [`MERGED_FLOOR`](sailing_proto::MERGED_FLOOR).
  ReservedOwnGeneration,
  /// A well-formed `Split` whose `child_gen` is reserved while `parent_gen_after` is an ordinary
  /// working generation. The apply path validates BOTH fields and poisons on either, so this entry
  /// will never apply — and a leg that checked only the field it folds would let it fence a live
  /// id at a perfectly plausible-looking generation.
  ReservedChildGeneration,
}

/// Mint a log entry of a SHAPE kind that no conforming replica can apply — what the removal
/// ceiling's log leg meets in the append-before-apply window, where nothing has refused it yet.
///
/// CENTRAL, not an [`EngineSubject`](crate::check::EngineSubject) hook: an invalid shape entry is
/// invalid for every engine, so there is nothing for a subject to decide and a subject hook would
/// only re-open a hole for one to decline the question through.
///
/// The generation the reserved forms name is `HIGHEST_WORKING_GENERATION` itself — the bottom of
/// the reserved band, so an engine that folded it would answer the terminal `MERGED_FLOOR` exactly,
/// the forged global verdict this leg exists to make impossible.
#[must_use]
pub fn mint_invalid_shape_entry(
  form: InvalidShapeEntry,
  term: sailing_proto::Term,
  index: sailing_proto::Index,
) -> sailing_proto::Entry {
  let child = bytes::Bytes::from_static(b"child");
  let payload = match form {
    InvalidShapeEntry::EmptyChildId => {
      sailing_proto::SplitPayload::new(bytes::Bytes::new(), 1, 7, bytes::Bytes::new())
    }
    InvalidShapeEntry::TruncatedEncoding => {
      sailing_proto::SplitPayload::new(child, 1, 7, bytes::Bytes::new())
    }
    InvalidShapeEntry::ReservedOwnGeneration => sailing_proto::SplitPayload::new(
      child,
      1,
      sailing_proto::HIGHEST_WORKING_GENERATION,
      bytes::Bytes::new(),
    ),
    InvalidShapeEntry::ReservedChildGeneration => sailing_proto::SplitPayload::new(
      child,
      sailing_proto::HIGHEST_WORKING_GENERATION,
      7,
      bytes::Bytes::new(),
    ),
  };
  let mut data = sailing_proto::fuzz_internals::shape_payload::split(&payload);
  if form == InvalidShapeEntry::TruncatedEncoding {
    data.truncate(data.len() - 1);
  }
  sailing_proto::Entry::new(term, index, sailing_proto::EntryKind::Split, data)
}
