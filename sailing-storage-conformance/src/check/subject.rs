//! The adapters a suite runs against.
//!
//! Every storage contract in sailing is about a DURABILITY EDGE — what is visible before one, what
//! survives past one. The kit can submit work, but only the implementation knows how to reach an
//! edge: which call is its barrier, and what a reopen after a crash means for it. A subject is that
//! knowledge, and nothing else.

use crate::fault::CrashClass;
use sailing_proto::{
  Entry, ForkId, GroupId, HardState, Index, LogStore, MultiEngine, NodeId, SnapshotMeta,
  StableStore, Term,
};
use std::vec::Vec;

/// A [`LogStore`] the kit can drive to a durability edge.
///
/// The log handed back MUST be FRESH — empty, never written, no queued completions. The suite
/// checks that first and stops if it is not, because every later check reads against a known
/// starting view.
pub trait LogSubject {
  /// The log under test.
  type Log: LogStore;

  /// The log. Called between every step; the same store each time.
  fn log(&mut self) -> &mut Self::Log;

  /// Make everything submitted so far durable and release its completions — the implementation's
  /// barrier (a batch flush, an fsync, a no-op for a synchronous store).
  fn barrier(&mut self);
}

/// A [`StableStore`] the kit can drive to a durability edge. Fresh, on the same terms as
/// [`LogSubject`].
pub trait StableSubject {
  /// The store under test.
  type Stable: StableStore;

  /// The store.
  fn stable(&mut self) -> &mut Self::Stable;

  /// The implementation's barrier — see [`LogSubject::barrier`].
  fn barrier(&mut self);

  /// A node id the suite may record as a vote or place in a configuration. Distinct `n` must give
  /// distinct ids.
  fn node_id(&self, n: u64) -> <Self::Stable as StableStore>::NodeId;
}

/// Whether an engine claims to survive a crash.
///
/// The distinction is not cosmetic: the crash half of the engine suite checks OPPOSITE laws for
/// the two tiers. A durable engine must give back exactly its barrier-covered state; a volatile
/// one must give back NOTHING, and a volatile engine that appeared to recover state would be
/// reporting durability it does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
  /// State dies with the engine. The in-memory reference engine is this tier.
  Volatile,
  /// State that reached a barrier survives a crash.
  Durable,
}

/// A [`MultiEngine`] the kit can open, drive, crash, and reopen.
pub trait EngineSubject {
  /// The engine's group id type.
  type Group: GroupId;
  /// The engine's node id type.
  type NodeId: NodeId;
  /// The engine under test.
  type Engine: MultiEngine<Self::Group, Self::NodeId>;

  /// What this engine claims about crashes.
  fn durability(&self) -> Durability;

  /// Open an engine over a BRAND-NEW medium: no groups, no lineage records, no boot epochs.
  fn open(&mut self) -> Self::Engine;

  /// Take `engine` away as `class` would, and reopen over whatever survived.
  ///
  /// The engine is passed by value because a crash is not a shutdown: an implementation must NOT
  /// get a chance to flush on the way out, and taking ownership is what makes "no teardown"
  /// enforceable rather than merely requested.
  fn crash(&mut self, engine: Self::Engine, class: CrashClass) -> Self::Engine;

  /// The `n`-th group id. Distinct `n` must give distinct ids.
  fn group(&self, n: u64) -> Self::Group;

  /// The `n`-th node id. Distinct `n` must give distinct ids.
  fn node(&self, n: u64) -> Self::NodeId;

  /// A log entry whose lineage move names `generation` for its own group — the SHAPE-ENTRY leg of
  /// the removal ceiling.
  ///
  /// `None` (the default) when the subject cannot mint one, and the suite then SKIPS that leg
  /// rather than passing it. Minting a shape entry needs the payload codec that decodes it, so
  /// only an implementation that owns that codec can answer.
  /// # Lineage generations this suite writes
  ///
  /// Every generation the kit hands `MultiEngine::set_group_gen` is a WORKING one — strictly below
  /// [`sailing_proto::HIGHEST_WORKING_GENERATION`] — because that is the caller's half of the
  /// method's contract and the kit is a caller. An implementation may ASSUME it: nothing here
  /// requires an engine to clamp, validate, or otherwise defend against a record in the reserved
  /// band, and a subject that does no checking at all conforms. What the suite does require is the
  /// consequence — the fence a legal record produces is a fence, never the reserved
  /// [`sailing_proto::MERGED_FLOOR`] terminal, which is read cluster-wide as a verdict no local
  /// removal is entitled to write.
  fn shape_entry(&self, _term: Term, _index: Index, _generation: u64) -> Option<Entry> {
    None
  }

  /// The surviving byte length of the device a [`CrashClass::TornTail`] would cut, so the suite
  /// can sweep torn offsets across real record boundaries instead of guessing.
  ///
  /// `None` (the default) when the engine has no byte medium; the suite then tears at a small
  /// fixed set of offsets, which is still meaningful for an engine whose medium it cannot see.
  fn tail_len(&self) -> Option<u64> {
    None
  }
}

/// A byte codec for the durable shapes — the subject of the serialization suite.
///
/// A store that keeps these types BY VALUE has nothing to check here. A store that persists them
/// does, and the fields at issue are the ones whose loss produces no error anywhere: a group that
/// silently never installs, a restart that is silently less safe than the run before it.
pub trait Codec {
  /// The node id the encoded shapes carry.
  type NodeId: NodeId;

  /// Encode a hard state.
  fn encode_hard_state(&self, hs: &HardState<Self::NodeId>) -> Vec<u8>;

  /// Decode a hard state; `None` on malformed input.
  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<Self::NodeId>>;

  /// Encode a snapshot meta.
  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<Self::NodeId>) -> Vec<u8>;

  /// Decode a snapshot meta; `None` on malformed input.
  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<Self::NodeId>>;

  /// The bytes a PRE-`lease_support` writer would have produced for `hs` — the input the legacy
  /// decode rule is about.
  ///
  /// `None` (the default) when the implementation cannot produce the old shape; the suite then
  /// SKIPS the legacy check. Skipping it leaves the sharpest rule in this suite unproven, so an
  /// implementation with any pre-format history should answer.
  fn encode_legacy_hard_state(&self, _hs: &HardState<Self::NodeId>) -> Option<Vec<u8>> {
    None
  }

  /// The `n`-th node id. Distinct `n` must give distinct ids.
  fn node_id(&self, n: u64) -> Self::NodeId;

  /// A lineage token the suite round-trips. The default fabricates one from the codec's own node
  /// ids' shape; an implementation with id-encoding constraints overrides it.
  fn fork_id(&self) -> ForkId {
    ForkId::new(
      bytes::Bytes::from_static(b"conformance-parent"),
      3,
      Index::new(17),
      Term::new(4),
      bytes::Bytes::from_static(b"conformance-child"),
      5,
    )
  }
}
