//! The committed-split state the endpoint stages between the deterministic apply point and the
//! driver's materialization crank: the pending forks (with their apply-derived recovery blobs),
//! the fork durability barrier over the parent's snapshots, and the group's lineage counter.
use super::*;

/// One committed split, staged at apply for the multi container to relay. Deliberately F-FREE —
/// the forked state-machine half rides beside the queue (see [`SplitState::forked_fsms`]) — so a
/// container can inspect a fork without touching the half, and G-FREE (the child id stays its
/// canonical `Data` encoding until the container decodes it).
// The lib-profile expectation self-expires when the container relay consumes the fields (an
// unfulfilled `expect` is a lint error), so it cannot outlive its reason.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) struct PendingFork<I> {
  /// The child group id's canonical `Data` encoding (1..=1024 bytes — validated at payload
  /// decode).
  pub child_bytes: Bytes,
  /// The child id's incarnation, from the committed entry.
  pub child_gen: u64,
  /// The parent's lineage counter after this split — the container's replay-guard anchor.
  pub parent_gen_after: u64,
  /// The child's recovery blob, derived AT APPLY from the just-forked half (`encode(child
  /// .snapshot())`), so blob and half correspond by construction. Persisted as the child's
  /// authoritative baseline by the manufactured snapshot install; never carried by the entry.
  pub blob: Bytes,
  /// The parent's VOTER set at the split entry (both-halves-identical by the total log order) —
  /// the child's colocated-by-construction bootstrap membership.
  pub voters: Vec<I>,
  /// The parent's ACTIVE read mode at the split entry when a committed migration set it
  /// (`None` for a never-migrated parent): the child inherits it through its baseline snapshot
  /// meta, exactly as a restart recovers a migrated mode from replicated state.
  pub read_only: Option<ReadOnlyOption>,
  /// The split entry's log index — the fork durability barrier's anchor.
  pub index: Index,
}

/// The endpoint-resident split state. One instance per endpoint, defaulted empty; restart seeds
/// the lineage from the recovered snapshot meta.
pub(crate) struct SplitState<I, F> {
  /// Committed-but-not-yet-relayed forks, in apply (log) order.
  pub(crate) pending_forks: VecDeque<PendingFork<I>>,
  /// The forked halves, index-aligned with [`pending_forks`](Self::pending_forks).
  pub(crate) forked_fsms: VecDeque<F>,
  /// THE FORK DURABILITY BARRIER: while `Some(i)`, `maybe_snapshot` refuses to capture at
  /// `applied >= i`. Set to the OLDEST unlifted split index — a parent snapshot at-or-past a
  /// split would compact the entry whose replay is the staged child's only recovery source
  /// before the child's baseline is flush-durable, so a correlated crash could lose the child
  /// outright. Lifted by the driver once the fork's materialization is behind its engine
  /// barrier.
  pub(crate) snapshot_cap: Option<Index>,
  /// The group's LINEAGE counter — one unified monotone per-id value for incarnation and shape
  /// (seeded from the admission generation / recovered snapshot meta; bumped to
  /// `parent_gen_after` when a split applies). Stamped into every snapshot meta this endpoint
  /// captures.
  pub(crate) shape_gen: u64,
  /// The lineage recovered from DURABLE state at boot (admission generation ⊔ the restored
  /// snapshot meta), BEFORE any restart replay re-bumped [`shape_gen`](Self::shape_gen): the
  /// container seeds its replay guard from this, so a fork re-staged by restart replay is
  /// relayed again rather than dropped as a duplicate.
  pub(crate) restored_lineage: u64,
}

impl<I, F> SplitState<I, F> {
  pub(crate) fn new(lineage: u64) -> Self {
    Self {
      pending_forks: VecDeque::new(),
      forked_fsms: VecDeque::new(),
      snapshot_cap: None,
      shape_gen: lineage,
      restored_lineage: lineage,
    }
  }
}

impl<I: fmt::Debug, F> fmt::Debug for SplitState<I, F> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SplitState")
      .field("pending_forks", &self.pending_forks.len())
      .field("snapshot_cap", &self.snapshot_cap)
      .field("shape_gen", &self.shape_gen)
      .field("restored_lineage", &self.restored_lineage)
      .finish_non_exhaustive()
  }
}

impl<I, F, R> Endpoint<I, F, R>
where
  F: StateMachine,
{
  /// Pop the oldest staged fork with its forked state-machine half (apply order — the FIFO the
  /// container's replay guard relies on). The two queues are pushed together at the apply arm,
  /// so they pop together.
  // The expectation self-expires the moment the container relay lands (an unfulfilled `expect`
  // is a lint error), so it cannot outlive its reason.
  #[cfg_attr(not(test), expect(dead_code))]
  pub(crate) fn pop_pending_fork(&mut self) -> Option<(PendingFork<I>, F)> {
    let fork = self.split.pending_forks.pop_front()?;
    let fsm = self
      .split
      .forked_fsms
      .pop_front()
      .expect("the fork and half queues are pushed together");
    Some((fork, fsm))
  }

  /// Lift the fork durability barrier for a fork at-or-below `past` (its baseline is now behind
  /// the local engine barrier). The cap moves to the OLDEST still-queued fork — never simply
  /// cleared, so a later staged-but-unflushed fork keeps the parent's snapshots fenced.
  // Self-expiring, as on `pop_pending_fork` (consumed by the container relay).
  #[cfg_attr(not(test), expect(dead_code))]
  pub(crate) fn lift_snapshot_cap(&mut self, past: Index) {
    if self.split.snapshot_cap.is_some_and(|cap| cap <= past) {
      self.split.snapshot_cap = self.split.pending_forks.front().map(|f| f.index);
    }
  }

  /// The group's lineage counter (incarnation ⊔ shape), LIVE: includes every applied split.
  // Self-expiring, as on `pop_pending_fork` (consumed by the container relay).
  #[cfg_attr(not(test), expect(dead_code))]
  pub(crate) fn shape_gen(&self) -> u64 {
    self.split.shape_gen
  }

  /// The lineage recovered from durable state at boot, BEFORE any restart replay re-bumped the
  /// live counter — the container's replay-guard seed.
  // Self-expiring, as on `pop_pending_fork` (consumed by the container relay).
  #[cfg_attr(not(test), expect(dead_code))]
  pub(crate) fn restored_lineage(&self) -> u64 {
    self.split.restored_lineage
  }

  /// Raise the lineage counter to at least `generation` — the admission seam: the container
  /// folds the embedder catalog's incarnation in at create/restore/fork admission (monotone, so
  /// a stale catalog value can never lower a replayed split's bump).
  // Self-expiring, as on `pop_pending_fork` (consumed by the container admission paths) — and
  // profile-unconditional: no test exercises it before then either.
  #[expect(dead_code)]
  pub(crate) fn raise_lineage(&mut self, generation: u64) {
    self.split.shape_gen = self.split.shape_gen.max(generation);
    self.split.restored_lineage = self.split.restored_lineage.max(generation);
  }
}
