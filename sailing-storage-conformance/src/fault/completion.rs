//! Completion-delivery faults: what goes wrong between a store making a write durable and the core
//! learning that it did.
//!
//! The store contract says a completion arrives eventually, exactly once, in FIFO order, and that a
//! LOST one costs liveness rather than safety. The durability PROBES exist to narrow that wedge:
//! once the queue is quiescent the core folds the store's own evidence instead of waiting for a
//! completion that will never come. Both halves need a channel that misbehaves on purpose.
//!
//! A faulty wrapper deliberately BREAKS the delivery contract — that is its job. It is not a
//! conforming store and must never be handed to a store suite as one; it is what a conforming
//! store is driven THROUGH.

use sailing_proto::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, OpId, SnapshotChunkRead, SnapshotMeta,
  StableDone, StableStore, Term,
};
use std::collections::VecDeque;

/// How a completion channel misbehaves.
///
/// The classes compose: a channel can reorder AND duplicate AND lose. Each is a real failure of a
/// real store — a completion queue drained out of order, a retried write acknowledged twice, an
/// acknowledgment dropped on the floor, an acknowledgment from the PREVIOUS incarnation of the node
/// arriving after a restart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompletionFaults {
  /// Hand completions back in reverse arrival order. The trait permits any order; a core that
  /// silently assumed submit order would break here.
  pub reorder: bool,
  /// Hand every completion back twice. A gate released twice must be the same release.
  pub duplicate: bool,
  /// Drop one completion in every `lose_every` (0 = never). THE class the durability probes exist
  /// for: the gated path must heal from the store's own evidence rather than wedging.
  pub lose_every: u32,
  /// Hold each completion for this many `poll` calls before handing it back.
  pub delay_polls: u32,
  /// Deliver, once, a completion minted by a PRIOR boot epoch — the acknowledgment a store that
  /// did not clear its queue on crash hands the next incarnation. Its lower epoch must make it
  /// unequal to, and strictly below, every current id.
  pub stale_delivery: bool,
}

impl CompletionFaults {
  /// A perfect channel.
  #[must_use]
  pub const fn none() -> Self {
    Self {
      reorder: false,
      duplicate: false,
      lose_every: 0,
      delay_polls: 0,
      stale_delivery: false,
    }
  }

  /// Reverse the arrival order.
  #[must_use]
  pub const fn reordering() -> Self {
    Self {
      reorder: true,
      ..Self::none()
    }
  }

  /// Hand every completion back twice.
  #[must_use]
  pub const fn duplicating() -> Self {
    Self {
      duplicate: true,
      ..Self::none()
    }
  }

  /// Drop one completion in every `n`.
  #[must_use]
  pub const fn losing_every(n: u32) -> Self {
    Self {
      lose_every: n,
      ..Self::none()
    }
  }

  /// Hold each completion for `n` polls.
  #[must_use]
  pub const fn delaying(n: u32) -> Self {
    Self {
      delay_polls: n,
      ..Self::none()
    }
  }

  /// Deliver one completion from a prior boot epoch.
  #[must_use]
  pub const fn stale_deliveries() -> Self {
    Self {
      stale_delivery: true,
      ..Self::none()
    }
  }

  /// Every class at once — the channel a store must still be judged sound against.
  #[must_use]
  pub const fn all() -> Self {
    Self {
      reorder: true,
      duplicate: true,
      lose_every: 3,
      delay_polls: 1,
      stale_delivery: true,
    }
  }

  /// The name each class carries in a report.
  #[must_use]
  pub const fn label(self) -> &'static str {
    match self {
      Self {
        reorder: true,
        duplicate: false,
        lose_every: 0,
        delay_polls: 0,
        stale_delivery: false,
      } => "reorder",
      Self {
        duplicate: true,
        reorder: false,
        lose_every: 0,
        delay_polls: 0,
        stale_delivery: false,
      } => "duplicate",
      Self {
        lose_every: 1..,
        reorder: false,
        duplicate: false,
        delay_polls: 0,
        stale_delivery: false,
      } => "loss",
      Self {
        delay_polls: 1..,
        reorder: false,
        duplicate: false,
        lose_every: 0,
        stale_delivery: false,
      } => "delay",
      Self {
        stale_delivery: true,
        reorder: false,
        duplicate: false,
        lose_every: 0,
        delay_polls: 0,
      } => "stale-delivery",
      _ => "combined",
    }
  }
}

/// The fault filter itself, over any completion type.
#[derive(Debug)]
struct FaultyQueue<D> {
  faults: CompletionFaults,
  /// Faulted completions with the polls each still owes.
  queued: VecDeque<(u32, D)>,
  arrived: u64,
  dropped: u64,
  stale_sent: bool,
}

impl<D: Clone> FaultyQueue<D> {
  const fn new(faults: CompletionFaults) -> Self {
    Self {
      faults,
      queued: VecDeque::new(),
      arrived: 0,
      dropped: 0,
      stale_sent: false,
    }
  }

  /// Admit one completion from the underlying store, applying the intake-time classes.
  fn admit(&mut self, done: D) {
    self.arrived += 1;
    if self.faults.lose_every != 0
      && self
        .arrived
        .is_multiple_of(u64::from(self.faults.lose_every))
    {
      self.dropped += 1;
      return;
    }
    let copies = if self.faults.duplicate { 2 } else { 1 };
    for _ in 0..copies {
      let item = (self.faults.delay_polls, done.clone());
      if self.faults.reorder {
        self.queued.push_front(item);
      } else {
        self.queued.push_back(item);
      }
    }
  }

  /// Hand back the first completion whose delay has run out, charging one poll to the rest.
  fn take(&mut self) -> Option<D> {
    let position = self.queued.iter().position(|(owed, _)| *owed == 0);
    for (owed, _) in &mut self.queued {
      *owed = owed.saturating_sub(1);
    }
    position
      .and_then(|at| self.queued.remove(at))
      .map(|(_, d)| d)
  }

  fn is_empty(&self) -> bool {
    self.queued.is_empty()
  }

  /// Whether the prior-incarnation completion is still owed.
  fn owes_stale(&mut self) -> bool {
    if self.faults.stale_delivery && !self.stale_sent {
      self.stale_sent = true;
      return true;
    }
    false
  }
}

/// An id from a PRIOR boot epoch: epoch-major ordering puts it strictly below, and unequal to,
/// every id of a live incarnation seeded at epoch 1 or above.
#[must_use]
pub fn prior_incarnation_op_id() -> OpId {
  OpId::first_of_epoch(0).next()
}

/// A [`LogStore`] whose completion channel misbehaves. Reads, writes, and the durability probe pass
/// straight through — only delivery is faulted, which is exactly where the hazard lives.
#[derive(Debug)]
pub struct FaultyLog<'a, L> {
  inner: &'a mut L,
  queue: FaultyQueue<LogDone>,
}

impl<'a, L: LogStore> FaultyLog<'a, L> {
  /// Wrap `inner`'s completion channel with `faults`.
  pub fn new(inner: &'a mut L, faults: CompletionFaults) -> Self {
    Self {
      inner,
      queue: FaultyQueue::new(faults),
    }
  }

  /// How many completions the channel swallowed.
  #[must_use]
  pub const fn dropped(&self) -> u64 {
    self.queue.dropped
  }

  /// Whether neither the channel nor the store behind it holds anything more to deliver — the
  /// drain's stopping condition, which a DELAYED completion must not be mistaken for.
  #[must_use]
  pub fn is_quiescent(&self) -> bool {
    self.queue.is_empty() && !self.inner.has_pending()
  }

  /// Move everything the store has ready into the faulted channel.
  pub fn pump(&mut self) -> Result<(), L::Error> {
    if self.queue.owes_stale() {
      self
        .queue
        .queued
        .push_front((0, LogDone::Appended(prior_incarnation_op_id())));
    }
    while let Some(done) = self.inner.poll() {
      self.queue.admit(done?);
    }
    Ok(())
  }
}

impl<L: LogStore> LogStore for FaultyLog<'_, L> {
  type Error = L::Error;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }

  fn last_index(&self) -> Index {
    self.inner.last_index()
  }

  fn durable_index(&self) -> Option<Index> {
    self.inner.durable_index()
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    self.inner.term(index)
  }

  fn entries(
    &self,
    range: core::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    self.inner.entries(range, max_bytes)
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    self.inner.submit_append(id, entries);
  }

  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
    self.queue.queued.clear();
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    if let Err(e) = self.pump() {
      return Some(Err(e));
    }
    self.queue.take().map(Ok)
  }

  /// Deliberately NOT exact: under `lose_every` the underlying store still holds a completion the
  /// channel is about to swallow, so this over-reports. A store must never do that; a faulty
  /// channel is precisely the condition that makes the core's own `has_pending` handling matter.
  fn has_pending(&self) -> bool {
    !self.queue.is_empty() || self.inner.has_pending()
  }
}

/// A [`StableStore`] whose completion channel misbehaves — the [`FaultyLog`] of the stable side.
#[derive(Debug)]
pub struct FaultyStable<'a, S> {
  inner: &'a mut S,
  queue: FaultyQueue<StableDone>,
}

impl<'a, S: StableStore> FaultyStable<'a, S> {
  /// Wrap `inner`'s completion channel with `faults`.
  pub fn new(inner: &'a mut S, faults: CompletionFaults) -> Self {
    Self {
      inner,
      queue: FaultyQueue::new(faults),
    }
  }

  /// How many completions the channel swallowed.
  #[must_use]
  pub const fn dropped(&self) -> u64 {
    self.queue.dropped
  }

  /// Whether neither the channel nor the store behind it holds anything more to deliver.
  #[must_use]
  pub fn is_quiescent(&self) -> bool {
    self.queue.is_empty() && !self.inner.has_pending()
  }

  /// Move everything the store has ready into the faulted channel.
  pub fn pump(&mut self) -> Result<(), S::Error> {
    if self.queue.owes_stale() {
      self
        .queue
        .queued
        .push_front((0, StableDone::Wrote(prior_incarnation_op_id())));
    }
    while let Some(done) = self.inner.poll() {
      self.queue.admit(done?);
    }
    Ok(())
  }
}

impl<S: StableStore> StableStore for FaultyStable<'_, S> {
  type NodeId = S::NodeId;
  type Error = S::Error;

  fn hard_state(&self) -> HardState<Self::NodeId> {
    self.inner.hard_state()
  }

  fn durable_hard_state(&self) -> Option<HardState<Self::NodeId>> {
    self.inner.durable_hard_state()
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<Self::NodeId>) {
    self.inner.submit_write(id, hard_state);
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<Self::NodeId>, data: bytes::Bytes) {
    self.inner.submit_snapshot(id, meta, data);
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<Self::NodeId>, bytes::Bytes)> {
    self.inner.snapshot()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<Self::NodeId>> {
    self.inner.durable_snapshot()
  }

  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<Self::NodeId>, u64, SnapshotChunkRead), Self::Error>> {
    self.inner.snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<Self::NodeId>,
    total_len: u64,
    offset: u64,
    data: &bytes::Bytes,
  ) -> Result<u64, Self::Error> {
    self
      .inner
      .accept_snapshot_chunk(meta, total_len, offset, data)
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<Self::NodeId>) -> Option<bytes::Bytes> {
    self.inner.take_staged_snapshot(meta)
  }

  fn discard_snapshot_staging(&mut self) {
    self.inner.discard_snapshot_staging();
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    if let Err(e) = self.pump() {
      return Some(Err(e));
    }
    self.queue.take().map(Ok)
  }

  /// Deliberately not exact — see [`FaultyLog::has_pending`].
  fn has_pending(&self) -> bool {
    !self.queue.is_empty() || self.inner.has_pending()
  }
}
