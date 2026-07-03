//! Multi-group reactor drivers: ONE `Send` task hosting N co-located Raft groups over a shared
//! transport (a [`MultiStreamCoordinator`](sailing_proto::MultiStreamCoordinator) or
//! [`MultiQuicCoordinator`](sailing_proto::MultiQuicCoordinator)) and a shared storage engine
//! ([`GroupEngine`](sailing_proto::GroupEngine)) with one batched durability barrier per crank.
//!
//! The single-group run loops generalize, they do not change shape: the socket/timer/command
//! plumbing is the single drivers' verbatim, the per-operation state
//! ([`Routing`](sailing_driver::shared::Routing)) is the single-group type instantiated PER GROUP,
//! and the one armed deadline folds every group's earliest consensus deadline in. Groups arrive
//! and leave at runtime through [`MultiCommand`](sailing_driver::MultiCommand) lifecycle commands
//! — the drivers bind EMPTY.

mod quic;
mod stream;

pub use quic::MultiReactorQuicDriver;
pub use stream::MultiReactorStreamDriver;

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use crate::DriverError;

/// How many times one group's `handle_storage` is re-driven within a single crank while it
/// reports `MorePending`; the remainder rides the next crank (the crank keeps the storage-redrive
/// deadline immediate), so no single group's completion backlog can monopolize a pass.
pub(crate) const STORAGE_REDRIVES: usize = 4;

/// The uniform verdict for an operation addressed to a group this host does not carry.
pub(crate) fn no_such_group<I>() -> DriverError<I> {
  DriverError::Rejected {
    reason: "no such group on this host".to_string(),
  }
}

/// Flatten a typed lifecycle/config refusal onto the driver's `Rejected` surface — the same
/// flatten the proto's propose/transfer/read errors get at the single-group boundary.
pub(crate) fn rejected<I>(e: impl core::fmt::Display) -> DriverError<I> {
  DriverError::Rejected {
    reason: e.to_string(),
  }
}

/// Cross-thread observability for a multi driver's shared engine: the driver republishes the
/// engine's [`flushes`](sailing_proto::GroupEngine::flushes) /
/// [`ops_flushed`](sailing_proto::GroupEngine::ops_flushed) counters after every storage crank, so
/// a test or operator thread can watch the fsync-amortization ratio (operations per barrier)
/// while the driver task exclusively owns the engine. Obtain it from the driver BEFORE spawning
/// `run()` (e.g. `driver.engine_metrics()`); clones share the counters.
#[derive(Clone, Default)]
pub struct EngineMetrics {
  inner: Arc<EngineMetricsInner>,
}

#[derive(Default)]
struct EngineMetricsInner {
  flushes: AtomicU64,
  ops_flushed: AtomicU64,
}

impl EngineMetrics {
  /// Durability barriers run so far (every [`GroupEngine::flush`](sailing_proto::GroupEngine::flush)
  /// call counts, including one that released nothing).
  #[must_use]
  pub fn flushes(&self) -> u64 {
    self.inner.flushes.load(Ordering::Acquire)
  }

  /// Total storage operations completed across every barrier so far. `ops_flushed / flushes` is
  /// the cross-group batch factor the shared engine exists for.
  #[must_use]
  pub fn ops_flushed(&self) -> u64 {
    self.inner.ops_flushed.load(Ordering::Acquire)
  }

  /// Publish the engine's current counters (driver-side, once per crank).
  pub(crate) fn record(&self, flushes: u64, ops_flushed: u64) {
    self.inner.flushes.store(flushes, Ordering::Release);
    self.inner.ops_flushed.store(ops_flushed, Ordering::Release);
  }
}
