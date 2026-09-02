//! Subjects over the in-tree reference engine.
//!
//! [`GroupEngine`] is the Sans-I/O reference implementation of the storage contract, so it is also
//! the kit's first conformance subject: the suites must pass against it, and a suite that would not
//! is a bug in the suite. Its stores stage their completions behind the engine barrier, which is
//! exactly the visible/durable split the store suites are about.

use crate::{
  check::{Durability, EngineSubject, LogSubject, StableSubject},
  fault::CrashClass,
};
use sailing_proto::{EngineLog, EngineStable, Entry, GroupEngine, Index, Term};

/// The group every store-level subject lends its handles from. Which id it is does not matter — a
/// store suite never looks at group identity — but it has to be SOME id, since a store handle only
/// exists inside a hosted group.
const SUBJECT_GROUP: u64 = 1;

/// A [`LogSubject`] over the reference engine's per-group log handle, whose barrier is the engine
/// flush.
#[derive(Debug)]
pub struct ReferenceLogSubject {
  engine: GroupEngine<u64, u64>,
}

impl Default for ReferenceLogSubject {
  fn default() -> Self {
    Self::new()
  }
}

impl ReferenceLogSubject {
  /// A subject over a fresh log.
  #[must_use]
  pub fn new() -> Self {
    let mut engine = GroupEngine::new();
    engine.add_group(SUBJECT_GROUP);
    Self { engine }
  }
}

impl LogSubject for ReferenceLogSubject {
  type Log = EngineLog;

  fn log(&mut self) -> &mut Self::Log {
    self
      .engine
      .stores(&SUBJECT_GROUP)
      .expect("the subject's group is admitted at construction")
      .0
  }

  fn barrier(&mut self) {
    self.engine.flush();
  }
}

/// A [`StableSubject`] over the reference engine's per-group stable handle, whose barrier is the
/// engine flush.
#[derive(Debug)]
pub struct ReferenceStableSubject {
  engine: GroupEngine<u64, u64>,
}

impl Default for ReferenceStableSubject {
  fn default() -> Self {
    Self::new()
  }
}

impl ReferenceStableSubject {
  /// A subject over a fresh stable store.
  #[must_use]
  pub fn new() -> Self {
    let mut engine = GroupEngine::new();
    engine.add_group(SUBJECT_GROUP);
    Self { engine }
  }
}

impl StableSubject for ReferenceStableSubject {
  type Stable = EngineStable<u64>;

  fn stable(&mut self) -> &mut Self::Stable {
    self
      .engine
      .stores(&SUBJECT_GROUP)
      .expect("the subject's group is admitted at construction")
      .1
  }

  fn barrier(&mut self) {
    self.engine.flush();
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

/// An [`EngineSubject`] over the in-memory reference engine.
///
/// Its tier is [`Durability::Volatile`], which is not a weakness of the subject but the engine's
/// documented contract: its floors survive exactly what it survives, which is nothing. The engine
/// suite checks the volatile law directly — after a crash of ANY class the reopened engine hosts
/// nothing and remembers no lineage — so an in-memory engine that appeared to recover state would
/// fail here rather than pass vacuously.
#[derive(Debug, Default)]
pub struct ReferenceEngineSubject;

impl ReferenceEngineSubject {
  /// The subject over [`GroupEngine`].
  #[must_use]
  pub fn in_memory() -> Self {
    Self
  }
}

impl EngineSubject for ReferenceEngineSubject {
  type Group = u64;
  type NodeId = u64;
  type Engine = GroupEngine<u64, u64>;

  fn durability(&self) -> Durability {
    Durability::Volatile
  }

  fn open(&mut self) -> Self::Engine {
    GroupEngine::new()
  }

  fn crash(&mut self, engine: Self::Engine, _class: CrashClass) -> Self::Engine {
    // The whole engine IS the medium: dropping it is the crash, and what comes back is what a new
    // process would find, which is nothing.
    drop(engine);
    GroupEngine::new()
  }

  fn group(&self, n: u64) -> Self::Group {
    n
  }

  fn node(&self, n: u64) -> Self::NodeId {
    n
  }

  fn shape_entry(&self, term: Term, index: Index, generation: u64) -> Option<Entry> {
    Some(crate::fault::mint_shape_entry(term, index, generation))
  }
}
