//! The suites.
//!
//! Each entry point takes a subject, runs every check it can reach, and returns a [`Report`]. A
//! suite never panics on a violation — it records one, so a single run reports EVERY breach rather
//! than the first.

mod report;
pub use report::{Report, Skip, Violation};

mod subject;
pub use subject::{Codec, Durability, EngineSubject, LogSubject, StableSubject};

mod completion;
pub use completion::{
  CompletionInjector, FaithfulInjector, completion_faults_log, completion_faults_log_with,
  completion_faults_stable, completion_faults_stable_with,
};

mod engine;
pub use engine::engine;

mod log;
pub use log::log_store;

mod restore;
pub use restore::restore_admission;

mod serialization;
pub use serialization::serialization;

mod stable;
pub use stable::stable_store;
