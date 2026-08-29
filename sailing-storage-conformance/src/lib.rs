//! Conformance suites for sailing's storage seams.
//!
//! An implementation of [`LogStore`], [`StableStore`], or [`MultiEngine`] is correct only if it
//! honours contracts that no type signature can express: which reads are submit-visible and which
//! are last-durable, when a completion may be released, what survives a crash, and what a
//! durability PROBE may claim. This crate turns those contracts into runnable checks.
//!
//! [`LogStore`]: sailing_proto::LogStore
//! [`StableStore`]: sailing_proto::StableStore
//! [`MultiEngine`]: sailing_proto::MultiEngine
//!
//! # The two halves
//!
//! [`check`] holds the suites. Each takes a SUBJECT — a small adapter an implementation writes over
//! its own type — and returns a report. The subject exists because the contracts are about
//! DURABILITY EDGES, and only the implementation knows how to reach one: the kit can submit work,
//! but only the subject can say "make it durable now" or "crash and reopen".
//!
//! [`fault`] holds the reference types the suites need and an implementation may reuse: a crashable
//! VFS with the three crash classes ([`CrashClass`]), completion-delivery faults for the async
//! completion contract, a reference durability probe showing what `durable_index` must answer, a
//! byte codec that round-trips every field the core depends on, and a durable engine over the crash
//! seam that the kit uses as its own subject.
//!
//! # Reading a report
//!
//! A suite never panics on a violation; it records one, so a single run reports EVERY breach rather
//! than stopping at the first. A check the subject cannot reach — an optional seam it does not
//! offer — is SKIPPED rather than passed, so a report that proves little says so.
//!
//! ```
//! use sailing_storage_conformance::{check, fault::JournalEngineSubject};
//!
//! let report = check::engine(&mut JournalEngineSubject::new());
//! report.assert_conformant();
//! assert!(report.passed_check("engine/exactly-flush-covered-state-survives"));
//! ```

pub mod check;
pub mod fault;

pub use check::{Codec, EngineSubject, LogSubject, Report, StableSubject, Violation};
pub use fault::{CompletionFaults, CrashClass, JournalEngine, ProbingLog, ReferenceCodec};
