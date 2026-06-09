//! Deterministic simulation harness for `sailing-proto`.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod store;
pub use store::{MemLog, MemStable, MemStoreError, StorageFaults, StoreMode};

mod network;
pub use network::NetworkFaults;

mod sm;
pub use sm::LogSm;

mod cluster;
pub use cluster::{AppliedLog, Cluster};
