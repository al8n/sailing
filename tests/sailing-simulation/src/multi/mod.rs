//! The multi-group simulation tier: a deterministic world of [`MultiRaft`] container hosts.
//!
//! Where [`crate::Cluster`] drives one `Endpoint` per node, [`MultiWorld`] drives one
//! `MultiRaft<u64, u64, LogSm>` per node over a group-tagged typed bus: every in-flight message
//! carries its group id, per-`(node, group)` stores back each replica, and the virtual clock is
//! shared by every group a node hosts (clocks are per NODE, matching production). Group ids come
//! from the caller under the single-incarnation contract — an id is never reused for a different
//! logical group.
//!
//! [`MultiRaft`]: sailing_proto::MultiRaft

mod world;
pub use world::MultiWorld;

mod oracles;

mod vopr;
pub use vopr::{MultiAction, MultiProfile, MultiVoprReport, run_multi_vopr};
