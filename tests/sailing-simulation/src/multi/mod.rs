//! The multi-group simulation tier: a deterministic world of [`MultiRaft`] container hosts.
//!
//! Where [`crate::Cluster`] drives one `Endpoint` per node, [`MultiWorld`] drives one
//! `MultiRaft<u64, u64, LogSm>` per node over a group-tagged typed bus: every in-flight message
//! carries its group id, per-`(node, group)` stores back each replica, and the virtual clock is
//! shared by every group a node hosts (clocks are per NODE, matching production). Group ids come
//! from the caller under the single-incarnation contract — an id is never reused for a different
//! logical group.
//!
//! # The M1 soak gate
//!
//! The exit-criterion soak is the ignored `soak_default_profile` band in `tests/multi_vopr.rs`
//! (64 seeds × 40_000 ticks under the default profile). ALWAYS run it under `--release` — the
//! same band in debug wastes hours:
//!
//! ```text
//! cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_default_profile
//! ```
//!
//! Shard it with `MULTI_VOPR_SEED_{START,END}` and `MULTI_VOPR_TICKS`. A failure prints
//! `seed=<S> tick=<T>` and replays deterministically anywhere via
//! `run_multi_vopr(S, ticks, MultiProfile::default_multi())`.
//!
//! [`MultiRaft`]: sailing_proto::MultiRaft

mod world;
pub use world::MultiWorld;

mod oracles;
// The gkv keyed-payload codec is the SIM-WIDE keyed workload contract: the shared `LogSm`
// partitions by it (`StateMachine::split`), so the codec is crate-visible rather than
// multi-private.
pub(crate) use oracles::{decode_gkv, encode_gkv};

mod vopr;
pub use vopr::{MultiAction, MultiProfile, MultiVoprReport, run_multi_vopr};

mod interaction;
pub use interaction::{MultiInteractionEnv, run_multi_interaction_file};

// The P6 split/merge instruction-conservation seam: a pure ledger type M4/M5 wire to the real
// split and merge actions; until then only its own teeth tests exercise it.
#[cfg_attr(
  not(test),
  expect(dead_code, reason = "M4/M5 wire the split/merge actions to this seam")
)]
mod conservation;
