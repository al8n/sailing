//! Sans-I/O Raft consensus core. See `docs/superpowers/specs/2026-06-02-sailing-design.md`.
//!
//! `alloc` is the mandatory floor (Case A); `std` layers OS facilities on top. There is
//! no no-alloc tier in v1, which is why `std = ["alloc", …]` rather than independent.
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc as std;
#[cfg(feature = "std")]
extern crate std;

#[cfg(not(feature = "alloc"))]
compile_error!("sailing-proto requires the `alloc` feature (it is enabled transitively by `std`)");

mod num;
pub use num::{Index, Term};

mod id;
pub use id::NodeId;

mod time;
pub use time::Instant;

mod data;
pub use data::{Data, DataRef, DecodeError};

mod entry;
pub use entry::{Entry, EntryKind};

mod hard_state;
pub use hard_state::HardState;

mod message;
pub use message::{
  AppendEntries, AppendResp, Heartbeat, HeartbeatResp, Message, Outgoing, RequestVote, VoteResp,
};

mod state_machine;
pub use state_machine::StateMachine;

mod storage;
pub use storage::{LogDone, LogStore, OpId, StableDone, StableStore};

mod error;
pub use error::ConfigError;

mod config;
pub use config::Config;
