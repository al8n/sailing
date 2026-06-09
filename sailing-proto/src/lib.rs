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

mod prng;
pub(crate) use prng::Prng;

mod num;
pub use num::{Index, Term};

mod id;
pub use id::NodeId;

mod time;
pub use time::Instant;

mod data;
pub use data::{Data, DecodeError};

mod entry;
pub use entry::{Entry, EntryKind};

mod hard_state;
pub use hard_state::HardState;

pub mod conf;
pub use conf::{
  ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};

mod quorum;
pub(crate) use quorum::{JointConfig, MajorityConfig, VoteResult};

mod message;
pub use message::{
  AppendEntries, AppendResp, Heartbeat, HeartbeatResp, InstallSnapshot, Message, Outgoing,
  ReadIndex, ReadIndexResp, RequestVote, SnapshotMeta, SnapshotResp, TimeoutNow, VoteResp,
};

mod state_machine;
pub use state_machine::StateMachine;

mod storage;
pub use storage::{LogDone, LogStore, OpId, StableDone, StableStore};

mod error;
pub use error::{ConfigError, ProposeError, ReadIndexError, TransferError};

mod inflights;
pub(crate) use inflights::Inflights;

mod progress;
pub(crate) use progress::{Progress, ProgressState};

mod config;
pub use config::{Config, ReadOnlyOption};

mod read_only;
pub(crate) use read_only::ReadOnly;
pub use read_only::ReadState;

mod event;
pub use event::{Applied, ConfChanged, Event, LeaderChanged};

mod endpoint;
pub use endpoint::{Endpoint, PoisonReason, Role, TimerKind};

mod tracker;
pub(crate) use tracker::Tracker;

#[cfg(test)]
pub(crate) mod testkit;
