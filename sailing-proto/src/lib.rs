//! Sans-I/O Raft consensus core. See `docs/superpowers/specs/2026-06-02-sailing-design.md`.
//!
//! `alloc` is the heap floor; `std` layers OS facilities on top. `std` and `alloc` are independent
//! features and the crate requires at least one of them; there is no no-alloc (`heapless`) tier in v1.
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(all(not(feature = "std"), feature = "alloc"))]
extern crate alloc as std;
#[cfg(feature = "std")]
extern crate std;

#[cfg(not(any(feature = "std", feature = "alloc")))]
compile_error!("sailing-proto requires at least one of the `std` or `alloc` features");

mod prng;
pub use prng::Prng;

mod num;
pub use num::{Index, Term};

mod id;
pub use cheap_clone::CheapClone;
pub use id::NodeId;

mod time;
pub use time::{Instant, Now, Wall};

mod data;
pub use data::{ByteCursor, Data, DecodeError};

mod entry;
pub use entry::{Entry, EntryKind};

mod maybe_owned;
pub use maybe_owned::MaybeOwned;

mod hard_state;
pub use hard_state::{HardState, LeaseSupport};

pub mod conf;
pub use conf::{
  ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};

mod quorum;
pub(crate) use quorum::{JointConfig, MajorityConfig, VoteResult};

mod message;
pub mod wire;
pub use message::{
  AppendEntries, AppendResponse, Heartbeat, HeartbeatResponse, InstallSnapshot, Message, Outgoing,
  ReadIndex, ReadIndexResponse, RequestVote, SnapshotMeta, SnapshotResponse, TimeoutNow,
  VoteResponse,
};

mod state_machine;
pub use state_machine::StateMachine;

mod storage;
pub use storage::{EntriesRead, LogDone, LogStore, OpId, StableDone, StableStore, StorageProgress};

mod error;
pub use error::{ConfigError, ProposeError, ReadIndexError, TransferError};

mod inflights;
pub(crate) use inflights::Inflights;

mod progress;
pub(crate) use progress::Progress;
pub use progress::ProgressState;

mod config;
pub use config::{Config, LeaseRefresh, ReadOnlyOption};

mod read_only;
pub(crate) use read_only::ReadOnly;
pub use read_only::{FailoverReadWindow, ReadState};

mod event;
pub use event::{Applied, ConfChanged, Event, LeaderChanged, ReadModeChanged};

mod endpoint;
pub use endpoint::{Endpoint, PeerProgress, PoisonReason, Role, TimerKind};

mod tracker;
pub(crate) use tracker::Tracker;

/// Decode entry points exposed ONLY for the `sailing-proto/fuzz` cargo-fuzz crate, behind the
/// `fuzzing` feature (off by default, never enabled by a normal build). These thin wrappers
/// reach `pub(crate)` codec paths the harness cannot otherwise call, monomorphized to a `u64`
/// id. They are NOT part of the public API or the semver surface — do not depend on this module.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzz_internals {
  use crate::ConfChangeV2;
  use bytes::Bytes;
  use std::vec::Vec;

  /// [`wire::decode_conf_change_v2`](crate::wire) over a `u64` id — the entry-payload decode
  /// path (the apply-poison surface).
  pub fn decode_conf_change_v2(data: Bytes) -> Result<ConfChangeV2<u64>, crate::DecodeError> {
    crate::wire::decode_conf_change_v2::<u64>(data)
  }

  /// [`wire::encode_conf_change_v2`](crate::wire) over a `u64` id.
  pub fn encode_conf_change_v2(cc: &ConfChangeV2<u64>, buf: &mut Vec<u8>) {
    crate::wire::encode_conf_change_v2::<u64>(cc, buf)
  }

  /// Drive the stream `FrameDecoder` over a chunked byte stream (modelling
  /// arbitrary socket reads): push each chunk, drain every complete frame. Returns the frames
  /// or the terminal decode error. The harness asserts no panic and that every yielded frame
  /// is within [`MAX_FRAME_LEN`].
  #[cfg(feature = "tcp")]
  pub fn drive_frame_decoder(chunks: &[Vec<u8>]) -> Result<Vec<Bytes>, crate::TransportError> {
    let mut dec = crate::transport::frame::FrameDecoder::new();
    let mut out = Vec::new();
    for chunk in chunks {
      dec.push(chunk);
      while let Some(frame) = dec.poll()? {
        out.push(frame);
      }
    }
    Ok(out)
  }

  /// The frame-length bound, for the frame-decoder harness's post-condition.
  #[cfg(feature = "tcp")]
  pub const MAX_FRAME_LEN: usize = crate::transport::frame::MAX_FRAME_LEN;
}

#[cfg(any(feature = "tcp", feature = "quic"))]
mod transport;
#[cfg(feature = "tls")]
pub use transport::TlsRecords;
#[cfg(feature = "quic")]
pub use transport::quic;
#[cfg(any(feature = "tcp", feature = "quic"))]
pub use transport::{ClusterId, ConnId, ConnRole, Peer, TransportError};
#[cfg(feature = "tcp")]
pub use transport::{
  Intake, LabelOptions, Labeled, Passthrough, RecordIo, StreamCoordinator, StreamTransport,
};

#[cfg(test)]
pub(crate) mod testkit;
