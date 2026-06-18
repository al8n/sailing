//! Proactor (compio) reference driver for `sailing-proto`.
//!
//! A single compio task owns a [`sailing_proto::quic::QuicCoordinator`] (or a
//! [`sailing_proto::StreamCoordinator`]) plus the embedder's
//! [`LogStore`](sailing_proto::LogStore)/[`StableStore`](sailing_proto::StableStore) and the
//! socket(s), and drives consensus over real I/O. The driver is generic over the state machine
//! and storage — it bundles no backend.
//!
//! # Scaling across cores
//!
//! One consensus group is one serial state machine: a single driver owns its coordinator,
//! storage, and socket, and `run()` drives them on one thread. The compio runtime's `spawn`
//! takes plain `!Send` futures and never migrates a task, so every task a driver creates — the
//! run loop, its persistent recv/accept tasks, the per-connection bridges — stays on the thread
//! that spawned it, by construction. There is no parallelism inside a group, and none would
//! help: consensus applies committed entries in log order, so one group's throughput ceiling is
//! one core by design.
//!
//! Scale-out is therefore N INDEPENDENT groups, not more threads in one group: one driver plus
//! one compio `Runtime` per thread, each driver binding its own socket/port and forming its own
//! cluster mesh. Groups share nothing — separate endpoints, separate stores, separate sockets.
//!
//! [`Handle`]s are the only objects meant to cross threads: a `Handle` is `Send + Sync` and
//! O(1) to clone, so any thread may submit to any group and await the committed reply — the
//! bounded command channel and the per-submit reply channel do the crossing.
//!
//! The one footgun: a compio socket attaches to the proactor of the thread that CONSTRUCTS it,
//! exactly once, so each driver must be constructed AND run on its own thread — build it inside
//! that thread's `Runtime` (e.g. at the top of its `block_on`), never on a coordinating thread
//! that then ships it elsewhere.

mod bridge;
mod clock;
mod config;
mod error;
mod handle;
mod quic_driver;
mod shared;
mod stream_driver;
mod wall_clock;

pub use clock::Clock;
pub use config::DriverConfig;
pub use error::{BindError, DriverError};
pub use handle::Handle;
pub use quic_driver::CompioQuicDriver;
pub use stream_driver::{AcceptorFactory, CompioStreamDriver, DialerFactory};
#[cfg(feature = "unverified-wall-clock")]
pub use wall_clock::UnverifiedSystemClock;
pub use wall_clock::{Monotonic, NtpDisciplinedClock, WallClock, WallReading};
