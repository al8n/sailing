//! Runtime-agnostic driver core for `sailing-proto`, shared by the proactor (`sailing-compio`) and
//! reactor (`sailing-reactor`) drivers.
//!
//! This crate holds the parts of a driver that do not touch a runtime, a socket, or an I/O model:
//! the cross-thread [`Handle`] and its command channel, the [`DriverConfig`], the [`Clock`] /
//! [`WallClock`] time seam, the inflight budget and event/reply routing, and the driver error
//! types. The I/O-specific halves — the socket bridges, the run loops, and the per-runtime entry
//! points — live in the driver crates that depend on this one.

mod clock;
mod config;
mod error;
mod handle;
pub mod shared;
mod wall_clock;

pub use clock::{Clock, jittered, validate_and_capture_eps};
pub use config::{DriverConfig, MAX_BOUNDED_QUEUE_DEPTH, MAX_CHANNEL_CAPACITY, MAX_REDIAL_BACKOFF};
pub use error::{BindError, DriverConfigError, DriverError};
pub use handle::{Command, Handle, Status};
#[cfg(feature = "unverified-wall-clock")]
pub use wall_clock::UnverifiedSystemClock;
pub use wall_clock::{Monotonic, NtpDisciplinedClock, WallClock, WallReading};
