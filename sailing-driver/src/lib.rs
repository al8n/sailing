//! Runtime-agnostic driver core for `sailing-proto`, shared by the proactor (`sailing-compio`) and
//! reactor (`sailing-reactor`) drivers.
//!
//! This crate holds the parts of a driver that do not touch a runtime, a socket, or an I/O model:
//! the cross-thread [`Handle`] and its command channel, the [`DriverConfig`], the [`Clock`] /
//! [`WallClock`] time seam, the inflight budget and event/reply routing, and the driver error
//! types. The I/O-specific halves — the socket bridges, the run loops, and the per-runtime entry
//! points — live in the driver crates that depend on this one.

mod error;
mod wall_clock;

pub use error::{BindError, DriverConfigError, DriverError};
#[cfg(feature = "unverified-wall-clock")]
pub use wall_clock::UnverifiedSystemClock;
pub use wall_clock::{Monotonic, NtpDisciplinedClock, WallClock, WallReading};
