//! Reactor-I/O (tokio/smol via [`agnostic`]) reference driver for `sailing-proto` — the readiness
//! sibling of the proactor [`sailing-compio`](https://docs.rs/sailing-compio).
//!
//! The driver is generic over any [`agnostic::Runtime`]: its `run()` future is `Send` and rides a
//! work-stealing runtime, so a group stays serial because it is ONE task, not by thread-pinning.
//! Because the driver may migrate across worker threads, its channels and shared counters are
//! `flume` + `Arc`, never compio's thread-per-core `lochan` + `Rc`.
//!
//! The runtime-agnostic core — the [`Handle`], [`DriverConfig`], the [`Clock`] / [`WallClock`] seam,
//! the inflight budget, and the routing — lives in `sailing-driver` and is shared with sailing-compio;
//! this crate provides only the readiness I/O half (the socket bridges and the run loop).

mod task;

#[cfg(feature = "unverified-wall-clock")]
pub use sailing_driver::UnverifiedSystemClock;
pub use sailing_driver::{
  BindError, Clock, DriverConfig, DriverConfigError, DriverError, Handle, MAX_BOUNDED_QUEUE_DEPTH,
  MAX_CHANNEL_CAPACITY, MAX_REDIAL_BACKOFF, Monotonic, NtpDisciplinedClock, WallClock, WallReading,
};
