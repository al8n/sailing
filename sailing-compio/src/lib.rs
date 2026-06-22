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
//!
//! # The LeaseGuard failover tier (synchronized wall clock)
//!
//! LeaseGuard lets a freshly-elected leader release its post-election commit-wait EARLY — as soon as a
//! precise wall-clock anchor proves the deposed leader's inherited lease has expired — AND serve
//! linearizable reads on the inherited committed prefix WHILE that wait holds, instead of waiting out a
//! conservative monotonic deadline before either. That anchor compares timestamps ACROSS nodes, so
//! unlike the steady-state lease (which needs only local monotonic clocks) it requires SYNCHRONIZED
//! wall clocks with a bounded cross-node error `ε_unc`. This driver supplies that wall through a
//! [`WallClock`] source selected as a type parameter; the default [`Monotonic`] supplies none and the
//! tier stays inert.
//!
//! ## Enabling it
//!
//! Configure the failover tier on the proto `Config` (the LeaseGuard read-only option, a lease
//! duration, a clock-drift bound, and `bounded_clock_uncertainty` = `ε_unc`), then bind with a
//! synchronized source via `bind_with_wall_clock` instead of `bind`:
//!
//! ```ignore
//! let config = Config::try_new(id, voters, election_timeout, heartbeat)?
//!     .with_read_only(ReadOnlyOption::LeaseGuard)
//!     .with_lease_duration(Duration::from_millis(200))
//!     .with_clock_drift_bound(Duration::from_millis(2))
//!     .with_bounded_clock_uncertainty(Duration::from_millis(5)); // ε_unc
//! let (driver, handle) = CompioQuicDriver::bind_with_wall_clock(
//!     addr, config, seed, fsm, opts, cluster, peers, log, stable,
//!     NtpDisciplinedClock,          // the synchronized wall source (a ZST, passed by value)
//!     DriverConfig::default(),
//! ).await?;
//! ```
//!
//! The default `bind` uses [`Monotonic`]; a failover `Config` paired with it is REJECTED at bind
//! ([`BindError::MissingWallSource`]) rather than silently degrading to a tier that never fires.
//!
//! ## Serving inherited reads
//!
//! While the commit-wait holds, [`Handle::failover_query`] serves a linearizable read on the inherited
//! committed prefix: the closure runs ON the driver thread against the FSM and the limbo region
//! `(index, limbo_upper]`, returning `Some(out)` to serve — having confirmed the read's key was not
//! written in that region (the proto is key-agnostic; the closure owns the command format) — or `None`
//! to decline. The call resolves to `Ok(None)` when no serve window is available (commit-wait already
//! lifted — read normally — inherited lease expired, or off the failover tier); the caller then falls
//! back to [`Handle::query`].
//!
//! ## The operator contract (READ THIS)
//!
//! `ε_unc` is an ASSERTION the library cannot verify: every node must keep `|W(t) − t| ≤ ε_unc`
//! against a SHARED epoch, under the same leap-second policy. [`NtpDisciplinedClock`] enforces it on
//! Linux by reading the kernel's NTP sync state and supplying no wall when the clock is unsynchronized
//! or its own worst-case error exceeds `ε_unc` — but a clock that LIES (claims a small error it does
//! not hold) can still serve a STALE read. `UnverifiedSystemClock` (a raw `SystemTime` that always
//! claims zero error) is NEVER the production path; it sits behind the off-by-default
//! `unverified-wall-clock` feature for tests only. Epoch + leap-policy agreement across nodes is as
//! load-bearing as `ε_unc` itself.
//!
//! ## Observing it
//!
//! `precise_releases` counts commit-waits the precise wall anchor released EARLY — nonzero proves the
//! tier is live end-to-end. `unprovable_floor_holds` counts waits held conservatively for want of a
//! provable wall — nonzero in a configured-failover deployment flags a node OUTSIDE the clock contract
//! (an unsynchronized clock, a missing source), the intended backstop rather than a wiring fault.

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
pub use config::{DriverConfig, MAX_CHANNEL_CAPACITY, MAX_REDIAL_BACKOFF};
pub use error::{BindError, DriverConfigError, DriverError};
pub use handle::Handle;
pub use quic_driver::CompioQuicDriver;
pub use stream_driver::{AcceptorFactory, CompioStreamDriver, DialerFactory};
#[cfg(feature = "unverified-wall-clock")]
pub use wall_clock::UnverifiedSystemClock;
pub use wall_clock::{Monotonic, NtpDisciplinedClock, WallClock, WallReading};
