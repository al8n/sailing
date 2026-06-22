//! Driver tuning knobs. Every value here SIZES a bound documented in the crate's memory model;
//! none can remove one.

use std::time::Duration;

use crate::error::DriverConfigError;

/// How many in-flight submits the budget admits by default.
pub(crate) const DEFAULT_MAX_INFLIGHT: usize = 1_024;
/// How many bytes of in-flight submit payload the budget admits by default.
pub(crate) const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
/// Default byte cap on the failover inherited-read limbo scan (matches the submit byte budget — the
/// driver's existing single-operation memory ceiling).
pub(crate) const DEFAULT_MAX_FAILOVER_LIMBO_BYTES: usize = 64 * 1024 * 1024;
/// Default capacity of the best-effort events tail.
pub(crate) const DEFAULT_EVENTS_CAP: usize = 1_024;
/// Default per-iteration command-drain budget (fairness against submit floods).
pub(crate) const DEFAULT_CMD_BUDGET: usize = 32;
/// Default capacity of the QUIC datagram channel (recv task → run loop).
pub(crate) const DEFAULT_RECV_CAP: usize = 256;
/// Default capacity of the stream drivers' shared inbound channel (bridges → run loop).
pub(crate) const DEFAULT_INBOUND_CAP: usize = 256;
/// Default capacity of the accept channel (accept task → run loop).
pub(crate) const DEFAULT_ACCEPT_CAP: usize = 16;
/// Default per-connection outbound byte budget (stream driver write queues).
pub(crate) const DEFAULT_OUTBOUND_BACKLOG: usize = 8 * 1024 * 1024;
/// Default cap on live stream-driver connections (accept admission control).
pub(crate) const DEFAULT_MAX_CONNS: usize = 64;
/// Default initial redial backoff.
pub(crate) const DEFAULT_REDIAL_BASE: Duration = Duration::from_millis(100);
/// Default redial backoff ceiling.
pub(crate) const DEFAULT_REDIAL_CAP: Duration = Duration::from_secs(5);

/// Inclusive ceiling for the LAZILY-allocated channel-sizing knobs (`max_inflight`, `events_cap`).
///
/// The command channel itself is now `flume::unbounded` and carries no cap — but `max_inflight`
/// still SIZES the submit budget, so this ceiling keeps a pathological budget from admitting an
/// astronomical pending count. `events_cap` sizes a `flume::bounded` channel whose `VecDeque` grows
/// only as it fills, so its hard limit is just the `cap == usize::MAX` `+ 1` overflow in the channel's
/// pending arithmetic; one shared ceiling well below that covers both.
///
/// The three LOCHAN-backed caps (`recv_cap`, `inbound_cap`, `accept_cap`) do NOT use this ceiling:
/// `lochan::mpsc::bounded(cap)` EAGER-allocates a `cap`-slot ring up front (see
/// [`MAX_BOUNDED_QUEUE_DEPTH`]), so a `(usize::MAX >> 2)`-scale value would OOM at bind, not lazily.
/// They get the far tighter [`MAX_BOUNDED_QUEUE_DEPTH`] instead.
///
/// The value is `(usize::MAX >> 2) − 1`, derived from `usize::MAX` so it is correct on any target
/// width (≈ 4.6×10¹⁸ on 64-bit, ≈ 1.07×10⁹ on 32-bit) and astronomically above any realistic channel
/// depth (the defaults are in the hundreds to low thousands). It is the ceiling, not a policy cap on
/// the operator's memory budget.
pub const MAX_CHANNEL_CAPACITY: usize = (usize::MAX >> 2) - 1;

/// Inclusive ceiling for the three EAGER-RING channel caps (`recv_cap`, `inbound_cap`, `accept_cap`).
///
/// Unlike `events_cap`'s `flume::bounded` (a lazily-growing `VecDeque`), `lochan::mpsc::bounded(cap)`
/// allocates a FIXED ring of exactly `cap` `MaybeUninit` slots in ONE allocation up front — so `cap` is
/// an immediate memory commitment at bind, not a lazy fill bound. A `cap` near [`MAX_CHANNEL_CAPACITY`]
/// (≈ `usize::MAX / 4`) would therefore try to allocate ~`usize::MAX / 4` elements and OOM / abort at
/// bind, so these three caps need a realistic eager-allocation ceiling rather than the lazy one.
///
/// `1 << 19` (524 288) is chosen WELL ABOVE the defaults — `recv_cap` / `inbound_cap` default to 256
/// and `accept_cap` to 16, so this leaves > 2000× headroom and normal tuning is unaffected — yet bounds
/// each ring to a sane up-front size. The slot element is the largest of `(Vec<u8>, SocketAddr)`
/// (`recv_cap`), `BridgeInbound` (`inbound_cap`), and `(TcpStream, SocketAddr)` (`accept_cap`), each on
/// the order of tens of bytes, so a full `1 << 19`-slot ring is roughly 524 288 × ~56 B ≈ 30 MB at the
/// very top — a low-tens-of-MB per-queue commitment, not an OOM. It is the eager-allocation safety
/// ceiling, not a policy cap on the operator's memory budget.
pub const MAX_BOUNDED_QUEUE_DEPTH: usize = 1 << 19;

/// `Instant`-safe ceiling for the redial backoff durations (`redial_base`, `redial_cap`).
///
/// A redial backoff is DOUBLED per attempt (`delay * 2`), JITTERED up to +25% (`jittered`, internally
/// `delay * 255 / 1024`), and ADDED to a `std::time::Instant` to schedule the next attempt — and both
/// `Duration * u32` and `Instant + Duration` PANIC on overflow. The largest value any of that math
/// sees is `redial_cap` (the doubling clamps there and the first delay is `redial_base ≤ redial_cap`),
/// so bounding both knobs to this ceiling keeps `delay * 255` (the largest jitter intermediate),
/// `delay * 2`, and `Instant + jittered(delay)` all overflow-free.
///
/// Mirrors the proto's `MAX_TIMER_MILLIS` (`u32::MAX` ms ≈ 49.7 days): comfortably below the `Instant`
/// overflow threshold (`u32::MAX` ms × 255 ≈ 34.7 years is far under `Duration::MAX`, and the `Instant`
/// addition of ≤ 1.25× this stays in range), yet far above any realistic RTT-scale redial backoff (the
/// default ceiling is 5 s). It is the `Instant`-safety bound, not a policy cap.
pub const MAX_REDIAL_BACKOFF: Duration = Duration::from_millis(u32::MAX as u64);

// `serde(default = "…")` needs a function PATH, not a const, so each knob's deserialize default is
// wrapped to return the single-source-of-truth `DEFAULT_*` const — the same consts the `Default` impl
// and clap's `default_value_t` use, so the three default paths can never drift. Gated on `serde` so
// the default build stays warning-free.
#[cfg(feature = "serde")]
const fn default_max_inflight() -> usize {
  DEFAULT_MAX_INFLIGHT
}
#[cfg(feature = "serde")]
const fn default_max_pending_bytes() -> usize {
  DEFAULT_MAX_PENDING_BYTES
}
#[cfg(feature = "serde")]
const fn default_events_cap() -> usize {
  DEFAULT_EVENTS_CAP
}
#[cfg(feature = "serde")]
const fn default_cmd_budget() -> usize {
  DEFAULT_CMD_BUDGET
}
#[cfg(feature = "serde")]
const fn default_recv_cap() -> usize {
  DEFAULT_RECV_CAP
}
#[cfg(feature = "serde")]
const fn default_inbound_cap() -> usize {
  DEFAULT_INBOUND_CAP
}
#[cfg(feature = "serde")]
const fn default_accept_cap() -> usize {
  DEFAULT_ACCEPT_CAP
}
#[cfg(feature = "serde")]
const fn default_max_outbound_backlog() -> usize {
  DEFAULT_OUTBOUND_BACKLOG
}
#[cfg(feature = "serde")]
const fn default_max_conns() -> usize {
  DEFAULT_MAX_CONNS
}
#[cfg(feature = "serde")]
const fn default_redial_base() -> Duration {
  DEFAULT_REDIAL_BASE
}
#[cfg(feature = "serde")]
const fn default_redial_cap() -> Duration {
  DEFAULT_REDIAL_CAP
}
#[cfg(feature = "serde")]
const fn default_max_failover_limbo_bytes() -> usize {
  DEFAULT_MAX_FAILOVER_LIMBO_BYTES
}

/// Tuning for a [`CompioQuicDriver`](crate::CompioQuicDriver) /
/// `CompioStreamDriver`. `Default` is sized for a small LAN cluster; every knob
/// adjusts the SIZE of a documented bound, never removes it.
///
/// The optional `serde` / `clap` layers (re)serialize / expose every TUNING knob — the two
/// `Duration`s as humantime strings (via `humantime-serde` / `humantime::parse_duration`), the rest
/// directly — each defaulting to its single-source-of-truth `DEFAULT_*` const, so a partial config
/// file or a flag-free CLI yields a config equal to [`Default`]. The runtime `storage_ready` CHANNEL
/// is neither serializable nor a CLI flag: it is `serde(skip)` / `arg(skip)`, stays `None` across both
/// paths, and is wired in programmatically.
///
/// Both PARSE paths (serde `Deserialize`, clap `FromArgMatches`) funnel through the validating
/// [`Self::validate`], so a config-file or CLI/env value that would wedge a bounded queue or overflow
/// a channel-capacity computation is rejected at parse time (see [`DriverConfigError`]). The direct
/// `serde::Serialize` derive needs no validation. A programmatically-constructed `DriverConfig` is NOT
/// validated; the drivers additionally harden the capacity arithmetic so even an extreme programmatic
/// value cannot panic.
///
/// **Per-knob runtime sink → bound.** Every knob is traced to the runtime primitive its parsed value
/// reaches, and bounded to that sink's hard limit so no parsed value can crash a primitive:
///
/// | Knob | Runtime sink | Bound |
/// |------|--------------|-------|
/// | `max_inflight` | the submit budget (`flume::unbounded` command channel; budget is the bound) | `1 ..= MAX_CHANNEL_CAPACITY − 1` |
/// | `events_cap` | `flume::bounded(events_cap)` (both drivers; lazy `VecDeque`) | `1 ..= MAX_CHANNEL_CAPACITY` |
/// | `recv_cap` | `lochan::mpsc::bounded(recv_cap)` (QUIC datagram channel; EAGER ring) | `1 ..= MAX_BOUNDED_QUEUE_DEPTH` |
/// | `inbound_cap` | `lochan::mpsc::bounded(inbound_cap)` (stream inbound channel; EAGER ring) | `1 ..= MAX_BOUNDED_QUEUE_DEPTH` |
/// | `accept_cap` | `lochan::mpsc::bounded(accept_cap)` (accept channel; EAGER ring) | `1 ..= MAX_BOUNDED_QUEUE_DEPTH` |
/// | `redial_base` | doubled + jittered + added to `std::time::Instant` (redial schedule) | `(0, MAX_REDIAL_BACKOFF]`, `≤ redial_cap` |
/// | `redial_cap` | the doubling ceiling — the largest value the jitter + `Instant` math sees | `(0, MAX_REDIAL_BACKOFF]` |
/// | `cmd_budget` | per-iteration loop counter (`for _ in 0..cmd_budget`) — no panic sink | non-zero only |
/// | `max_conns` | live-connection count comparison — no panic sink | non-zero only |
/// | `max_pending_bytes` | in-flight byte-budget comparison — no panic, no eager alloc | non-zero only |
/// | `max_outbound_backlog` | per-connection byte-budget comparison — no panic, no eager alloc | non-zero only |
/// | `max_failover_limbo_bytes` | limbo-scan byte-budget comparison — no panic, no eager alloc | non-zero only |
///
/// The byte budgets and the two pure counters drive only comparisons / a bounded loop (never a channel
/// assert, a `Duration`/`Instant` overflow, or an eager allocation of their value's worth of slots), so
/// they are checked only for the zero hazard — there is deliberately no arbitrary upper cap on what is
/// simply the operator's memory budget.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "DriverConfigSerde"))]
pub struct DriverConfig {
  /// In-flight submit cap (count). Exhaustion returns
  /// [`DriverError::Busy`](crate::DriverError::Busy) at the handle, before anything is queued.
  pub max_inflight: usize,
  /// In-flight submit cap (payload bytes).
  pub max_pending_bytes: usize,
  /// Events-tail capacity. The tail is BEST-EFFORT: when full, new events are dropped for the
  /// tail (never for the pending completions, which ride their own oneshots).
  pub events_cap: usize,
  /// Commands drained per run-loop iteration before the I/O select (fairness: a continuous
  /// submit stream cannot starve inbound I/O or timers).
  pub cmd_budget: usize,
  /// QUIC datagram channel capacity (recv task → run loop). When full the recv task parks, no
  /// receive is in flight, and arrivals queue in — then overflow — the kernel socket buffer:
  /// exactly UDP backpressure, whose drops QUIC loss recovery absorbs.
  pub recv_cap: usize,
  /// Stream drivers' shared inbound frame-channel capacity (bridge readers → run loop). Full ⇒
  /// the reader task parks ⇒ TCP backpressure on that peer.
  pub inbound_cap: usize,
  /// Accept channel capacity (accept task → run loop). Full ⇒ the accept task parks and the
  /// kernel listen backlog is the overflow.
  pub accept_cap: usize,
  /// Per-connection outbound byte budget (stream driver). An enqueue that would exceed it
  /// closes the connection — the peer has stopped consuming.
  pub max_outbound_backlog: usize,
  /// Live-connection cap for the stream driver's ACCEPT admission (floored at construction to
  /// twice the peer book so the mutual-dial mesh always fits). Mesh dials are never refused —
  /// consensus liveness — so the table's true worst-case occupancy is this cap plus the
  /// missing-mesh dial count (at most the peer book): accepted sockets are bounded here, dialed
  /// ones by the peer book itself.
  pub max_conns: usize,
  /// Initial redial backoff (jittered, doubled per attempt up to [`Self::redial_cap`]) — and
  /// the LINK-STABILITY window: a connection bound continuously for at least this long resets
  /// its peer's backoff to base. Keep it above the network's close-propagation time (an
  /// RTT-scale bound), so the mutual-dial tie-break's transient survivors — validated bindings
  /// that die within an RTT — never reset the backoff whose doubling de-synchronizes the two
  /// sides' redials and makes the race converge.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  pub redial_base: Duration,
  /// Redial backoff ceiling.
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  pub redial_cap: Duration,
  /// Wake signal for genuinely-ASYNC stores: the embedder clones a sender into its store and
  /// signals it on each I/O completion; the run loop drains it to empty each iteration and a
  /// signal wakes a sleeping loop so `handle_storage` runs promptly. Synchronous stores leave
  /// this `None` — `handle_storage` already runs every iteration.
  ///
  /// A runtime `flume::Receiver` is neither serializable nor a CLI flag, so it is skipped by both
  /// optional layers: `serde(skip)` deserializes it to `None` (and omits it on serialize), and
  /// `arg(skip)` keeps it off the CLI as `None`. It is wired in programmatically after parsing.
  #[cfg_attr(feature = "serde", serde(skip))]
  pub storage_ready: Option<flume::Receiver<()>>,
  /// Byte cap on the failover inherited-read limbo scan — the `(commit, limbo_upper]` region a
  /// failover query checks its key against. A post-election limbo larger than this (an unbounded
  /// inherited tail) falls the query back to a normal read instead of loading the whole tail: the read
  /// is NOT charged to the submit budget, so this is the bound that keeps it from OOMing or stalling
  /// the driver.
  pub max_failover_limbo_bytes: usize,
}

impl DriverConfig {
  /// Validate the tuning knobs. Both PARSE paths (serde `Deserialize` and clap `FromArgMatches`)
  /// route through here, so a config-file or CLI/env value that would wedge a bounded queue or
  /// overflow a channel-capacity computation is rejected at parse time rather than building a driver
  /// that panics or hot-loops at run time.
  ///
  /// The checks cover the CONCRETE hazards — a parsed value reaching a runtime primitive with a hard
  /// limit (a channel-capacity ceiling, a `Duration`/`Instant` overflow) — not arbitrary policy caps:
  ///
  /// - `max_inflight` must be non-zero (a zero budget admits no submit ⇒ no progress) AND must stay
  ///   below the channel ceiling: the command channel is `flume::unbounded`, so this bounds the
  ///   submit BUDGET rather than a channel buffer, rejecting `usize::MAX` and, more tightly, any
  ///   value above [`MAX_CHANNEL_CAPACITY`] − 1 so a pathological budget can never admit an
  ///   astronomical pending count;
  /// - `cmd_budget` must be non-zero (a zero per-iteration drain budget stalls every submit);
  /// - `events_cap` must be non-zero (a zero-capacity bounded channel can never hold its item ⇒ the
  ///   producing task is wedged) AND at most [`MAX_CHANNEL_CAPACITY`] (it sizes a lazily-growing
  ///   `flume::bounded`, so the ceiling only guards the channel's pending arithmetic);
  /// - `recv_cap` / `inbound_cap` / `accept_cap` must each be non-zero (same wedge) AND at most
  ///   [`MAX_BOUNDED_QUEUE_DEPTH`] (the far tighter EAGER-allocation ceiling: each sizes a
  ///   `lochan::mpsc::bounded` ring that allocates all `cap` slots UP FRONT, so an astronomical value
  ///   would OOM at bind rather than fill lazily);
  /// - `max_pending_bytes` / `max_outbound_backlog` / `max_conns` / `max_failover_limbo_bytes` must
  ///   each be non-zero (a zero turns the corresponding budget/cap into a reject-everything gate);
  /// - `redial_base` and `redial_cap` must each be non-zero (a zero backoff is a hot retry loop),
  ///   `redial_base <= redial_cap` (the ceiling must not sit below the floor), AND each at most
  ///   [`MAX_REDIAL_BACKOFF`] (the backoff is doubled, jittered, and added to a `std::time::Instant`,
  ///   which a near-`Duration::MAX` value would overflow and panic at the first redial).
  ///
  /// The byte-budget knobs (`max_pending_bytes` / `max_outbound_backlog` / `max_conns` /
  /// `max_failover_limbo_bytes`) are checked only for the zero hazard above: each is simply the
  /// operator's memory budget, drives no channel-capacity assert and no `Duration`/`Instant` overflow,
  /// so there is deliberately no arbitrary UPPER cap on it (see the type docs' per-knob sink table).
  pub fn validate(&self) -> Result<(), DriverConfigError> {
    if self.max_inflight == 0 {
      return Err(DriverConfigError::ZeroMaxInflight);
    }
    if self.max_inflight == usize::MAX {
      return Err(DriverConfigError::MaxInflightOverflow);
    }
    // The command channel is `flume::unbounded`, so this caps the submit BUDGET (one below the
    // shared ceiling), keeping a pathological budget from admitting an astronomical pending count.
    if self.max_inflight > MAX_CHANNEL_CAPACITY - 1 {
      return Err(DriverConfigError::MaxInflightAboveChannelCeiling);
    }
    if self.cmd_budget == 0 {
      return Err(DriverConfigError::ZeroCmdBudget);
    }
    if self.events_cap == 0 {
      return Err(DriverConfigError::ZeroEventsCap);
    }
    if self.events_cap > MAX_CHANNEL_CAPACITY {
      return Err(DriverConfigError::EventsCapAboveChannelCeiling);
    }
    if self.recv_cap == 0 {
      return Err(DriverConfigError::ZeroRecvCap);
    }
    // The lochan ring eager-allocates `recv_cap` slots at bind, so it is bounded by the far tighter
    // eager-allocation ceiling, not the lazy channel one.
    if self.recv_cap > MAX_BOUNDED_QUEUE_DEPTH {
      return Err(DriverConfigError::RecvCapAboveQueueCeiling);
    }
    if self.inbound_cap == 0 {
      return Err(DriverConfigError::ZeroInboundCap);
    }
    if self.inbound_cap > MAX_BOUNDED_QUEUE_DEPTH {
      return Err(DriverConfigError::InboundCapAboveQueueCeiling);
    }
    if self.accept_cap == 0 {
      return Err(DriverConfigError::ZeroAcceptCap);
    }
    if self.accept_cap > MAX_BOUNDED_QUEUE_DEPTH {
      return Err(DriverConfigError::AcceptCapAboveQueueCeiling);
    }
    if self.max_pending_bytes == 0 {
      return Err(DriverConfigError::ZeroMaxPendingBytes);
    }
    if self.max_outbound_backlog == 0 {
      return Err(DriverConfigError::ZeroMaxOutboundBacklog);
    }
    if self.max_conns == 0 {
      return Err(DriverConfigError::ZeroMaxConns);
    }
    if self.max_failover_limbo_bytes == 0 {
      return Err(DriverConfigError::ZeroMaxFailoverLimboBytes);
    }
    if self.redial_base.is_zero() {
      return Err(DriverConfigError::ZeroRedialBase);
    }
    if self.redial_cap.is_zero() {
      return Err(DriverConfigError::ZeroRedialCap);
    }
    // Both backoff durations feed the doubling + jitter + `Instant`-addition redial math; a value near
    // `Duration::MAX` would overflow it and panic at the first redial, so each is bounded `Instant`-safe.
    if self.redial_base > MAX_REDIAL_BACKOFF {
      return Err(DriverConfigError::RedialBaseTooLarge);
    }
    if self.redial_cap > MAX_REDIAL_BACKOFF {
      return Err(DriverConfigError::RedialCapTooLarge);
    }
    if self.redial_base > self.redial_cap {
      return Err(DriverConfigError::RedialBaseAboveCap);
    }
    Ok(())
  }
}

impl Default for DriverConfig {
  fn default() -> Self {
    Self {
      max_inflight: DEFAULT_MAX_INFLIGHT,
      max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
      events_cap: DEFAULT_EVENTS_CAP,
      cmd_budget: DEFAULT_CMD_BUDGET,
      recv_cap: DEFAULT_RECV_CAP,
      inbound_cap: DEFAULT_INBOUND_CAP,
      accept_cap: DEFAULT_ACCEPT_CAP,
      max_outbound_backlog: DEFAULT_OUTBOUND_BACKLOG,
      max_conns: DEFAULT_MAX_CONNS,
      redial_base: DEFAULT_REDIAL_BASE,
      redial_cap: DEFAULT_REDIAL_CAP,
      storage_ready: None,
      max_failover_limbo_bytes: DEFAULT_MAX_FAILOVER_LIMBO_BYTES,
    }
  }
}

// The serde / clap parse mirrors below carry the per-knob deserialize defaults / `#[arg(...)]`
// attributes and the skipped runtime channel, then funnel through the validating builder
// [`DriverConfig::from_parts_validated`] via their respective `TryFrom`. Splitting the parse derives
// off the public struct is what lets `validate` gate both paths: serde routes `Deserialize` through
// `DriverConfigSerde` (`#[serde(try_from = …)]`), and the hand-written `FromArgMatches` parses
// `DriverConfigCli` then converts. The public struct keeps only the direct `Serialize` derive.

impl DriverConfig {
  /// Build a `DriverConfig` from already-parsed knob values and a `None` runtime channel, then run
  /// [`Self::validate`]. The two parse mirrors forward field-by-field to here so construction and
  /// validation live in one place; the embedder wires `storage_ready` in afterward.
  #[cfg(any(feature = "serde", feature = "clap"))]
  #[allow(clippy::too_many_arguments)]
  fn from_parts_validated(
    max_inflight: usize,
    max_pending_bytes: usize,
    events_cap: usize,
    cmd_budget: usize,
    recv_cap: usize,
    inbound_cap: usize,
    accept_cap: usize,
    max_outbound_backlog: usize,
    max_conns: usize,
    redial_base: Duration,
    redial_cap: Duration,
    max_failover_limbo_bytes: usize,
  ) -> Result<Self, DriverConfigError> {
    let cfg = Self {
      max_inflight,
      max_pending_bytes,
      events_cap,
      cmd_budget,
      recv_cap,
      inbound_cap,
      accept_cap,
      max_outbound_backlog,
      max_conns,
      redial_base,
      redial_cap,
      storage_ready: None,
      max_failover_limbo_bytes,
    };
    cfg.validate()?;
    Ok(cfg)
  }
}

/// Serde-only parse mirror for [`DriverConfig`]. NOT part of the public API — it carries the serde
/// per-knob deserialize defaults / humantime adapters and is always converted to a VALIDATED
/// [`DriverConfig`] via [`TryFrom`]. The runtime `storage_ready` channel is not a wire field; the
/// conversion supplies `None`.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverConfigSerde {
  #[serde(default = "default_max_inflight")]
  max_inflight: usize,
  #[serde(default = "default_max_pending_bytes")]
  max_pending_bytes: usize,
  #[serde(default = "default_events_cap")]
  events_cap: usize,
  #[serde(default = "default_cmd_budget")]
  cmd_budget: usize,
  #[serde(default = "default_recv_cap")]
  recv_cap: usize,
  #[serde(default = "default_inbound_cap")]
  inbound_cap: usize,
  #[serde(default = "default_accept_cap")]
  accept_cap: usize,
  #[serde(default = "default_max_outbound_backlog")]
  max_outbound_backlog: usize,
  #[serde(default = "default_max_conns")]
  max_conns: usize,
  #[serde(default = "default_redial_base", with = "humantime_serde")]
  redial_base: Duration,
  #[serde(default = "default_redial_cap", with = "humantime_serde")]
  redial_cap: Duration,
  #[serde(default = "default_max_failover_limbo_bytes")]
  max_failover_limbo_bytes: usize,
}

#[cfg(feature = "serde")]
impl TryFrom<DriverConfigSerde> for DriverConfig {
  type Error = DriverConfigError;

  fn try_from(c: DriverConfigSerde) -> Result<Self, Self::Error> {
    Self::from_parts_validated(
      c.max_inflight,
      c.max_pending_bytes,
      c.events_cap,
      c.cmd_budget,
      c.recv_cap,
      c.inbound_cap,
      c.accept_cap,
      c.max_outbound_backlog,
      c.max_conns,
      c.redial_base,
      c.redial_cap,
      c.max_failover_limbo_bytes,
    )
  }
}

/// Clap-only parse mirror for [`DriverConfig`]. NOT part of the public API — it carries the clap
/// `Args` derive and the per-knob `#[arg(...)]` attributes, and is always converted to a VALIDATED
/// [`DriverConfig`] via [`TryFrom`]. The runtime `storage_ready` channel is `arg(skip)` and the
/// conversion supplies `None`.
#[cfg(feature = "clap")]
#[derive(clap::Args)]
struct DriverConfigCli {
  #[arg(
    id = "driver-max-inflight",
    long = "max-inflight",
    env = "SAILING_DRIVER_MAX_INFLIGHT",
    default_value_t = DEFAULT_MAX_INFLIGHT
  )]
  max_inflight: usize,
  #[arg(
    id = "driver-max-pending-bytes",
    long = "max-pending-bytes",
    env = "SAILING_DRIVER_MAX_PENDING_BYTES",
    default_value_t = DEFAULT_MAX_PENDING_BYTES
  )]
  max_pending_bytes: usize,
  #[arg(
    id = "driver-events-cap",
    long = "events-cap",
    env = "SAILING_DRIVER_EVENTS_CAP",
    default_value_t = DEFAULT_EVENTS_CAP
  )]
  events_cap: usize,
  #[arg(
    id = "driver-cmd-budget",
    long = "cmd-budget",
    env = "SAILING_DRIVER_CMD_BUDGET",
    default_value_t = DEFAULT_CMD_BUDGET
  )]
  cmd_budget: usize,
  #[arg(
    id = "driver-recv-cap",
    long = "recv-cap",
    env = "SAILING_DRIVER_RECV_CAP",
    default_value_t = DEFAULT_RECV_CAP
  )]
  recv_cap: usize,
  #[arg(
    id = "driver-inbound-cap",
    long = "inbound-cap",
    env = "SAILING_DRIVER_INBOUND_CAP",
    default_value_t = DEFAULT_INBOUND_CAP
  )]
  inbound_cap: usize,
  #[arg(
    id = "driver-accept-cap",
    long = "accept-cap",
    env = "SAILING_DRIVER_ACCEPT_CAP",
    default_value_t = DEFAULT_ACCEPT_CAP
  )]
  accept_cap: usize,
  #[arg(
    id = "driver-max-outbound-backlog",
    long = "max-outbound-backlog",
    env = "SAILING_DRIVER_MAX_OUTBOUND_BACKLOG",
    default_value_t = DEFAULT_OUTBOUND_BACKLOG
  )]
  max_outbound_backlog: usize,
  #[arg(
    id = "driver-max-conns",
    long = "max-conns",
    env = "SAILING_DRIVER_MAX_CONNS",
    default_value_t = DEFAULT_MAX_CONNS
  )]
  max_conns: usize,
  #[arg(
    id = "driver-redial-base",
    long = "redial-base",
    env = "SAILING_DRIVER_REDIAL_BASE",
    value_parser = humantime::parse_duration,
    default_value = "100ms"
  )]
  redial_base: Duration,
  #[arg(
    id = "driver-redial-cap",
    long = "redial-cap",
    env = "SAILING_DRIVER_REDIAL_CAP",
    value_parser = humantime::parse_duration,
    default_value = "5s"
  )]
  redial_cap: Duration,
  #[arg(
    id = "driver-max-failover-limbo-bytes",
    long = "max-failover-limbo-bytes",
    env = "SAILING_DRIVER_MAX_FAILOVER_LIMBO_BYTES",
    default_value_t = DEFAULT_MAX_FAILOVER_LIMBO_BYTES
  )]
  max_failover_limbo_bytes: usize,
}

#[cfg(feature = "clap")]
impl TryFrom<DriverConfigCli> for DriverConfig {
  type Error = DriverConfigError;

  fn try_from(c: DriverConfigCli) -> Result<Self, Self::Error> {
    Self::from_parts_validated(
      c.max_inflight,
      c.max_pending_bytes,
      c.events_cap,
      c.cmd_budget,
      c.recv_cap,
      c.inbound_cap,
      c.accept_cap,
      c.max_outbound_backlog,
      c.max_conns,
      c.redial_base,
      c.redial_cap,
      c.max_failover_limbo_bytes,
    )
  }
}

#[cfg(feature = "clap")]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  // Map a parse-time [`DriverConfigError`] to a clap value-validation error so an invalid CLI/env
  // config surfaces through clap's own error path (exit code, formatted message) rather than building
  // a driver that panics or hot-loops.
  fn cfg_err(e: DriverConfigError) -> Error {
    Error::raw(clap::error::ErrorKind::ValueValidation, e)
  }

  impl Args for DriverConfig {
    fn augment_args(cmd: Command) -> Command {
      DriverConfigCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      DriverConfigCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for DriverConfig {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      // Parse the mirror, then route through the VALIDATING `TryFrom` so an invalid CLI/env config
      // is rejected at parse time, not silently built.
      let cli = DriverConfigCli::from_arg_matches(m)?;
      DriverConfig::try_from(cli).map_err(cfg_err)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // TRANSACTIONAL update: apply every override to a `candidate` CLONE, validate it, and commit
      // back to `self` only on success. A rejected update (e.g. `--max-inflight 0`) leaves `self`
      // byte-for-byte unchanged, so a caller that catches the clap error keeps a still-valid config.
      // The clone preserves the runtime `storage_ready` channel across the update.
      let mut candidate = self.clone();
      // Apply ONLY operator-supplied overrides — args whose value came from the command line or an
      // env var, not a clap default. A bare derived update treats every `default_value` arg as
      // present and would reset unset fields back to their clap defaults.
      macro_rules! take {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(v) = m.get_one::<$ty>($id) {
              candidate.$field = v.clone();
            }
          }
        };
      }
      take!("driver-max-inflight", max_inflight, usize);
      take!("driver-max-pending-bytes", max_pending_bytes, usize);
      take!("driver-events-cap", events_cap, usize);
      take!("driver-cmd-budget", cmd_budget, usize);
      take!("driver-recv-cap", recv_cap, usize);
      take!("driver-inbound-cap", inbound_cap, usize);
      take!("driver-accept-cap", accept_cap, usize);
      take!("driver-max-outbound-backlog", max_outbound_backlog, usize);
      take!("driver-max-conns", max_conns, usize);
      take!("driver-redial-base", redial_base, Duration);
      take!("driver-redial-cap", redial_cap, Duration);
      take!(
        "driver-max-failover-limbo-bytes",
        max_failover_limbo_bytes,
        usize
      );
      // Validate before committing, so a rejected update leaves `self` untouched (see above).
      candidate.validate().map_err(cfg_err)?;
      *self = candidate;
      Ok(())
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  // `DriverConfig` carries a non-`Eq` `flume::Receiver`, so the suites compare the value knobs
  // field-by-field and assert `storage_ready` separately. Asserting against `Default` keeps the
  // expectations pinned to the single-source-of-truth `DEFAULT_*` consts without restating them.
  #[cfg(any(feature = "serde", feature = "clap"))]
  fn assert_knobs_default(c: &DriverConfig) {
    let d = DriverConfig::default();
    assert_eq!(c.max_inflight, d.max_inflight);
    assert_eq!(c.max_pending_bytes, d.max_pending_bytes);
    assert_eq!(c.events_cap, d.events_cap);
    assert_eq!(c.cmd_budget, d.cmd_budget);
    assert_eq!(c.recv_cap, d.recv_cap);
    assert_eq!(c.inbound_cap, d.inbound_cap);
    assert_eq!(c.accept_cap, d.accept_cap);
    assert_eq!(c.max_outbound_backlog, d.max_outbound_backlog);
    assert_eq!(c.max_conns, d.max_conns);
    assert_eq!(c.redial_base, d.redial_base);
    assert_eq!(c.redial_cap, d.redial_cap);
    assert_eq!(c.max_failover_limbo_bytes, d.max_failover_limbo_bytes);
  }

  #[test]
  fn default_is_valid() {
    // The validating funnel must accept the defaults, or every flag-free parse would fail.
    assert!(DriverConfig::default().validate().is_ok());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_round_trip_preserves_every_knob() {
    let cfg = DriverConfig {
      max_inflight: 7,
      max_pending_bytes: 11,
      events_cap: 13,
      cmd_budget: 17,
      recv_cap: 19,
      inbound_cap: 23,
      accept_cap: 29,
      max_outbound_backlog: 31,
      max_conns: 37,
      redial_base: Duration::from_millis(250),
      redial_cap: Duration::from_secs(9),
      // A live channel on the source; it must NOT survive serialization.
      storage_ready: Some(flume::unbounded().1),
      max_failover_limbo_bytes: 41,
    };
    let json = serde_json::to_string(&cfg).unwrap();
    // The skipped channel is absent from the wire form.
    assert!(
      !json.contains("storage_ready"),
      "storage_ready must be skipped on serialize: {json}"
    );
    let back: DriverConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.max_inflight, 7);
    assert_eq!(back.max_pending_bytes, 11);
    assert_eq!(back.events_cap, 13);
    assert_eq!(back.cmd_budget, 17);
    assert_eq!(back.recv_cap, 19);
    assert_eq!(back.inbound_cap, 23);
    assert_eq!(back.accept_cap, 29);
    assert_eq!(back.max_outbound_backlog, 31);
    assert_eq!(back.max_conns, 37);
    assert_eq!(back.redial_base, Duration::from_millis(250));
    assert_eq!(back.redial_cap, Duration::from_secs(9));
    assert_eq!(back.max_failover_limbo_bytes, 41);
    // The runtime channel is reconstituted as `None`, never carried across the wire.
    assert!(back.storage_ready.is_none());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_humantime_renders_durations_as_strings() {
    let json = serde_json::to_string(&DriverConfig::default()).unwrap();
    assert!(json.contains("100ms"), "redial_base as humantime: {json}");
    assert!(json.contains("5s"), "redial_cap as humantime: {json}");
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_empty_object_yields_all_defaults() {
    let back: DriverConfig = serde_json::from_str("{}").unwrap();
    assert_knobs_default(&back);
    assert!(back.storage_ready.is_none());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_partial_config_defaults_the_rest() {
    // A single knob supplied; every other knob (and the skipped channel) takes its default.
    let back: DriverConfig = serde_json::from_str(r#"{ "max_conns": 256 }"#).unwrap();
    assert_eq!(back.max_conns, 256);
    let d = DriverConfig::default();
    assert_eq!(back.max_inflight, d.max_inflight);
    assert_eq!(back.redial_base, d.redial_base);
    assert_eq!(back.redial_cap, d.redial_cap);
    assert_eq!(back.max_failover_limbo_bytes, d.max_failover_limbo_bytes);
    assert!(back.storage_ready.is_none());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_unknown_field() {
    // `deny_unknown_fields` guards against a typo'd knob silently doing nothing.
    assert!(serde_json::from_str::<DriverConfig>(r#"{ "nope": 1 }"#).is_err());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_max_inflight_usize_max() {
    // `usize::MAX` would overflow the `max_inflight + 1` command-channel sizing: the validating
    // funnel rejects it at deserialize time.
    let json = format!(r#"{{ "max_inflight": {} }}"#, usize::MAX);
    assert!(serde_json::from_str::<DriverConfig>(&json).is_err());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_max_inflight_above_channel_ceiling() {
    // `usize::MAX / 2` clears the `usize::MAX` overflow check yet exceeds the submit-budget ceiling;
    // the funnel rejects it at parse time.
    // (Falsify: drop the `> MAX_CHANNEL_CAPACITY - 1` check in `validate` and this parse succeeds.)
    let json = format!(r#"{{ "max_inflight": {} }}"#, usize::MAX / 2);
    assert!(serde_json::from_str::<DriverConfig>(&json).is_err());
    // One below the ceiling is the largest accepted value.
    let json = format!(r#"{{ "max_inflight": {} }}"#, MAX_CHANNEL_CAPACITY - 1);
    assert!(serde_json::from_str::<DriverConfig>(&json).is_ok());
    let json = format!(r#"{{ "max_inflight": {} }}"#, MAX_CHANNEL_CAPACITY);
    assert!(serde_json::from_str::<DriverConfig>(&json).is_err());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_events_cap_above_ceiling() {
    // The lazily-growing `flume::bounded` events tail is bounded to the lazy channel ceiling; one
    // above it is rejected, the ceiling itself accepted. (Falsify: drop the `events_cap >
    // MAX_CHANNEL_CAPACITY` check and the over case parses.)
    let over = format!(r#"{{ "events_cap": {} }}"#, MAX_CHANNEL_CAPACITY + 1);
    assert!(
      serde_json::from_str::<DriverConfig>(&over).is_err(),
      "events_cap above the channel ceiling must be rejected"
    );
    let at = format!(r#"{{ "events_cap": {MAX_CHANNEL_CAPACITY} }}"#);
    assert!(
      serde_json::from_str::<DriverConfig>(&at).is_ok(),
      "events_cap == the channel ceiling must be accepted"
    );
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_eager_ring_caps_above_queue_ceiling() {
    // The three lochan eager-ring caps allocate all `cap` slots at bind, so they are bounded by the
    // far tighter `MAX_BOUNDED_QUEUE_DEPTH`: one above it is rejected (it would OOM at bind), the
    // ceiling itself accepted, and a `MAX_CHANNEL_CAPACITY`-scale value (fine for the lazy channels) is
    // now rejected for these. (Falsify: drop any `> MAX_BOUNDED_QUEUE_DEPTH` check and its over case
    // parses.)
    for field in ["recv_cap", "inbound_cap", "accept_cap"] {
      let over = format!(r#"{{ "{field}": {} }}"#, MAX_BOUNDED_QUEUE_DEPTH + 1);
      assert!(
        serde_json::from_str::<DriverConfig>(&over).is_err(),
        "{field} above the eager-ring ceiling must be rejected"
      );
      let at = format!(r#"{{ "{field}": {MAX_BOUNDED_QUEUE_DEPTH} }}"#);
      assert!(
        serde_json::from_str::<DriverConfig>(&at).is_ok(),
        "{field} == the eager-ring ceiling must be accepted"
      );
      // A value that the lazy channel ceiling would still permit is rejected for an eager ring.
      let lazy_scale = format!(r#"{{ "{field}": {MAX_CHANNEL_CAPACITY} }}"#);
      assert!(
        serde_json::from_str::<DriverConfig>(&lazy_scale).is_err(),
        "{field} at the lazy channel ceiling must be rejected by the tighter eager-ring ceiling"
      );
    }
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_zero_redial_base_and_cap() {
    // A zero redial backoff is a hot retry loop; both endpoints are rejected.
    assert!(serde_json::from_str::<DriverConfig>(r#"{ "redial_base": "0s" }"#).is_err());
    assert!(serde_json::from_str::<DriverConfig>(r#"{ "redial_cap": "0s" }"#).is_err());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_redial_base_above_cap() {
    // The ceiling must not sit below the floor.
    let json = r#"{ "redial_base": "10s", "redial_cap": "1s" }"#;
    assert!(serde_json::from_str::<DriverConfig>(json).is_err());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_rejects_redial_near_duration_max() {
    // A redial backoff near `Duration::MAX` overflows the doubling + jitter + `Instant` math and would
    // panic at the first redial; the funnel rejects both knobs past the `Instant`-safe bound at parse
    // time. (Falsify: drop the `> MAX_REDIAL_BACKOFF` checks and these near-MAX values parse.) The
    // humantime literals stay below `Duration::MAX` (parseable) but well above the ~49.7-day bound; the
    // crate-level boundary cases live in `programmatic_validate_rejects_each_sink_overflow`.
    // `redial_cap` alone past the bound.
    assert!(serde_json::from_str::<DriverConfig>(r#"{ "redial_cap": "100000000000s" }"#).is_err());
    // `redial_base` alone past the bound (paired over-bound cap so the `RedialBaseTooLarge` check is
    // exercised independently of the `base <= cap` one).
    let json = r#"{ "redial_base": "100000000000s", "redial_cap": "100000000000s" }"#;
    assert!(serde_json::from_str::<DriverConfig>(json).is_err());
    // A large-but-in-bound pair (4_233_600 s = 49 days < the ~49.7-day ceiling) still parses.
    let json = r#"{ "redial_base": "4233600s", "redial_cap": "4233600s" }"#;
    assert!(serde_json::from_str::<DriverConfig>(json).is_ok());
  }

  #[cfg(feature = "serde")]
  #[test]
  fn serde_valid_config_parses() {
    // A non-default but VALID config parses cleanly through the funnel.
    let json = r#"{ "max_inflight": 8, "redial_base": "1s", "redial_cap": "10s" }"#;
    let cfg: DriverConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.max_inflight, 8);
    assert_eq!(cfg.redial_base, Duration::from_secs(1));
    assert_eq!(cfg.redial_cap, Duration::from_secs(10));
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_no_flags_yields_all_defaults() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    let cli = Cli::try_parse_from(["prog"]).unwrap();
    assert_knobs_default(&cli.driver);
    // The skipped channel is not a flag; it defaults to `None`.
    assert!(cli.driver.storage_ready.is_none());
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_flags_override_knobs() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    let cli = Cli::try_parse_from([
      "prog",
      "--max-conns",
      "256",
      "--redial-base",
      "250ms",
      "--redial-cap",
      "9s",
    ])
    .unwrap();
    assert_eq!(cli.driver.max_conns, 256);
    assert_eq!(cli.driver.redial_base, Duration::from_millis(250));
    assert_eq!(cli.driver.redial_cap, Duration::from_secs(9));
    // Unsupplied knobs stay at their defaults.
    assert_eq!(cli.driver.max_inflight, DEFAULT_MAX_INFLIGHT);
    assert!(cli.driver.storage_ready.is_none());
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_rejects_max_inflight_usize_max() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    // `usize::MAX` would overflow the command-channel sizing: clap surfaces the validation failure.
    let arg = format!("{}", usize::MAX);
    assert!(Cli::try_parse_from(["prog", "--max-inflight", &arg]).is_err());
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_rejects_zero_redial_base_and_cap() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    assert!(Cli::try_parse_from(["prog", "--redial-base", "0s"]).is_err());
    assert!(Cli::try_parse_from(["prog", "--redial-cap", "0s"]).is_err());
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_rejects_redial_base_above_cap() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    assert!(Cli::try_parse_from(["prog", "--redial-base", "10s", "--redial-cap", "1s"]).is_err());
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_valid_flags_parse() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    // A non-default but VALID set of flags parses cleanly through the funnel.
    let cli = Cli::try_parse_from(["prog", "--max-inflight", "8", "--redial-base", "1s"]).unwrap();
    assert_eq!(cli.driver.max_inflight, 8);
    assert_eq!(cli.driver.redial_base, Duration::from_secs(1));
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_rejects_channel_caps_above_ceiling() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    // `max_inflight` that clears the `usize::MAX` overflow check yet exceeds the submit-budget ceiling.
    let half = format!("{}", usize::MAX / 2);
    assert!(Cli::try_parse_from(["prog", "--max-inflight", &half]).is_err());
    // The lazy `events_cap` above the lazy channel ceiling is rejected through clap's error path.
    let over_lazy = format!("{}", MAX_CHANNEL_CAPACITY + 1);
    assert!(
      Cli::try_parse_from(["prog", "--events-cap", &over_lazy]).is_err(),
      "--events-cap above the channel ceiling must be rejected"
    );
    // Each eager-ring cap above the tighter eager-ring ceiling is likewise rejected — including a
    // `MAX_CHANNEL_CAPACITY`-scale value that the lazy ceiling would have allowed.
    let over_eager = format!("{}", MAX_BOUNDED_QUEUE_DEPTH + 1);
    let lazy_scale = format!("{}", MAX_CHANNEL_CAPACITY);
    for flag in ["--recv-cap", "--inbound-cap", "--accept-cap"] {
      assert!(
        Cli::try_parse_from(["prog", flag, &over_eager]).is_err(),
        "{flag} above the eager-ring ceiling must be rejected"
      );
      assert!(
        Cli::try_parse_from(["prog", flag, &lazy_scale]).is_err(),
        "{flag} at the lazy channel ceiling must be rejected by the tighter eager-ring ceiling"
      );
    }
  }

  #[cfg(feature = "clap")]
  #[test]
  fn clap_rejects_redial_near_duration_max() {
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      driver: DriverConfig,
    }

    // ~3171 years, far above the ~49.7-day Instant-safe bound (the boundary case is programmatic).
    let huge = "100000000000s";
    assert!(Cli::try_parse_from(["prog", "--redial-cap", huge]).is_err());
    assert!(Cli::try_parse_from(["prog", "--redial-base", huge, "--redial-cap", huge]).is_err());
  }

  #[test]
  fn programmatic_validate_rejects_each_sink_overflow() {
    // The PROGRAMMATIC `validate` (the funnel the parse paths share) rejects every parsed-value sink
    // hazard, independent of serde/clap: an over-ceiling channel cap and an over-bound redial.
    let over_inflight = DriverConfig {
      max_inflight: usize::MAX / 2,
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_inflight.validate(),
      Err(DriverConfigError::MaxInflightAboveChannelCeiling)
    ));
    let over_events = DriverConfig {
      events_cap: MAX_CHANNEL_CAPACITY + 1,
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_events.validate(),
      Err(DriverConfigError::EventsCapAboveChannelCeiling)
    ));
    // The three eager-ring caps are bounded by the tighter `MAX_BOUNDED_QUEUE_DEPTH`: a value above it
    // (and, a fortiori, a `MAX_CHANNEL_CAPACITY`-scale value the lazy ceiling would permit) is rejected
    // with the per-cap eager-ring variant.
    let over_recv = DriverConfig {
      recv_cap: MAX_BOUNDED_QUEUE_DEPTH + 1,
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_recv.validate(),
      Err(DriverConfigError::RecvCapAboveQueueCeiling)
    ));
    let over_inbound = DriverConfig {
      inbound_cap: MAX_BOUNDED_QUEUE_DEPTH + 1,
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_inbound.validate(),
      Err(DriverConfigError::InboundCapAboveQueueCeiling)
    ));
    let over_accept = DriverConfig {
      accept_cap: MAX_BOUNDED_QUEUE_DEPTH + 1,
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_accept.validate(),
      Err(DriverConfigError::AcceptCapAboveQueueCeiling)
    ));
    // A `MAX_CHANNEL_CAPACITY`-scale value (fine for the lazy `events_cap`) is rejected for an eager
    // ring — this is the OOM-at-bind hazard the tighter ceiling closes.
    let lazy_scale_recv = DriverConfig {
      recv_cap: MAX_CHANNEL_CAPACITY,
      ..DriverConfig::default()
    };
    assert!(matches!(
      lazy_scale_recv.validate(),
      Err(DriverConfigError::RecvCapAboveQueueCeiling)
    ));
    let over_cap = DriverConfig {
      redial_cap: MAX_REDIAL_BACKOFF + Duration::from_secs(1),
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_cap.validate(),
      Err(DriverConfigError::RedialCapTooLarge)
    ));
    let over_base = DriverConfig {
      redial_base: MAX_REDIAL_BACKOFF + Duration::from_secs(1),
      redial_cap: MAX_REDIAL_BACKOFF + Duration::from_secs(2),
      ..DriverConfig::default()
    };
    assert!(matches!(
      over_base.validate(),
      Err(DriverConfigError::RedialBaseTooLarge)
    ));
    // The bounds themselves validate (boundary is inclusive for the caps, `- 1` for max_inflight).
    // The lazy `events_cap` sits at `MAX_CHANNEL_CAPACITY`; the three eager-ring caps at the tighter
    // `MAX_BOUNDED_QUEUE_DEPTH`.
    let at_bounds = DriverConfig {
      max_inflight: MAX_CHANNEL_CAPACITY - 1,
      events_cap: MAX_CHANNEL_CAPACITY,
      recv_cap: MAX_BOUNDED_QUEUE_DEPTH,
      inbound_cap: MAX_BOUNDED_QUEUE_DEPTH,
      accept_cap: MAX_BOUNDED_QUEUE_DEPTH,
      redial_base: MAX_REDIAL_BACKOFF,
      redial_cap: MAX_REDIAL_BACKOFF,
      ..DriverConfig::default()
    };
    assert!(at_bounds.validate().is_ok());
  }
}
