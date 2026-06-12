//! Driver tuning knobs. Every value here SIZES a bound documented in the crate's memory model;
//! none can remove one.

use std::time::Duration;

/// How many in-flight submits the budget admits by default.
pub(crate) const DEFAULT_MAX_INFLIGHT: usize = 1_024;
/// How many bytes of in-flight submit payload the budget admits by default.
pub(crate) const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
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

/// Tuning for a [`CompioQuicDriver`](crate::CompioQuicDriver) /
/// `CompioStreamDriver`. `Default` is sized for a small LAN cluster; every knob
/// adjusts the SIZE of a documented bound, never removes it.
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
  pub redial_base: Duration,
  /// Redial backoff ceiling.
  pub redial_cap: Duration,
  /// Wake signal for genuinely-ASYNC stores: the embedder clones a sender into its store and
  /// signals it on each I/O completion; the run loop drains it to empty each iteration and a
  /// signal wakes a sleeping loop so `handle_storage` runs promptly. Synchronous stores leave
  /// this `None` — `handle_storage` already runs every iteration.
  pub storage_ready: Option<flume::Receiver<()>>,
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
    }
  }
}
