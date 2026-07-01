//! Crypto-provider plumbing for the QUIC transport. Its rustls provider is chosen by the enabled
//! `quic-rustls-*` backend, independently of the byte-stream `tls-*` backend — they coincide only
//! when the caller enables matching features.
//!
//! [`QuicOptions`] holds the quinn-proto configs plus a tuned `TransportConfig` whose timer/window
//! values come from a [`QuicTuning`] (defaults shown):
//!
//! - `max_idle_timeout` = 3 000 ms — above the default 1 000 ms election timeout, the longest
//!   legitimate silence on a mesh edge while keep-alive is off (see below for why it is on).
//! - `keep_alive_interval` = idle/3. Raft's steady-state traffic is leader→follower only, so the
//!   follower↔follower mesh edges carry NOTHING between elections — yet a candidate needs every
//!   edge hot the moment an election starts (a cold redial costs a handshake mid-election).
//!   Keep-alive pings are what hold those zero-traffic edges under the idle timeout; idle/3 keeps
//!   two consecutive lost pings from idling a healthy edge out. quinn's default is no keep-alive.
//! - `initial_rtt` = 50 ms: the pre-sample loss-recovery estimate. The derived initial PTO
//!   (~150 ms) sits well under the election timeout, so a dropped handshake datagram retransmits
//!   long before a follower's election timer can fire over a not-yet-connected link.
//! - `max_concurrent_bidi_streams` = 4: each side opens ONE consensus stream; 4 leaves headroom
//!   for the mutual-dial doubling plus a stream reopen.
//! - `receive_window` (connection) = 16 MiB, `stream_receive_window` = 8 MiB. The single consensus
//!   stream carries frames up to [`MAX_FRAME_LEN`](crate::transport::frame::MAX_FRAME_LEN)
//!   (64 MiB, a snapshot install); a window smaller than the frame only THROTTLES it (credit
//!   regrants as the reader drains — it cannot deadlock), so the windows bound per-connection
//!   memory instead of admitting a whole snapshot in flight.
//!
//! Use [`ClusterTls`] to build a [`QuicOptions`] with mandatory mutual TLS over cluster-private
//! roots: the stock WebPki verifiers perform chain validation against the cluster CA, so a peer
//! without a valid cluster cert is rejected at the TLS handshake before any stream opens. The
//! security-relevant construction (roots, mTLS, TLS 1.3, ALPN) is pinned; only the timer/window
//! values are tunable via [`ClusterTls::tuning`].

use std::{sync::Arc, time::Duration, vec, vec::Vec};

use quinn_proto::{
  ClientConfig, EndpointConfig, IdleTimeout, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// The default rustls [`CryptoProvider`](rustls::crypto::CryptoProvider) the mTLS QUIC configs are
/// built from, selected by the enabled `quic-rustls-*` backend. The whole QUIC crypto stack follows
/// this one backend: quinn-proto's rustls TLS handshake AND the endpoint retry-token /
/// stateless-reset keys quinn derives from the same compiled provider (the workspace `quinn-proto`
/// dep links only the enabled backend, so `quic-aws-lc-rs` is genuinely aws-lc-rs-only). `ring` takes
/// precedence when both are present.
///
/// This single-backend guarantee holds within sailing's own feature graph (a CI cargo-tree check
/// enforces it). Cargo feature unification is global, though: a *different* crate in the dependency
/// tree enabling `quinn-proto/rustls-ring` pulls ring in alongside, and quinn then prefers ring for
/// the endpoint keys — so a strict single-backend / FIPS deployment must ensure nothing else enables
/// another quinn backend, or supply the endpoint keys explicitly via [`QuicOptions::new`].
///
/// A caller can override the TLS-handshake provider at runtime via [`ClusterTls::with_provider`], or
/// build every quinn config directly via [`QuicOptions::new`].
#[cfg(any(feature = "quic-rustls-ring", feature = "quic-rustls-aws-lc-rs"))]
fn active_provider() -> Arc<rustls::crypto::CryptoProvider> {
  #[cfg(feature = "quic-rustls-ring")]
  {
    Arc::new(rustls::crypto::ring::default_provider())
  }
  #[cfg(all(feature = "quic-rustls-aws-lc-rs", not(feature = "quic-rustls-ring")))]
  {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
  }
}

// `quic` links quinn-proto's core but NOT its rustls integration (the workspace dep no longer bakes
// a backend), so the QUIC transport cannot assemble any config without one. Require a backend.
#[cfg(all(
  feature = "quic",
  not(any(feature = "quic-rustls-ring", feature = "quic-rustls-aws-lc-rs"))
))]
compile_error!(
  "the `quic` feature needs a crypto backend: enable `quic-rustls-ring` or `quic-rustls-aws-lc-rs` \
   (or an alias: `quic-ring` / `quic-aws-lc-rs` / `quic-rustls`)"
);

/// The ALPN protocol every sailing QUIC connection negotiates. A non-sailing peer (or a
/// version-skewed one once this changes) fails the TLS handshake instead of mis-decoding frames.
const ALPN: &[u8] = b"sailing";

/// Default idle timeout. Must exceed the election timeout: between elections a follower↔follower
/// edge carries no consensus traffic at all (Raft is leader-centric), and even the leader→follower
/// edges go quiet for up to an election timeout during leadership changes. Keep-alive pings (below)
/// are what actually hold the zero-traffic edges; this is the backstop they refresh.
const IDLE_TIMEOUT_MILLIS: u64 = 3_000;

/// Keep-alive PING interval derived from the idle timeout: one third, so up to two consecutive
/// lost keep-alives still refresh the peer's idle timer in time. quinn's default is NO keep-alive,
/// under which a healthy-but-quiet connection idles out — and the follower↔follower mesh edges are
/// exactly that between elections, precisely the edges a candidate needs live when its election
/// timer fires. A zero result (an idle timeout too small to subdivide) leaves keep-alive off.
const fn keep_alive_interval_millis(idle_timeout_millis: u64) -> u64 {
  idle_timeout_millis / 3
}

/// Default initial RTT estimate for loss recovery BEFORE the first RTT sample. quinn derives the
/// initial Probe Timeout (the delay before a lost handshake packet first retransmits) from this;
/// its WAN-tuned default (333 ms → ~1 s PTO) would let one dropped Initial stall a dial right
/// through an election timeout. 50 ms (PTO ~150 ms) keeps handshake recovery well inside the
/// election timeout while staying ~50× a real datacenter RTT (no spurious early retransmits). A
/// geo-replicated cluster raises it via [`QuicTuning::with_initial_rtt_millis`] together with its
/// consensus timing.
const INITIAL_RTT_MILLIS: u64 = 50;

/// Pinned max concurrent bidi streams. Each side opens ONE consensus stream per connection; 4
/// covers the mutual-dial doubling plus a reopen without admitting unbounded peer-minted streams.
pub(crate) const MAX_BIDI_STREAMS: u32 = 4;

/// Connection-level receive window. Deliberately BELOW the 64 MiB max frame: a snapshot install is
/// throttled through the window (credit regrants as the reader drains; flow control cannot
/// deadlock), bounding per-connection buffered memory instead of admitting a whole snapshot.
const CONNECTION_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;

/// Per-stream receive window, bounded below the connection window so the single consensus stream
/// cannot pin the entire connection window at once.
const STREAM_RECEIVE_WINDOW: u64 = 8 * 1024 * 1024;

/// The largest value a QUIC `VarInt` can carry (`2^62 - 1`). The byte-WINDOW tuning setters clamp to
/// this so an embedder-supplied window can never make the `VarInt` conversions in
/// [`QuicOptions::build_transport`] fail at construction time.
const MAX_VARINT_U64: u64 = (1 << 62) - 1;

/// Upper bound for the TIMER tuning knobs (idle timeout, initial RTT, keep-alive), chosen so the
/// resolved `Duration` is `std::time::Instant`-safe. The byte-window knobs are NOT durations and
/// stay at the `VarInt` clamp; only the timers reach an `Instant`.
///
/// quinn turns each timer into a `Duration` it later ADDS to a `std::time::Instant` (the keep-alive,
/// idle, and PTO deadlines), and `Instant + Duration` PANICS on overflow. `VarInt`'s `2^62-1`-ms
/// ceiling (~1.5×10^17 ms ≈ 1.5×10^23 ns) is FAR past the `u64`-nanosecond `Instant` overflow
/// threshold (~584 years ≈ 1.8×10^19 ns), so a parsed `u64::MAX` that clears the `VarInt` clamp
/// would still panic quinn at runtime — an operator config typo taking the node down after a clean
/// startup. `u32::MAX` ms (~49.7 days ≈ 4.29×10^15 ns) is the bound here: comfortably below the
/// `Instant` overflow threshold (so `Instant + Duration` of this bound cannot overflow), yet far
/// above any realistic QUIC idle / keep-alive / RTT for any deployment.
const MAX_TIMER_MILLIS: u64 = u32::MAX as u64;

// The timer bound is strictly tighter than the `VarInt` window ceiling, so an idle timeout clamped
// to it always converts to a quinn `IdleTimeout` (a `VarInt`) AND is `Instant`-safe.
const _: () = assert!(MAX_TIMER_MILLIS < MAX_VARINT_U64);

/// Clamp an embedder-supplied WINDOW value into `1..=MAX_VARINT_U64`: never zero (a zero window is a
/// wedge, not a tuning), never past the QUIC `VarInt` range.
const fn clamp_window(v: u64) -> u64 {
  if v == 0 {
    1
  } else if v > MAX_VARINT_U64 {
    MAX_VARINT_U64
  } else {
    v
  }
}

/// Clamp an embedder-supplied TIMER value into `1..=MAX_TIMER_MILLIS`: never zero (a zero timeout is
/// a wedge, not a tuning), never past the `Instant`-safe timer bound (see `MAX_TIMER_MILLIS` for
/// why the bound is well below the `VarInt` ceiling).
const fn clamp_timer(v: u64) -> u64 {
  if v == 0 {
    1
  } else if v > MAX_TIMER_MILLIS {
    MAX_TIMER_MILLIS
  } else {
    v
  }
}

/// Clamp an embedder-supplied keep-alive override into `0..=MAX_TIMER_MILLIS`: `0` keeps its
/// "keep-alive off" meaning, any positive value is clamped to the `Instant`-safe timer bound. Unlike
/// the other timers this admits `0` (it is the documented disable, not a wedge).
const fn clamp_keep_alive(v: u64) -> u64 {
  if v > MAX_TIMER_MILLIS {
    MAX_TIMER_MILLIS
  } else {
    v
  }
}

// `serde(default = "…")` needs a function PATH (the pinned consts above are private, so they cannot
// be named in an `#[arg(default_value_t = …)]` from another module either — but they ARE in scope
// here, which is all the clap mirror needs). Each value knob's serde default is wrapped to return
// its single-source-of-truth const; the `Option<u64>` keep-alive uses bare `serde(default)` (`None`)
// and has no clap default. Gated on `serde` so the default build stays warning-free.
#[cfg(feature = "serde")]
const fn default_idle_timeout_millis() -> u64 {
  IDLE_TIMEOUT_MILLIS
}
#[cfg(feature = "serde")]
const fn default_initial_rtt_millis() -> u64 {
  INITIAL_RTT_MILLIS
}
#[cfg(feature = "serde")]
const fn default_connection_receive_window() -> u64 {
  CONNECTION_RECEIVE_WINDOW
}
#[cfg(feature = "serde")]
const fn default_stream_receive_window() -> u64 {
  STREAM_RECEIVE_WINDOW
}

/// Embedder-tunable timer and flow-control values for the QUIC `TransportConfig`, with `Default` =
/// the pinned LAN-tuned constants (see each constant for the rationale). A geo-replicated cluster —
/// where the defaults' assumptions (sub-50 ms RTT, election-timeout headroom) do not hold —
/// overrides them via [`ClusterTls::tuning`].
///
/// **Scope (the security posture).** This carries ONLY performance knobs — timers and window
/// sizes. The security-relevant construction (cluster-private roots, mandatory mTLS, TLS 1.3,
/// ALPN) lives exclusively inside [`ClusterTls::build`] and no tuning value can reach it.
///
/// Setters clamp every value so no embedder input can wedge or crash the transport. The byte WINDOW
/// knobs clamp to `1..=2^62-1` (the QUIC `VarInt` range), so the `VarInt` conversions at
/// construction cannot fail. The TIMER knobs (idle timeout, initial RTT, keep-alive) clamp instead
/// to the tighter `Instant`-safe `MAX_TIMER_MILLIS` bound: quinn adds each as a `Duration` to a
/// `std::time::Instant`, and a value merely within `VarInt` range (e.g. a parsed `u64::MAX`) would
/// overflow that addition and PANIC quinn at runtime. A zero timeout/window clamps up to the `1`-ms
/// minimum (an explicit `0` keep-alive keeps its "off" meaning).
///
/// `serde`/`clap` (optional) parse this through the SAME clamping setters: `Deserialize` and the
/// CLI/env path both route a raw value map through a private mirror and into
/// [`QuicTuning::new`]`.with_*(…)`, so an out-of-range deserialized/parsed value is CLAMPED exactly
/// as a programmatic builder would — never accepted raw. `Serialize` emits the RESOLVED fields
/// (`keep_alive_interval_millis` is the override `Option`, not the derived idle/3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(from = "QuicTuningParts"))]
pub struct QuicTuning {
  /// `max_idle_timeout`, milliseconds. Default [`IDLE_TIMEOUT_MILLIS`].
  idle_timeout_millis: u64,
  /// `keep_alive_interval`, milliseconds. `None` (the default) derives idle/3 — the two-lost-pings
  /// margin [`keep_alive_interval_millis`] documents; an explicit `Some(0)` disables keep-alive.
  keep_alive_interval_millis: Option<u64>,
  /// `initial_rtt`, milliseconds. Default [`INITIAL_RTT_MILLIS`].
  initial_rtt_millis: u64,
  /// Connection-level `receive_window`, bytes. Default [`CONNECTION_RECEIVE_WINDOW`].
  connection_receive_window: u64,
  /// Per-stream `stream_receive_window`, bytes. Default [`STREAM_RECEIVE_WINDOW`].
  stream_receive_window: u64,
}

impl QuicTuning {
  /// The default tuning — exactly the constants the transport pins without an override.
  #[must_use]
  pub const fn new() -> Self {
    Self {
      idle_timeout_millis: IDLE_TIMEOUT_MILLIS,
      keep_alive_interval_millis: None,
      initial_rtt_millis: INITIAL_RTT_MILLIS,
      connection_receive_window: CONNECTION_RECEIVE_WINDOW,
      stream_receive_window: STREAM_RECEIVE_WINDOW,
    }
  }

  /// Idle timeout in milliseconds (see `IDLE_TIMEOUT_MILLIS` for the consensus coupling).
  #[inline(always)]
  pub const fn idle_timeout_millis(&self) -> u64 {
    self.idle_timeout_millis
  }

  /// The RESOLVED keep-alive interval in milliseconds: the explicit override when one was set
  /// (`0` = keep-alive off), otherwise idle/3 — derived from the CURRENT idle timeout, so raising
  /// the idle timeout scales the keep-alive with it.
  #[inline(always)]
  pub const fn keep_alive_interval_millis(&self) -> u64 {
    match self.keep_alive_interval_millis {
      Some(ms) => ms,
      None => keep_alive_interval_millis(self.idle_timeout_millis),
    }
  }

  /// Initial RTT estimate in milliseconds (see `INITIAL_RTT_MILLIS` for why the default is
  /// LAN-tuned and what a geo-replicated cluster must raise it to).
  #[inline(always)]
  pub const fn initial_rtt_millis(&self) -> u64 {
    self.initial_rtt_millis
  }

  /// Connection-level receive window in bytes (see `CONNECTION_RECEIVE_WINDOW`).
  #[inline(always)]
  pub const fn connection_receive_window(&self) -> u64 {
    self.connection_receive_window
  }

  /// Per-stream receive window in bytes (see `STREAM_RECEIVE_WINDOW`).
  #[inline(always)]
  pub const fn stream_receive_window(&self) -> u64 {
    self.stream_receive_window
  }

  /// Override the idle timeout (milliseconds; clamped to `1..=MAX_TIMER_MILLIS`, the
  /// `Instant`-safe timer bound). Must stay ABOVE the election timeout, or healthy links idle out
  /// between elections; with the default (derived) keep-alive the idle/3 ping interval scales
  /// along with it.
  #[must_use]
  pub const fn with_idle_timeout_millis(mut self, millis: u64) -> Self {
    self.idle_timeout_millis = clamp_timer(millis);
    self
  }

  /// Override the keep-alive interval (milliseconds; `0` disables keep-alive, any positive value is
  /// clamped to `MAX_TIMER_MILLIS`, the `Instant`-safe timer bound). Without an override the
  /// interval is derived as idle/3. Disabling keep-alive on a production mesh is a liveness hazard:
  /// the zero-traffic follower↔follower edges then idle out between elections (see
  /// `IDLE_TIMEOUT_MILLIS`).
  #[must_use]
  pub const fn with_keep_alive_interval_millis(mut self, millis: u64) -> Self {
    self.keep_alive_interval_millis = Some(clamp_keep_alive(millis));
    self
  }

  /// Override the initial RTT estimate (milliseconds; clamped to `1..=MAX_TIMER_MILLIS`, the
  /// `Instant`-safe timer bound). Keep it at or above the real inter-node RTT: an estimate far
  /// below it provokes spurious handshake retransmits, while the election timeout must in turn stay
  /// above the resulting ~3×RTT initial probe timeout.
  #[must_use]
  pub const fn with_initial_rtt_millis(mut self, millis: u64) -> Self {
    self.initial_rtt_millis = clamp_timer(millis);
    self
  }

  /// Override the connection-level receive window (bytes; clamped to `1..=2^62-1`, the QUIC
  /// `VarInt` range). A window below the 64 MiB max frame only throttles a snapshot install (credit
  /// regrants as the reader drains); it cannot deadlock it.
  #[must_use]
  pub const fn with_connection_receive_window(mut self, bytes: u64) -> Self {
    self.connection_receive_window = clamp_window(bytes);
    self
  }

  /// Override the per-stream receive window (bytes; clamped to `1..=2^62-1`, the QUIC `VarInt`
  /// range). Keep it at or below the connection window.
  #[must_use]
  pub const fn with_stream_receive_window(mut self, bytes: u64) -> Self {
    self.stream_receive_window = clamp_window(bytes);
    self
  }
}

impl Default for QuicTuning {
  fn default() -> Self {
    Self::new()
  }
}

/// Raw parse mirror for [`QuicTuning`]. NOT part of the public API — it carries the serde per-knob
/// deserialize defaults and the clap `#[arg(...)]` attributes, and is ALWAYS converted to a
/// [`QuicTuning`] via the clamping [`From`] (see below). Because `QuicTuning` is non-generic, ONE
/// shared mirror serves both serde and clap (no `FromStr` split — that was only forced by `Config`'s
/// generic `I`). The fields are the raw wire values; clamping happens in the conversion, so the
/// mirror itself holds whatever was supplied.
#[cfg(any(feature = "serde", feature = "clap"))]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[cfg_attr(feature = "clap", derive(clap::Args))]
struct QuicTuningParts {
  #[cfg_attr(feature = "serde", serde(default = "default_idle_timeout_millis"))]
  #[cfg_attr(
    feature = "clap",
    arg(
      id = "quic-idle-timeout-millis",
      long = "quic-idle-timeout-millis",
      env = "SAILING_QUIC_IDLE_TIMEOUT_MILLIS",
      default_value_t = IDLE_TIMEOUT_MILLIS
    )
  )]
  idle_timeout_millis: u64,
  // `None` (the default) derives idle/3; an explicit value (incl. `0` = off) overrides it. Bare
  // `serde(default)` → `None`; no clap default, so an unset flag leaves the derivation in place.
  #[cfg_attr(feature = "serde", serde(default))]
  #[cfg_attr(
    feature = "clap",
    arg(
      id = "quic-keep-alive-interval-millis",
      long = "quic-keep-alive-interval-millis",
      env = "SAILING_QUIC_KEEP_ALIVE_INTERVAL_MILLIS"
    )
  )]
  keep_alive_interval_millis: Option<u64>,
  #[cfg_attr(feature = "serde", serde(default = "default_initial_rtt_millis"))]
  #[cfg_attr(
    feature = "clap",
    arg(
      id = "quic-initial-rtt-millis",
      long = "quic-initial-rtt-millis",
      env = "SAILING_QUIC_INITIAL_RTT_MILLIS",
      default_value_t = INITIAL_RTT_MILLIS
    )
  )]
  initial_rtt_millis: u64,
  #[cfg_attr(
    feature = "serde",
    serde(default = "default_connection_receive_window")
  )]
  #[cfg_attr(
    feature = "clap",
    arg(
      id = "quic-connection-receive-window",
      long = "quic-connection-receive-window",
      env = "SAILING_QUIC_CONNECTION_RECEIVE_WINDOW",
      default_value_t = CONNECTION_RECEIVE_WINDOW
    )
  )]
  connection_receive_window: u64,
  #[cfg_attr(feature = "serde", serde(default = "default_stream_receive_window"))]
  #[cfg_attr(
    feature = "clap",
    arg(
      id = "quic-stream-receive-window",
      long = "quic-stream-receive-window",
      env = "SAILING_QUIC_STREAM_RECEIVE_WINDOW",
      default_value_t = STREAM_RECEIVE_WINDOW
    )
  )]
  stream_receive_window: u64,
}

// The CLAMPING conversion both parse paths funnel through: rebuild from `QuicTuning::new()` and run
// every raw value back through the `with_*` setters, so a deserialized/parsed value is clamped
// EXACTLY as a programmatic builder would — windows to the `VarInt` range, timers to the
// `Instant`-safe `MAX_TIMER_MILLIS` bound. This is what makes the conversion INFALLIBLE — the
// setters never reject — so `QuicTuning` needs only `From`, not a validating `TryFrom`.
#[cfg(any(feature = "serde", feature = "clap"))]
impl From<QuicTuningParts> for QuicTuning {
  fn from(p: QuicTuningParts) -> Self {
    let mut t = Self::new()
      .with_idle_timeout_millis(p.idle_timeout_millis)
      .with_initial_rtt_millis(p.initial_rtt_millis)
      .with_connection_receive_window(p.connection_receive_window)
      .with_stream_receive_window(p.stream_receive_window);
    // The keep-alive is an `Option` override: a present value (incl. `0` = off) replaces the idle/3
    // derivation, an absent one leaves it. The setter clamps a positive override to the timer bound.
    if let Some(ms) = p.keep_alive_interval_millis {
      t = t.with_keep_alive_interval_millis(ms);
    }
    t
  }
}

// Apply a clap UPDATE to a [`QuicTuning`] preserving every un-flagged field. Seed a parts mirror
// from the CURRENT resolved fields and overwrite a field ONLY when its arg's value came from the
// command line or an env var — NOT from a clap default. A bare derived `QuicTuningParts` update
// treats every `default_value_t` arg as present and would reset the un-flagged knobs back to their
// pinned defaults (silently shrinking a WAN/large-window tuning on a partial reload); the
// `value_source` gate is exactly what stops that. The seeded parts then go through the clamping
// `From` so an operator-supplied out-of-range value is clamped just as a programmatic builder would.
// Shared with [`QuicConfigOptions`]'s hand-written update, which flattens this type.
#[cfg(feature = "clap")]
pub(super) fn update_quic_tuning(tuning: &mut QuicTuning, m: &clap::ArgMatches) {
  use clap::parser::ValueSource;

  let mut parts = QuicTuningParts {
    idle_timeout_millis: tuning.idle_timeout_millis,
    keep_alive_interval_millis: tuning.keep_alive_interval_millis,
    initial_rtt_millis: tuning.initial_rtt_millis,
    connection_receive_window: tuning.connection_receive_window,
    stream_receive_window: tuning.stream_receive_window,
  };
  macro_rules! take {
    ($id:literal, $field:ident, $ty:ty) => {
      if matches!(
        m.value_source($id),
        Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
      ) {
        if let Some(v) = m.get_one::<$ty>($id) {
          parts.$field = v.clone();
        }
      }
    };
  }
  take!("quic-idle-timeout-millis", idle_timeout_millis, u64);
  take!("quic-initial-rtt-millis", initial_rtt_millis, u64);
  take!(
    "quic-connection-receive-window",
    connection_receive_window,
    u64
  );
  take!("quic-stream-receive-window", stream_receive_window, u64);
  // `keep_alive_interval_millis` is an `Option<u64>` override (a present value, incl. `0`, replaces
  // the idle/3 derivation; absent leaves it). An operator-supplied flag sets the `Some`; otherwise
  // the seeded value (the current override state) is preserved.
  if matches!(
    m.value_source("quic-keep-alive-interval-millis"),
    Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
  ) {
    parts.keep_alive_interval_millis = m.get_one::<u64>("quic-keep-alive-interval-millis").copied();
  }
  *tuning = parts.into();
}

#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches};

  // `clap::Args`/`FromArgMatches` delegate to the `QuicTuningParts` mirror, then run the parsed
  // parts through the clamping `From` — mirroring `Config`'s mirror delegation. There is no
  // validating step (the clamp is infallible), so unlike `Config` the conversion cannot error.
  impl Args for QuicTuning {
    fn augment_args(cmd: Command) -> Command {
      QuicTuningParts::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      QuicTuningParts::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for QuicTuning {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      QuicTuningParts::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      update_quic_tuning(self, m);
      Ok(())
    }
  }
};

/// Default cap on the number of LIVE connections the bridge holds at once (dialed + accepted). The
/// network is untrusted: an inbound flood of foreign-CA / no-cert Initials would otherwise each
/// allocate a `Connection` before identity validation could reject it. At the cap the bridge
/// statelessly refuses further inbound attempts instead of allocating. The coordinator RAISES the
/// effective cap to [`mesh_connection_floor`] of the tracked peer count at construction, so a
/// default-cap node on a large cluster still admits its whole steady-state mesh.
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// A small constant floor on the connection cap, so even a 1- or 2-node cluster (whose mutual-dial
/// mesh is tiny) keeps a little accept/reconnect headroom.
const MIN_CONNECTION_FLOOR: usize = 4;

/// The minimum live-connection cap that admits a node's full steady-state mutual-dial mesh over
/// `peers` TRACKED peers (every id the consensus endpoint replicates to, EXCLUDING this node —
/// voters in both joint halves, learners, and incoming learners alike: the transport meshes with
/// all of them), plus reconnect headroom: `max(MIN_CONNECTION_FLOOR, 3 * peers)`.
///
/// The mutual-dial design keeps TWO physical connections per peer pair (each side dials the other
/// and both are kept; see the bridge's bind policy), so a node holds `2*peers` steady-state
/// connections; a reconnecting peer can briefly hold a THIRD (the new dial/accept overlapping the
/// old one), so one reconnect slot per peer is added. The coordinator raises `max_connections` to
/// this when the configured cap is lower, so the cap can never refuse a legitimate steady-state
/// mesh connection; it still bounds an untrusted-network flood.
pub(crate) const fn mesh_connection_floor(peers: usize) -> usize {
  let mesh_with_reconnect = peers * 3;
  if mesh_with_reconnect > MIN_CONNECTION_FLOOR {
    mesh_with_reconnect
  } else {
    MIN_CONNECTION_FLOOR
  }
}

/// Immutable QUIC config bundle handed to the coordinator. Accessor-only; all fields are private
/// and cannot be mutated after construction (the security-relevant parts are pinned by
/// [`ClusterTls::build`]).
pub struct QuicOptions {
  endpoint: Arc<EndpointConfig>,
  client: Option<ClientConfig>,
  server: Option<Arc<ServerConfig>>,
  /// Set to `true` by [`ClusterTls::build`]; `false` for the accept-any test path.
  requires_client_auth: bool,
  /// Cap on the number of live connections the bridge holds at once. Inbound attempts past this
  /// are refused (stateless close) instead of allocating, bounding an accept flood.
  max_connections: usize,
}

impl QuicOptions {
  /// Build from **fully caller-supplied** quinn configs plus a tuning — the config-level escape hatch
  /// (mirroring memberlist). A `quic-rustls-*` backend feature is still required (it links quinn's
  /// rustls integration and sets quinn's default endpoint-key crypto); on top of it the caller
  /// assembles every config, so the handshake provider — ring / aws-lc-rs / a FIPS module / a custom
  /// provider — and, by building the keys below, the endpoint crypto too, are the caller's. The
  /// caller assembles the `EndpointConfig` (its reset
  /// key sets stateless-reset crypto) and the quinn `ClientConfig`/`ServerConfig`
  /// (`rustls::*::builder_with_provider(..)` + `Quic{Client,Server}Config::try_from`). For full
  /// backend / FIPS control the server's retry/validation-token key is separate: it comes from
  /// `ServerConfig` (whose `with_crypto` derives it from the compiled backend), so override
  /// `ServerConfig::token_key` too, not just the `EndpointConfig`.
  ///
  /// This leaves `requires_client_auth = false`, so the cluster-CA mTLS enforcement is the caller's
  /// responsibility on this path; use [`ClusterTls`] (optionally with [`ClusterTls::with_provider`])
  /// for the batteries-included mTLS build. The tuned `TransportConfig` (idle timeout + keep-alive +
  /// stream caps + flow-control windows) is constructed internally and installed on both configs.
  pub fn new(
    endpoint: EndpointConfig,
    client: Option<ClientConfig>,
    server: Option<ServerConfig>,
    tuning: QuicTuning,
  ) -> Self {
    Self::new_inner(endpoint, client, server, tuning, false)
  }

  fn new_inner(
    endpoint: EndpointConfig,
    client: Option<ClientConfig>,
    server: Option<ServerConfig>,
    tuning: QuicTuning,
    requires_client_auth: bool,
  ) -> Self {
    let transport = Self::build_transport(&tuning);
    let server = server.map(|mut s| {
      s.transport_config(transport.clone());
      Arc::new(s)
    });
    let mut client = client;
    if let Some(ref mut c) = client {
      c.transport_config(transport);
    }
    Self {
      endpoint: Arc::new(endpoint),
      client,
      server,
      requires_client_auth,
      max_connections: DEFAULT_MAX_CONNECTIONS,
    }
  }

  /// Cheap clone of the endpoint config arc.
  #[inline(always)]
  pub fn endpoint_config(&self) -> Arc<EndpointConfig> {
    self.endpoint.clone()
  }

  /// Cheap clone of the client config used for outbound dials, if any.
  #[inline(always)]
  pub fn client_config(&self) -> Option<ClientConfig> {
    self.client.clone()
  }

  /// Cheap clone of the server config arc, if any.
  #[inline(always)]
  pub fn server_config(&self) -> Option<Arc<ServerConfig>> {
    self.server.clone()
  }

  /// Whether the server config was built with mandatory client-certificate authentication. `true`
  /// only when constructed via [`ClusterTls::build`]; the provided identity scheme requires it.
  #[inline(always)]
  pub const fn requires_client_auth(&self) -> bool {
    self.requires_client_auth
  }

  /// The cap on live connections (dialed + accepted). The bridge refuses inbound attempts once the
  /// table holds this many, bounding an untrusted-network accept flood.
  #[inline(always)]
  pub const fn max_connections(&self) -> usize {
    self.max_connections
  }

  /// Override the live-connection cap (see [`Self::max_connections`]). The coordinator RAISES the
  /// effective cap to the membership-sized mesh floor — `max(4, 3 * peers)` over every TRACKED
  /// peer (voters in both joint halves, learners, and incoming learners, minus the node itself) —
  /// whenever the value set here is lower, and recomputes that floor as committed configuration
  /// changes apply, so the cap can never refuse a legitimate mesh connection even as the
  /// membership grows. A value of 0 is clamped to 1 so at least one connection is always
  /// admissible.
  #[must_use]
  pub const fn with_max_connections(mut self, max: usize) -> Self {
    self.max_connections = if max == 0 { 1 } else { max };
    self
  }

  /// Build the tuned `TransportConfig` shared between server and client. The timer/window values
  /// come from `tuning`; the stream caps and the closed protocol surfaces below are pinned.
  fn build_transport(tuning: &QuicTuning) -> Arc<TransportConfig> {
    let mut tc = TransportConfig::default();
    let idle = IdleTimeout::try_from(Duration::from_millis(tuning.idle_timeout_millis()))
      .expect("idle timeout within the Instant-safe timer bound (clamped by the tuning setter)");
    tc.max_idle_timeout(Some(idle));
    // Keep-alive pings hold the zero-traffic follower↔follower mesh edges under the idle timeout
    // (see `keep_alive_interval_millis`); a resolved 0 means keep-alive off.
    let keep_alive = tuning.keep_alive_interval_millis();
    if keep_alive > 0 {
      tc.keep_alive_interval(Some(Duration::from_millis(keep_alive)));
    }
    tc.initial_rtt(Duration::from_millis(tuning.initial_rtt_millis()));
    tc.max_concurrent_bidi_streams(VarInt::from_u32(MAX_BIDI_STREAMS));
    // Close the protocol surfaces this transport does NOT use, so a buggy/version-skewed but
    // fully validated peer cannot pin connection-level receive credit or memory on them:
    // - Incoming UNIDIRECTIONAL streams: limit 0 — consensus rides ONE framed bidi stream; a peer
    //   cannot mint a uni stream against a 0 limit, and forcing one is a protocol violation quinn
    //   turns into a connection close.
    // - DATAGRAM receive: `None` stops advertising `max_datagram_size`, so an unsolicited QUIC
    //   DATAGRAM frame is a protocol violation rather than buffered bytes.
    tc.max_concurrent_uni_streams(VarInt::from_u32(0));
    tc.datagram_receive_buffer_size(None);
    tc.receive_window(
      VarInt::from_u64(tuning.connection_receive_window())
        .expect("connection window within VarInt range (clamped by the tuning setter)"),
    );
    tc.stream_receive_window(
      VarInt::from_u64(tuning.stream_receive_window())
        .expect("stream window within VarInt range (clamped by the tuning setter)"),
    );
    Arc::new(tc)
  }
}

/// Builds a [`QuicOptions`] bundle with mandatory mutual TLS over a cluster-private root CA.
///
/// Both directions are fully authenticated:
///
/// - **Server side** uses [`rustls::server::WebPkiClientVerifier`] rooted at the cluster CA, which
///   makes client certificates mandatory by default. A peer without a cert (or whose cert does not
///   chain to the cluster CA) is rejected at the TLS handshake before any QUIC stream opens.
/// - **Client side** uses [`rustls::client::WebPkiServerVerifier`] with the same cluster CA, and
///   presents this node's cert chain for mutual authentication.
///
/// Both configs are TLS 1.3-only with ALPN `b"sailing"`.
///
/// ## SNI server name
///
/// The stock `WebPkiServerVerifier` validates the SNI `server_name` the dialer supplies against
/// the server cert's Subject Alternative Names. Mint each node's cert with a DNS SAN of the form
/// `node-<id-hex>.<cluster-hex>.sailing` (the coordinator derives that name on `connect` from the
/// dialed peer's `Data` encoding and the cluster id), so the verifier can match it. DNS bounds
/// one label to 63 octets, so this SAN form supports id encodings up to 29 bytes; larger ids
/// dial with an explicit name (`connect_with_server_name`) and certs minted to match.
pub struct ClusterTls {
  roots: rustls::RootCertStore,
  chain: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
  tuning: QuicTuning,
  provider: Option<Arc<rustls::crypto::CryptoProvider>>,
}

impl ClusterTls {
  /// Create a new `ClusterTls` builder.
  ///
  /// - `roots` — the cluster-private CA(s); only peers whose cert chains to one of these roots
  ///   will complete the handshake.
  /// - `chain` — this node's certificate chain (leaf first).
  /// - `key` — the private key for the leaf certificate.
  pub fn new(
    roots: rustls::RootCertStore,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
  ) -> Self {
    Self {
      roots,
      chain,
      key,
      tuning: QuicTuning::new(),
      provider: None,
    }
  }

  /// Override the rustls [`CryptoProvider`](rustls::crypto::CryptoProvider) used to assemble the mTLS
  /// **TLS-handshake** configs, letting the caller pick the backend at runtime (ring / aws-lc-rs / a
  /// FIPS module / a custom provider) instead of the compile-time `quic-rustls-*` default.
  ///
  /// This overrides the TLS-handshake provider ONLY; the QUIC endpoint retry-token / stateless-reset
  /// keys always follow the compile-time `quic-rustls-*` backend, and nothing checks that the two
  /// agree. Whenever the provider you pass differs from that backend — a custom or FIPS provider in
  /// any build, or a mismatched provider in a multi-backend build — the result is a SPLIT config
  /// (handshake from your provider, endpoint keys from the backend), not an error. To keep one
  /// backend end to end, pass a provider matching the compiled `quic-rustls-*` (or rely on the
  /// default); for a runtime backend that also drives the endpoint keys, build every quinn config
  /// directly via [`QuicOptions::new`].
  ///
  /// The provider must build a usable rustls TLS 1.3 config (else [`Self::try_build`] fails with
  /// [`ClusterTlsError::Rustls`]) AND expose the QUIC initial cipher suite `TLS13_AES_128_GCM_SHA256`
  /// (else [`ClusterTlsError::Quic`]). Left unset, the build uses the feature-selected backend default
  /// (`quic` requires one).
  #[must_use]
  pub fn with_provider(mut self, provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
    self.provider = Some(provider);
    self
  }

  /// Override the transport timer/window tuning for the built [`QuicOptions`]. The default is
  /// [`QuicTuning::new`] (the LAN-tuned constants). Tuning carries ONLY performance knobs — the
  /// mandatory-mTLS construction this builder performs is unaffected by any tuning value.
  #[must_use]
  pub fn tuning(mut self, tuning: QuicTuning) -> Self {
    self.tuning = tuning;
    self
  }

  /// Consume the builder and produce a [`QuicOptions`] with both a server config (mandatory client
  /// auth) and a client config (mTLS).
  ///
  /// # Panics
  ///
  /// Panics if the supplied roots/chain/key are not a valid cluster-CA bundle (an empty root store,
  /// a key that does not match the leaf, or an unusable crypto provider — an invalid TLS 1.3 config,
  /// or one missing the `TLS13_AES_128_GCM_SHA256` QUIC initial suite). This
  /// is the back-compat panic-on-misconfig surface; [`Self::try_build`] returns those same
  /// failures as a recoverable [`ClusterTlsError`] instead (preferred for any path that parses the
  /// bundle from operator-supplied files, where a mismatched cert/key is an ordinary
  /// cert-rotation mistake, not a programming error).
  pub fn build(self) -> QuicOptions {
    self
      .try_build()
      .expect("ClusterTls::build: invalid cluster CA bundle")
  }

  /// Consume the builder and produce a [`QuicOptions`] with both a server config (mandatory client
  /// auth) and a client config (mTLS), returning a typed [`ClusterTlsError`] instead of panicking
  /// when the bundle is invalid.
  ///
  /// Every fallible assembly step is mapped to a recoverable error: building the cluster-CA
  /// certificate verifiers ([`ClusterTlsError::Verifier`]), the rustls config assembly — selecting
  /// TLS 1.3 and accepting the leaf cert/key pair, where a **mismatched cert and key** surfaces
  /// ([`ClusterTlsError::Rustls`]), and the quinn TLS-config conversion, which fails when the provider
  /// lacks the QUIC initial cipher suite `TLS13_AES_128_GCM_SHA256` ([`ClusterTlsError::Quic`]). A
  /// mismatched cert/key, an invalid leaf, or an unusable provider are ordinary cert-rotation /
  /// configuration mistakes, so they are recoverable here rather than a process panic.
  pub fn try_build(self) -> Result<QuicOptions, ClusterTlsError> {
    use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use rustls::{client::WebPkiServerVerifier, server::WebPkiClientVerifier};

    let provider = self.provider.clone().unwrap_or_else(active_provider);
    let roots = Arc::new(self.roots);

    // Server: mandatory client-cert auth via the cluster CA.
    let client_verifier =
      WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone()).build()?;
    let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])?
      .with_client_cert_verifier(client_verifier)
      .with_single_cert(self.chain.clone(), self.key.clone_key())?;
    rustls_server.alpn_protocols = vec![ALPN.to_vec()];
    let qsc = QuicServerConfig::try_from(Arc::new(rustls_server))?;
    let server = ServerConfig::with_crypto(Arc::new(qsc));

    // Client: verify the server against the cluster CA; present this node's cert.
    let server_verifier =
      WebPkiServerVerifier::builder_with_provider(roots, provider.clone()).build()?;
    let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])?
      .dangerous()
      .with_custom_certificate_verifier(server_verifier)
      .with_client_auth_cert(self.chain, self.key)?;
    rustls_client.alpn_protocols = vec![ALPN.to_vec()];
    let qcc = QuicClientConfig::try_from(Arc::new(rustls_client))?;
    let client = ClientConfig::new(Arc::new(qcc));

    Ok(QuicOptions::new_inner(
      EndpointConfig::default(),
      Some(client),
      Some(server),
      self.tuning,
      true,
    ))
  }
}

/// A failure assembling the mandatory-mTLS [`QuicOptions`] in [`ClusterTls::try_build`] from an
/// invalid cluster-CA bundle.
///
/// These are construction-time configuration mistakes — a mismatched cert/key, an invalid leaf, an
/// empty root store, a provider without TLS 1.3 — that [`ClusterTls::build`] turns into a panic but
/// [`ClusterTls::try_build`] surfaces here so a caller parsing the bundle from operator-supplied
/// files can recover instead of crashing the process.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ClusterTlsError {
  /// Building a cluster-CA certificate verifier failed (e.g. an empty root store — no trust
  /// anchors). Covers both the server-side client verifier and the client-side server verifier
  /// (rustls uses one `VerifierBuilderError` type for both).
  #[error("failed to build the cluster-CA certificate verifier: {0}")]
  Verifier(#[from] rustls::server::VerifierBuilderError),
  /// Assembling the rustls TLS 1.3 config failed: the provider offers no TLS 1.3 usable cipher
  /// suite, or — the common cert-rotation mistake — the supplied leaf certificate and private key
  /// do not match (a bad/mismatched cert/key pair).
  #[error("invalid cluster TLS configuration (cert/key mismatch or TLS 1.3 unsupported): {0}")]
  Rustls(#[from] rustls::Error),
  /// Converting the assembled rustls config into a quinn QUIC crypto config failed: the provider
  /// exposes no QUIC initial cipher suite (`TLS13_AES_128_GCM_SHA256`, the fixed suite QUIC uses for
  /// Initial packets). Distinct from [`Self::Rustls`] — the rustls TLS 1.3 config itself is valid, it
  /// just lacks that one required suite.
  #[error(
    "cluster TLS configuration is not usable for QUIC (provider lacks the QUIC initial cipher suite \
     TLS13_AES_128_GCM_SHA256): {0}"
  )]
  Quic(#[from] quinn_proto::crypto::rustls::NoInitialCipherSuite),
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;

  /// A test-only cluster CA + per-node certificate issuer (rcgen). `issue_node` issues leaf certs
  /// signed by the CA with the DNS SAN `node-<id-hex>.<cluster-hex>.sailing` — the name
  /// [`sni_for`](super::super::sni_for) derives on `connect`, so the stock WebPki verifier
  /// matches it.
  pub(crate) struct TestClusterCa {
    ca_cert: rcgen::Certificate,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
  }

  pub(crate) struct TestNodeCert {
    pub(crate) cert: rcgen::Certificate,
    pub(crate) key: rcgen::KeyPair,
  }

  impl TestClusterCa {
    pub(crate) fn generate() -> Self {
      let mut params = rcgen::CertificateParams::new(Vec::new()).expect("empty CA params");
      params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
      params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
      let key = rcgen::KeyPair::generate().expect("CA key pair");
      let ca_cert = params.self_signed(&key).expect("self-signed CA");
      let issuer = rcgen::Issuer::new(
        rcgen::CertificateParams::new(Vec::new()).expect("issuer params"),
        key,
      );
      Self { ca_cert, issuer }
    }

    /// The CA certificate in PEM form (for the from-files config layer's tests).
    pub(crate) fn ca_cert_pem(&self) -> std::string::String {
      self.ca_cert.pem()
    }

    /// Build a `RootCertStore` containing the CA certificate.
    pub(crate) fn roots(&self) -> rustls::RootCertStore {
      let mut store = rustls::RootCertStore::empty();
      store
        .add(CertificateDer::from(self.ca_cert.der().to_vec()))
        .expect("CA cert parses as a trust anchor");
      store
    }

    /// Issue a leaf certificate signed by this CA with the SAN `node-<id_hex>.<cluster_hex>.sailing`.
    pub(crate) fn issue_node(&self, san: &str) -> TestNodeCert {
      let mut params =
        rcgen::CertificateParams::new(std::vec![san.to_string()]).expect("valid DNS SAN");
      params
        .key_usages
        .push(rcgen::KeyUsagePurpose::DigitalSignature);
      params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
      params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
      let key = rcgen::KeyPair::generate().expect("leaf key pair");
      let cert = params.signed_by(&key, &self.issuer).expect("signed leaf");
      TestNodeCert { cert, key }
    }

    /// A ready [`ClusterTls`] for one node (roots + the node's chain + key).
    pub(crate) fn cluster_tls(&self, san: &str) -> ClusterTls {
      let node = self.issue_node(san);
      ClusterTls::new(
        self.roots(),
        std::vec![CertificateDer::from(node.cert.der().to_vec())],
        PrivateKeyDer::try_from(node.key.serialize_der()).expect("leaf key DER"),
      )
    }
  }

  #[test]
  fn cluster_tls_builds_mutual_auth_options() {
    let ca = TestClusterCa::generate();
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .build();
    assert!(
      opts.requires_client_auth(),
      "ClusterTls pins mandatory mTLS"
    );
    assert!(opts.client_config().is_some(), "dial config present");
    assert!(opts.server_config().is_some(), "accept config present");
    assert_eq!(opts.max_connections(), DEFAULT_MAX_CONNECTIONS);
  }

  #[test]
  fn try_build_succeeds_on_a_valid_bundle() {
    let ca = TestClusterCa::generate();
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .try_build()
      .expect("a valid cluster bundle builds without error");
    assert!(opts.requires_client_auth());
    assert!(opts.client_config().is_some());
    assert!(opts.server_config().is_some());
  }

  #[cfg(feature = "quic-rustls-ring")]
  #[test]
  fn with_provider_overrides_the_default_and_builds() {
    // The runtime provider seam: supplying an explicit `CryptoProvider` builds the same
    // mandatory-mTLS bundle the compile-time default would (here, ring), rather than being locked to
    // the feature-selected `active_provider()`.
    let ca = TestClusterCa::generate();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .with_provider(provider)
      .try_build()
      .expect("an explicit provider builds a valid cluster bundle");
    assert!(opts.requires_client_auth());
    assert!(opts.client_config().is_some());
    assert!(opts.server_config().is_some());
  }

  #[cfg(feature = "quic-rustls-ring")]
  #[test]
  fn with_provider_missing_quic_initial_suite_fails_with_quic_error() {
    // A provider with TLS 1.3 suites but WITHOUT the QUIC initial suite (AES-128-GCM) still satisfies
    // rustls, then fails quinn's QUIC config conversion -- surfaced as ClusterTlsError::Quic, not
    // Rustls. Locks in the two-stage provider contract documented on `with_provider`.
    let ca = TestClusterCa::generate();
    let mut provider = rustls::crypto::ring::default_provider();
    provider
      .cipher_suites
      .retain(|cs| cs.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256);
    let result = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .with_provider(Arc::new(provider))
      .try_build();
    assert!(
      matches!(result, Err(ClusterTlsError::Quic(_))),
      "a provider without the QUIC initial suite must fail as ClusterTlsError::Quic"
    );
  }

  #[test]
  fn try_build_rejects_a_mismatched_cert_and_key() {
    // Pair leaf A's certificate with leaf B's private key: a valid-PEM-but-mismatched bundle.
    // `try_build` must return the typed error (`Rustls`, from rustls' cert/key acceptance), not
    // panic the way `build` does.
    let ca = TestClusterCa::generate();
    let leaf_a = ca.issue_node("node-0a.00000000000000000000000000000000.sailing");
    let leaf_b = ca.issue_node("node-0b.00000000000000000000000000000000.sailing");
    let cluster = ClusterTls::new(
      ca.roots(),
      std::vec![CertificateDer::from(leaf_a.cert.der().to_vec())],
      PrivateKeyDer::try_from(leaf_b.key.serialize_der()).expect("leaf B key DER"),
    );
    match cluster.try_build() {
      Err(ClusterTlsError::Rustls(_)) => {}
      Err(e) => panic!("a mismatched cert/key must be ClusterTlsError::Rustls, got {e:?}"),
      Ok(_) => panic!("a mismatched cert/key must not build successfully"),
    }
  }

  #[test]
  fn try_build_rejects_an_empty_root_store() {
    // An empty `RootCertStore` has no trust anchors, so the verifier builder fails — surfaced as
    // the typed `Verifier` error rather than a panic.
    let ca = TestClusterCa::generate();
    let leaf = ca.issue_node("node-01.00000000000000000000000000000000.sailing");
    let cluster = ClusterTls::new(
      rustls::RootCertStore::empty(),
      std::vec![CertificateDer::from(leaf.cert.der().to_vec())],
      PrivateKeyDer::try_from(leaf.key.serialize_der()).expect("leaf key DER"),
    );
    match cluster.try_build() {
      Err(ClusterTlsError::Verifier(_)) => {}
      Err(e) => panic!("an empty root store must be ClusterTlsError::Verifier, got {e:?}"),
      Ok(_) => panic!("an empty root store must not build successfully"),
    }
  }

  #[test]
  fn tuning_clamps_and_derives() {
    let t = QuicTuning::new();
    assert_eq!(t.idle_timeout_millis(), IDLE_TIMEOUT_MILLIS);
    assert_eq!(
      t.keep_alive_interval_millis(),
      IDLE_TIMEOUT_MILLIS / 3,
      "keep-alive derives idle/3 by default"
    );
    assert_eq!(t.initial_rtt_millis(), INITIAL_RTT_MILLIS);

    // Zero clamps to 1 (a zero timeout/window is a wedge, not a tuning).
    let t = QuicTuning::new()
      .with_idle_timeout_millis(0)
      .with_initial_rtt_millis(0)
      .with_connection_receive_window(0)
      .with_stream_receive_window(0);
    assert_eq!(t.idle_timeout_millis(), 1);
    assert_eq!(t.initial_rtt_millis(), 1);
    assert_eq!(t.connection_receive_window(), 1);
    assert_eq!(t.stream_receive_window(), 1);

    // A byte WINDOW past the VarInt range clamps to 2^62-1 so build_transport's conversions cannot
    // fail.
    let t = QuicTuning::new().with_connection_receive_window(u64::MAX);
    assert_eq!(t.connection_receive_window(), MAX_VARINT_U64);

    // A TIMER past the Instant-safe bound clamps to MAX_TIMER_MILLIS (NOT the larger VarInt ceiling)
    // so quinn never adds an overflowing `Duration` to a `std::time::Instant`. A raw `u64::MAX` from
    // a config typo is the case that would otherwise panic quinn at runtime.
    let t = QuicTuning::new()
      .with_idle_timeout_millis(u64::MAX)
      .with_initial_rtt_millis(u64::MAX)
      .with_keep_alive_interval_millis(u64::MAX);
    assert_eq!(t.idle_timeout_millis(), MAX_TIMER_MILLIS);
    assert_eq!(t.initial_rtt_millis(), MAX_TIMER_MILLIS);
    assert_eq!(
      t.keep_alive_interval_millis(),
      MAX_TIMER_MILLIS,
      "a positive keep-alive override clamps to the timer bound, not u64::MAX"
    );
    // The clamped timers are FINITE durations build_transport can hand quinn without overflow.
    let _ = QuicOptions::build_transport(&t);

    // An explicit keep-alive override replaces the derivation; 0 = off.
    let t = QuicTuning::new().with_keep_alive_interval_millis(0);
    assert_eq!(t.keep_alive_interval_millis(), 0);
    let t = QuicTuning::new().with_keep_alive_interval_millis(250);
    assert_eq!(t.keep_alive_interval_millis(), 250);

    // The raised idle timeout scales the derived keep-alive with it.
    let t = QuicTuning::new().with_idle_timeout_millis(9_000);
    assert_eq!(t.keep_alive_interval_millis(), 3_000);
  }

  #[test]
  fn timer_clamp_keeps_build_transport_instant_safe() {
    // The whole point of the timer clamp: a `u64::MAX` config typo for EVERY timer knob (which
    // clears the VarInt window clamp but would overflow `Instant + Duration` inside quinn) is
    // clamped to MAX_TIMER_MILLIS, and build_transport then succeeds with finite durations rather
    // than handing quinn a panic-inducing `Duration`.
    let t = QuicTuning::new()
      .with_idle_timeout_millis(u64::MAX)
      .with_initial_rtt_millis(u64::MAX)
      .with_keep_alive_interval_millis(u64::MAX);
    assert_eq!(t.idle_timeout_millis(), MAX_TIMER_MILLIS);
    assert_eq!(t.initial_rtt_millis(), MAX_TIMER_MILLIS);
    assert_eq!(t.keep_alive_interval_millis(), MAX_TIMER_MILLIS);
    // Every resolved timer fits in a `Duration` quinn can add to a `std::time::Instant` without
    // overflowing — assert that directly on the resolved millis (mirrors what build_transport feeds
    // quinn), then drive build_transport itself.
    for ms in [
      t.idle_timeout_millis(),
      t.initial_rtt_millis(),
      t.keep_alive_interval_millis(),
    ] {
      assert!(
        std::time::Instant::now()
          .checked_add(Duration::from_millis(ms))
          .is_some(),
        "the clamped timer {ms} ms must be Instant-addable"
      );
    }
    let _ = QuicOptions::build_transport(&t);
  }

  #[test]
  fn mesh_floor_covers_the_mutual_dial_mesh() {
    assert_eq!(mesh_connection_floor(0), MIN_CONNECTION_FLOOR);
    assert_eq!(mesh_connection_floor(1), MIN_CONNECTION_FLOOR);
    assert_eq!(mesh_connection_floor(2), 6);
    assert_eq!(mesh_connection_floor(4), 12);
    // 63 peers (a 64-node mesh): 3*63 = 189 — the per-peer bound times the peer count.
    assert_eq!(mesh_connection_floor(63), 189);
  }

  #[test]
  fn options_cap_override_is_clamped() {
    let ca = TestClusterCa::generate();
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .build()
      .with_max_connections(0);
    assert_eq!(opts.max_connections(), 1, "0 clamps to 1");
  }

  #[cfg(feature = "serde")]
  #[test]
  fn tuning_serde_round_trips_and_fills_defaults() {
    // Full round-trip: every resolved field survives Serialize → Deserialize.
    let full = QuicTuning::new()
      .with_idle_timeout_millis(9_000)
      .with_initial_rtt_millis(120)
      .with_connection_receive_window(32 * 1024 * 1024)
      .with_stream_receive_window(16 * 1024 * 1024)
      .with_keep_alive_interval_millis(250);
    let json = serde_json::to_string(&full).unwrap();
    assert_eq!(serde_json::from_str::<QuicTuning>(&json).unwrap(), full);

    // A PARTIAL `{}` is the full default (every per-field `serde(default)` fires; keep-alive → None,
    // so the resolved interval is the idle/3 derivation).
    let partial: QuicTuning = serde_json::from_str("{}").unwrap();
    assert_eq!(partial, QuicTuning::new());
    assert_eq!(
      partial.keep_alive_interval_millis(),
      IDLE_TIMEOUT_MILLIS / 3
    );

    // A partial config carrying only one knob fills the rest from the consts.
    let one: QuicTuning = serde_json::from_str(r#"{"idle_timeout_millis": 9000}"#).unwrap();
    assert_eq!(one.idle_timeout_millis(), 9_000);
    assert_eq!(one.initial_rtt_millis(), INITIAL_RTT_MILLIS);
    assert_eq!(one.connection_receive_window(), CONNECTION_RECEIVE_WINDOW);
  }

  #[cfg(feature = "serde")]
  #[test]
  fn tuning_deserialize_goes_through_the_clamp() {
    // The KEY safety check: an out-of-range deserialized value is CLAMPED by the `From`, not
    // accepted raw. A `0` idle timeout (a wedge) becomes the `1`-ms minimum, and a byte window past
    // the VarInt range saturates to `2^62-1` — exactly the programmatic-builder clamp.
    let clamped: QuicTuning = serde_json::from_str(
      r#"{"idle_timeout_millis": 0, "connection_receive_window": 18446744073709551615}"#,
    )
    .unwrap();
    assert_eq!(clamped.idle_timeout_millis(), 1, "0 clamps to the min");
    assert_eq!(
      clamped.connection_receive_window(),
      MAX_VARINT_U64,
      "past-range clamps to the VarInt ceiling"
    );

    // A deserialized `u64::MAX` for each TIMER knob clamps to the Instant-safe MAX_TIMER_MILLIS (NOT
    // the larger VarInt ceiling), and build_transport then succeeds with finite durations — the
    // config-typo case that would otherwise panic quinn after a clean startup.
    let clamped: QuicTuning = serde_json::from_str(
      r#"{"idle_timeout_millis": 18446744073709551615, "initial_rtt_millis": 18446744073709551615, "keep_alive_interval_millis": 18446744073709551615}"#,
    )
    .unwrap();
    assert_eq!(clamped.idle_timeout_millis(), MAX_TIMER_MILLIS);
    assert_eq!(clamped.initial_rtt_millis(), MAX_TIMER_MILLIS);
    assert_eq!(
      clamped.keep_alive_interval_millis(),
      MAX_TIMER_MILLIS,
      "the keep-alive override resolves to the timer bound, not u64::MAX"
    );
    let _ = QuicOptions::build_transport(&clamped);
  }

  #[cfg(feature = "serde")]
  #[test]
  fn tuning_serde_rejects_unknown_field() {
    assert!(
      serde_json::from_str::<QuicTuning>(r#"{"nonsense": 1}"#).is_err(),
      "deny_unknown_fields rejects an unrecognized knob"
    );
  }

  #[cfg(feature = "clap")]
  #[test]
  fn tuning_clap_parses_defaults_and_clamps() {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      tuning: QuicTuning,
    }

    // No flags → every default, byte-identical to `new()`.
    let cli = Cli::try_parse_from(["app"]).unwrap();
    assert_eq!(cli.tuning, QuicTuning::new());

    // A supplied flag is parsed; an out-of-range value goes through the SAME clamp as serde.
    let cli = Cli::try_parse_from(["app", "--quic-idle-timeout-millis", "0"]).unwrap();
    assert_eq!(cli.tuning.idle_timeout_millis(), 1, "0 clamps to the min");

    let cli = Cli::try_parse_from([
      "app",
      "--quic-keep-alive-interval-millis",
      "250",
      "--quic-stream-receive-window",
      "12345",
    ])
    .unwrap();
    assert_eq!(cli.tuning.keep_alive_interval_millis(), 250);
    assert_eq!(cli.tuning.stream_receive_window(), 12345);
    // The unset idle timeout stays at its default.
    assert_eq!(cli.tuning.idle_timeout_millis(), IDLE_TIMEOUT_MILLIS);

    // A `u64::MAX` parsed for each TIMER knob clamps to the Instant-safe MAX_TIMER_MILLIS through
    // the clap path too, and build_transport then succeeds with finite durations.
    let cli = Cli::try_parse_from([
      "app",
      "--quic-idle-timeout-millis",
      "18446744073709551615",
      "--quic-initial-rtt-millis",
      "18446744073709551615",
      "--quic-keep-alive-interval-millis",
      "18446744073709551615",
    ])
    .unwrap();
    assert_eq!(cli.tuning.idle_timeout_millis(), MAX_TIMER_MILLIS);
    assert_eq!(cli.tuning.initial_rtt_millis(), MAX_TIMER_MILLIS);
    assert_eq!(
      cli.tuning.keep_alive_interval_millis(),
      MAX_TIMER_MILLIS,
      "the keep-alive override clamps to the timer bound on the clap path, not u64::MAX"
    );
    let _ = QuicOptions::build_transport(&cli.tuning);
  }

  #[cfg(feature = "clap")]
  #[test]
  fn tuning_clap_env_is_wired() {
    use clap::CommandFactory;
    #[derive(clap::Parser)]
    struct Cli {
      #[command(flatten)]
      tuning: QuicTuning,
    }
    let cmd = Cli::command();
    let arg = cmd
      .get_arguments()
      .find(|a| a.get_id().as_str() == "quic-idle-timeout-millis")
      .unwrap();
    assert_eq!(
      arg.get_env().and_then(|e| e.to_str()),
      Some("SAILING_QUIC_IDLE_TIMEOUT_MILLIS")
    );
  }

  #[cfg(feature = "clap")]
  #[test]
  fn tuning_clap_update_preserves_omitted_non_default_fields() {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      tuning: QuicTuning,
    }

    // A base carrying NON-default values across every knob (a WAN/large-window tuning).
    let base = QuicTuning::new()
      .with_idle_timeout_millis(9_000)
      .with_initial_rtt_millis(120)
      .with_connection_receive_window(32 * 1024 * 1024)
      .with_stream_receive_window(16 * 1024 * 1024)
      .with_keep_alive_interval_millis(250);

    let mut cli = Cli { tuning: base };
    // Supply EXACTLY ONE flag. A bare derived update would treat every other `default_value_t` arg
    // as present and reset it to its pinned default; the `value_source` gate must leave them alone.
    cli
      .try_update_from(["app", "--quic-initial-rtt-millis", "200"])
      .unwrap();

    // The one flagged field changed...
    assert_eq!(cli.tuning.initial_rtt_millis(), 200);
    // ...and EVERY other non-default field is PRESERVED (this is the falsifying assertion: it fails
    // if the value_source gate is removed and the derived reset shrinks them to the defaults).
    assert_eq!(cli.tuning.idle_timeout_millis(), 9_000);
    assert_eq!(cli.tuning.connection_receive_window(), 32 * 1024 * 1024);
    assert_eq!(cli.tuning.stream_receive_window(), 16 * 1024 * 1024);
    assert_eq!(cli.tuning.keep_alive_interval_millis(), 250);

    // A supplied value still routes through the clamp on update (0 → the 1-ms minimum).
    let mut cli = Cli { tuning: base };
    cli
      .try_update_from(["app", "--quic-idle-timeout-millis", "0"])
      .unwrap();
    assert_eq!(
      cli.tuning.idle_timeout_millis(),
      1,
      "0 clamps on update too"
    );
    // The other non-default fields are still preserved across the clamped update.
    assert_eq!(cli.tuning.initial_rtt_millis(), 120);
  }

  #[test]
  fn quic_options_new_is_the_accept_any_path() {
    // The public `new` is the no-mandatory-auth construction (embedders building their own configs):
    // `requires_client_auth` stays false (only `ClusterTls::build` pins it true), and the cap
    // defaults.
    let opts = QuicOptions::new(
      EndpointConfig::default(),
      None::<ClientConfig>,
      None::<ServerConfig>,
      QuicTuning::new(),
    );
    assert!(
      !opts.requires_client_auth(),
      "new() does not pin mandatory mTLS"
    );
    assert!(opts.client_config().is_none());
    assert!(opts.server_config().is_none());
    assert_eq!(opts.max_connections(), DEFAULT_MAX_CONNECTIONS);
  }

  #[cfg(feature = "clap")]
  #[test]
  fn tuning_clap_update_applies_window_and_keepalive_overrides() {
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
      #[command(flatten)]
      tuning: QuicTuning,
    }
    let mut cli = Cli {
      tuning: QuicTuning::new(),
    };
    // The connection-window and keep-alive override applied on UPDATE — the value-source-gated take
    // branches the preserve-omitted test does not exercise.
    cli
      .try_update_from([
        "app",
        "--quic-connection-receive-window",
        "33554432",
        "--quic-keep-alive-interval-millis",
        "0",
      ])
      .unwrap();
    assert_eq!(cli.tuning.connection_receive_window(), 33_554_432);
    assert_eq!(
      cli.tuning.keep_alive_interval_millis(),
      0,
      "an explicit 0 override turns keep-alive off on update"
    );
  }
}
