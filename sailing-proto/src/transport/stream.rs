//! The pluggable record layer: a Sans-I/O byte transform (framing/crypto/identity handshake) that a
//! `Conn` drives. Sealed — only the layers in this module (`Passthrough`, `TlsRecords`, `Labeled`)
//! implement it.
//!
//! The trait is deliberately NOT generic over the node id `I`: a record layer deals only in raw id
//! BYTES (the `NodeId` encoding). The `Conn`, which knows the concrete `I`, decodes
//! [`RecordIo::peer_identity`] into a [`Peer<I>`](super::Peer). This keeps the identity-agnostic
//! layers (`Passthrough`) from implementing the trait once per `I` (which would make every method
//! call ambiguous).
use std::vec::Vec;

use crate::Instant;

/// Seals [`RecordIo`]/[`StreamTransport`] so only the in-crate record layers implement them. The
/// trait itself is re-exported at the crate root (so a driver can NAME record layers in bounds and
/// construct them), but no downstream type can implement it — the contract below binds only the
/// in-crate layers and their sole intended driver, `Conn`.
pub(crate) mod sealed {
  /// The sealing supertrait.
  pub trait Sealed {}
}

/// The outcome of feeding inbound transport bytes to a record layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intake {
  /// All input was consumed.
  Done,
  /// `n` bytes were consumed (`n <= input.len()`; `n == 0` is legal); the caller must drain
  /// decoded plaintext via [`RecordIo::read_plaintext`] and re-feed the remaining `input[n..]`
  /// VERBATIM — the record layer's receive side is full (backpressure). If an iteration consumes
  /// nothing AND drains no plaintext, the layer is wedged: the caller closes the connection
  /// rather than silently dropping the tail (which would desync framing).
  Pending(usize),
  /// The stream is terminally broken (handshake failure, cluster mismatch). The latch is sticky:
  /// every subsequent call reports `Failed` and the other methods become inert no-ops. Close the
  /// connection.
  Failed,
}

/// A Sans-I/O record layer. It accepts inbound wire bytes, surfaces decoded plaintext, accepts
/// outbound plaintext, and emits wire bytes — with an optional handshake that authenticates a peer.
///
/// Sealed: the concrete layers are `Passthrough`, `TlsRecords`, and `Labeled` — downstream crates
/// can name and construct them but cannot implement the trait. `Conn` is the sole intended driver;
/// the contract notes below are what it relies on.
///
/// # Contract (binding on every implementation)
///
/// - **Append semantics:** [`read_plaintext`](Self::read_plaintext) and
///   [`poll_transport_transmit`](Self::poll_transport_transmit) APPEND to `out` (they never clear
///   it) and return the number of bytes appended.
/// - **Prefix accepts:** [`write_plaintext`](Self::write_plaintext) may accept any prefix
///   (including 0 bytes) under backpressure; the caller retains the unaccepted tail and re-offers
///   it verbatim later. An implementation must never reorder or interleave accepted bytes.
/// - **Intake:** see [`Intake`] — `Pending(n)` requires `n <= input.len()`; `Failed` is sticky.
/// - **Failure inertness:** after `Failed` (or an internal abort), `write_plaintext` accepts 0,
///   the drains produce nothing, and `handle_transport_data` keeps returning `Failed`.
pub trait RecordIo: sealed::Sealed {
  /// Feed inbound wire bytes: advance any handshake and buffer decoded plaintext.
  fn handle_transport_data(&mut self, input: &[u8], now: Instant) -> Intake;
  /// Append queued outbound wire bytes onto `out`, returning the number appended.
  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize;
  /// Append decoded inbound plaintext onto `out`, returning the number appended.
  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize;
  /// Queue outbound plaintext for encoding; returns the number of bytes accepted (a prefix —
  /// possibly 0 under backpressure; the caller re-offers the tail verbatim).
  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize;
  /// Bytes currently queued inside this layer awaiting transmission (its own buffering plus any
  /// inner layer's). The occupancy projection lets the connection enforce ONE outbound bound that
  /// covers every layer of buffering — without it, per-layer caps drift apart and the true
  /// outstanding total is invisible to backpressure decisions.
  fn buffered_outbound(&self) -> usize;
  /// Whether the handshake (record-layer and/or identity) is still in progress.
  fn is_handshaking(&self) -> bool;
  /// The authenticated peer's raw id bytes (the `NodeId` encoding), once the handshake has bound
  /// one. The `Conn` decodes these into a concrete `Peer<I>`.
  fn peer_identity(&self) -> Option<&[u8]>;
  /// Whether the peer has signalled an in-band close (e.g. a TLS `close_notify`).
  fn peer_has_closed(&self) -> bool;
  /// Whether this layer provides confidentiality — a type-level property.
  fn is_secure() -> bool
  where
    Self: Sized;
}

/// A sealed marker naming any record layer, so `Conn` and the coordinators can bound on it without
/// referring to the crate-internal [`RecordIo`]. Implemented automatically for every record layer;
/// cannot be implemented downstream.
pub trait StreamTransport: sealed::Sealed {}

impl<T: RecordIo> StreamTransport for T {}
