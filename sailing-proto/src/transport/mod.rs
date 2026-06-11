//! Sans-I/O transport: framed, reliable streams that drive `Endpoint`.
//! Feature-gated; the consensus core stays no_std + dependency-free by default.

// The framed-stream stack belongs to `tcp` (and `tls`, which implies it); a bare `quic` build gets
// only the shared types below until the QUIC coordinator lands.
#[cfg(feature = "tcp")]
mod conn;
#[cfg(feature = "tcp")]
mod coordinator;
#[cfg(feature = "tcp")]
mod frame;
#[cfg(feature = "tcp")]
mod labeled;
#[cfg(feature = "tcp")]
mod passthrough;
#[cfg(feature = "tcp")]
mod router;
#[cfg(feature = "tcp")]
mod stream;
#[cfg(feature = "tls")]
mod tls;

#[cfg(feature = "tcp")]
pub use coordinator::StreamCoordinator;
#[cfg(feature = "tcp")]
pub use labeled::{LabelOptions, Labeled};
#[cfg(feature = "tcp")]
pub use passthrough::Passthrough;
#[cfg(feature = "tcp")]
pub use stream::{Intake, RecordIo, StreamTransport};
#[cfg(feature = "tls")]
pub use tls::TlsRecords;

/// Release excess capacity from a FULLY-DRAINED buffer that once absorbed a large burst.
///
/// Every transport buffer is bounded by its layer's cap, but `clear()`/`drain()` retain peak
/// capacity — one 64 MiB burst would otherwise pin that much heap per connection for its
/// lifetime. Shrinking only when empty AND well past the retention size avoids regrow thrash in
/// steady state (ordinary traffic never trips the 4× threshold).
#[cfg(feature = "tcp")]
pub(crate) fn shrink_excess(buf: &mut std::vec::Vec<u8>) {
  /// Capacity worth keeping around for steady-state traffic.
  const RETAIN: usize = 64 * 1024;
  if buf.is_empty() && buf.capacity() > 4 * RETAIN {
    buf.shrink_to(RETAIN);
  }
}

/// A 16-byte cluster identity; peers reject handshakes from other clusters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterId(
  /// The raw 16 cluster-identity bytes.
  pub [u8; 16],
);

/// An authenticated transport peer (replica id of type `I`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer<I> {
  /// The peer's replica id.
  pub id: I,
}

/// Per-connection handle assigned by the driver.
///
/// CONTRACT: the driver must assign ids in monotonically increasing order (a simple counter).
/// The router's duplicate-peer tie-break relies on it — when two connections authenticate as the
/// same peer, the HIGHER id (the newer dial) wins and the older is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId(
  /// The driver-assigned connection number (monotonically increasing).
  pub u64,
);

/// Which side opened a connection (affects handshake ordering).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnRole {
  /// This node initiated the connection.
  Dialer,
  /// This node accepted the connection.
  Acceptor,
}

/// Transport-layer failure. DISTINCT from `crate::PoisonReason`: a transport fault
/// closes one connection and never poisons the consensus `Endpoint`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
  /// A frame's length prefix exceeds the maximum allowed size.
  #[error("frame exceeds the maximum size")]
  FrameTooLarge,
  /// A framed payload failed to decode into a `Message`.
  #[error("message decode failed")]
  Decode,
  /// The record layer rejected the stream (handshake failure, cluster mismatch).
  #[error("record layer rejected the stream")]
  Record,
  /// An application frame arrived before the connection was validated.
  #[error("connection not yet validated")]
  NotValidated,
}
