//! Sans-I/O transport: framed, reliable streams that drive `Endpoint`.
//! Feature-gated; the consensus core stays no_std + dependency-free by default.
// The transport is built bottom-up: each lower layer (framing, the record layers, `Conn`) lands
// before the consumer that uses it (the coordinators). This module-scoped allow keeps those
// intermediate states warning-clean; it is removed once the coordinators wire everything together.
#![allow(dead_code)]

mod frame;
mod labeled;
mod passthrough;
mod stream;

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

/// Opaque per-connection handle assigned by the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId(
  /// The driver-assigned connection number.
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
