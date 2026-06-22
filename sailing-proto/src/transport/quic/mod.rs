//! Sans-I/O QUIC transport over `quinn_proto`: the driver pumps UDP datagrams; the consensus
//! [`Endpoint`](crate::Endpoint) and its determinism stay I/O-free. Std-only.

mod bridge;
mod config;
mod conn;
mod coordinator;
mod crypto;
mod identity;

pub use bridge::DialError;
pub use config::{QuicConfigError, QuicConfigOptions};
pub use coordinator::QuicCoordinator;

/// The largest possible hello encoding: the fixed header plus a maximum-length peer id. The QUIC
/// transport bounds its pre-authentication read with this (the control preface IS a hello, framed),
/// so an unvalidated peer cannot buffer more than a hello's worth of bytes before identity binds.
pub(crate) const MAX_HELLO_LEN: usize =
  super::labeled::HELLO_HEADER + super::labeled::MAX_PEER_ID_LEN;

pub use crypto::{ClusterTls, ClusterTlsError, QuicOptions, QuicTuning};
pub use identity::{Hello, Identified, IdentityCtx, IdentityOutcome, IdentitySource};
