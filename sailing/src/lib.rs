//! Sailing: a Sans-I/O Raft consensus library.
//!
//! This is the umbrella crate. It re-exports the runtime-agnostic protocol core as
//! [`proto`] and, behind feature flags, an I/O driver: the runtime-agnostic
//! [`reactor`] (tokio / smol via `agnostic`) or the completion-I/O [`compio`] backend.
//!
//! ```toml
//! sailing = { version = "0.1", features = ["tokio"] }   # reactor on tokio
//! sailing = { version = "0.1", features = ["compio"] }  # completion-I/O backend
//! # the no_std + alloc protocol core only (no driver):
//! sailing = { version = "0.1", default-features = false, features = ["alloc", "tcp"] }
//! ```
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// The Sans-I/O Raft protocol core: the `Endpoint` state machine, typed messages,
/// the storage traits, events, and the transport record layer.
pub use sailing_proto as proto;

/// The runtime-agnostic reactor driver (tokio / smol via `agnostic`); pick the runtime
/// with the `tokio` or `smol` feature.
#[cfg(feature = "reactor")]
#[cfg_attr(docsrs, doc(cfg(feature = "reactor")))]
pub mod reactor {
  pub use sailing_reactor::*;
}

/// The completion-I/O (compio) driver.
#[cfg(feature = "compio")]
#[cfg_attr(docsrs, doc(cfg(feature = "compio")))]
pub mod compio {
  pub use sailing_compio::*;
}
