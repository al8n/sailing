//! The reactor reference driver: one consensus group over framed reliable streams ([`stream`]) on
//! any [`agnostic::Runtime`]. The readiness sibling of the compio stream driver — it owns its
//! coordinator, the embedder's stores, and its listener, and runs the §6.2 driver loop on one
//! `Send` task. The proto-error mapping it shares with the compio drivers lives here.

mod stream;

pub use stream::{AcceptorFactory, DialerFactory, ReactorStreamDriver};

use sailing_proto::ProposeError;

use crate::DriverError;

/// Map the proto's propose-time error to the driver's typed surface.
pub(crate) fn map_propose_err<I: core::fmt::Debug>(e: ProposeError<I>) -> DriverError<I> {
  match e {
    ProposeError::NotLeader { leader } => DriverError::NotLeader { leader },
    ProposeError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// Map the proto's transfer-time error, preserving the redirect hint.
pub(crate) fn map_transfer_err<I: core::fmt::Debug>(
  e: sailing_proto::TransferError<I>,
) -> DriverError<I> {
  match e {
    sailing_proto::TransferError::NotLeader { leader } => DriverError::NotLeader { leader },
    sailing_proto::TransferError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: format!("{other:?}"),
    },
  }
}

/// Map the proto's read-index error: a missing leader is the same redirect signal as a propose
/// rejection (retry once a leader is known), the rest carry their reason.
pub(crate) fn map_read_err<I>(e: sailing_proto::ReadIndexError) -> DriverError<I> {
  match e {
    sailing_proto::ReadIndexError::NoLeader => DriverError::NotLeader { leader: None },
    sailing_proto::ReadIndexError::Poisoned => DriverError::Poisoned,
    other => DriverError::Rejected {
      reason: other.to_string(),
    },
  }
}
