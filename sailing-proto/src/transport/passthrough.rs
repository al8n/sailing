//! `Passthrough`: a plaintext record layer — a bounded byte pipe with no crypto, handshake, or
//! identity. Used under `Labeled` on a plain-TCP link (or in a trusted network).
use super::stream::{Intake, RecordIo, sealed};
use crate::Instant;
use std::vec::Vec;

/// Inbound plaintext buffer cap (256 KiB); a larger inbound chunk yields `Pending` backpressure.
const RECV_LIMIT: usize = 256 * 1024;
/// Outbound buffer cap (64 MiB); `write_plaintext` accepts only up to the remaining room.
const SEND_LIMIT: usize = 64 * 1024 * 1024;

/// A plaintext record layer: inbound wire bytes ARE plaintext and outbound plaintext IS the wire.
/// No handshake, no identity, no confidentiality.
pub struct Passthrough {
  inbound: Vec<u8>,
  outbound: Vec<u8>,
}

impl Passthrough {
  /// A new, empty passthrough pipe.
  pub fn new() -> Self {
    Self {
      inbound: Vec::new(),
      outbound: Vec::new(),
    }
  }
}

impl Default for Passthrough {
  fn default() -> Self {
    Self::new()
  }
}

impl sealed::Sealed for Passthrough {}

impl RecordIo for Passthrough {
  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    let room = RECV_LIMIT.saturating_sub(self.inbound.len());
    if input.len() <= room {
      self.inbound.extend_from_slice(input);
      Intake::Done
    } else {
      self.inbound.extend_from_slice(&input[..room]);
      Intake::Pending(room)
    }
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.outbound.len();
    out.extend_from_slice(&self.outbound);
    self.outbound.clear();
    n
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.inbound.len();
    out.extend_from_slice(&self.inbound);
    self.inbound.clear();
    n
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    let room = SEND_LIMIT.saturating_sub(self.outbound.len());
    let take = plaintext.len().min(room);
    self.outbound.extend_from_slice(&plaintext[..take]);
    take
  }

  fn buffered_outbound(&self) -> usize {
    self.outbound.len()
  }

  fn is_handshaking(&self) -> bool {
    false
  }

  fn peer_identity(&self) -> Option<&[u8]> {
    None
  }

  fn peer_has_closed(&self) -> bool {
    false
  }

  fn is_secure() -> bool {
    false
  }
}

#[cfg(test)]
mod tests;
