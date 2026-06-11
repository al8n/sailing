//! `TlsRecords`: a rustls-backed Sans-I/O record layer providing TLS 1.2/1.3 confidentiality.
//! Identity is supplied by wrapping it as `Labeled<TlsRecords>`; this layer alone is anonymous.
//! Requires the `tls` feature (rustls + a crypto provider).
use super::stream::{Intake, RecordIo, sealed};
use crate::Instant;
use rustls::{ClientConfig, Connection, ServerConfig, pki_types::ServerName};
use std::{
  io::{Read, Write},
  sync::Arc,
  vec::Vec,
};

/// Bound on rustls's internal plaintext send buffer. With the limit set, `writer().write` accepts
/// only what fits (a genuine partial accept) instead of buffering without bound.
const SEND_LIMIT: usize = 64 * 1024 * 1024;

/// A rustls connection driven through the Sans-I/O `RecordIo` interface.
pub struct TlsRecords {
  conn: Connection,
  peer_closed: bool,
  aborted: bool,
  /// Conservative count of accepted plaintext not yet drained as ciphertext: incremented by
  /// `write_plaintext`'s accepted count, reset when a transmit poll leaves rustls with nothing
  /// further to write. An upper bound, not byte-exact (rustls exposes no buffered-byte getter) —
  /// with one HANDSHAKE-WINDOW exception: plaintext written before the handshake completes (the
  /// `Labeled` hello) sits in rustls's send buffer awaiting traffic keys while each handshake
  /// flight's drain resets this counter, so it can UNDER-count by up to the hello size until the
  /// handshake finishes. Bounded (≤ the hello, ≤64 KiB) and harmless: application sends cannot
  /// start pre-validation, so the occupancy gate is not consulted while the gap exists.
  queued_plain: usize,
}

impl TlsRecords {
  /// A client-side TLS record layer authenticating the server as `server_name`.
  pub fn client(
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
  ) -> Result<Self, rustls::Error> {
    let mut conn: Connection = rustls::ClientConnection::new(config, server_name)?.into();
    conn.set_buffer_limit(Some(SEND_LIMIT));
    Ok(Self {
      conn,
      peer_closed: false,
      aborted: false,
      queued_plain: 0,
    })
  }

  /// A server-side TLS record layer.
  pub fn server(config: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
    let mut conn: Connection = rustls::ServerConnection::new(config)?.into();
    conn.set_buffer_limit(Some(SEND_LIMIT));
    Ok(Self {
      conn,
      peer_closed: false,
      aborted: false,
      queued_plain: 0,
    })
  }
}

impl TlsRecords {
  /// Test-only: queue a TLS `close_notify` so tests can exercise the in-band close path.
  #[cfg(test)]
  pub(crate) fn send_close_notify_for_test(&mut self) {
    self.conn.send_close_notify();
  }
}

impl sealed::Sealed for TlsRecords {}

impl RecordIo for TlsRecords {
  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    if self.aborted {
      return Intake::Failed;
    }
    let mut rest = input;
    while !rest.is_empty() {
      match self.conn.read_tls(&mut rest) {
        // Post-close_notify latch: once the peer's close_notify has been processed, rustls
        // consumes no further input. The close is surfaced via `peer_has_closed`; any trailing
        // bytes are discarded (a conforming peer sends nothing after close_notify).
        Ok(0) => return Intake::Done,
        Ok(_) => {}
        // rustls signals RECEIVED-PLAINTEXT backpressure as `ErrorKind::Other` ("received
        // plaintext buffer full" — an internal ~16 KiB cap that `set_buffer_limit` does NOT
        // raise; that limit governs the send side only). This is the routine path for any
        // ciphertext chunk decrypting past the cap — e.g. one socket read of a 1 MiB
        // AppendEntries batch — NOT a fault: report Pending so the caller drains plaintext and
        // re-feeds the remainder. Treating it as fatal would kill the connection on every
        // consensus message over ~16 KiB, in a permanent redial/kill flap.
        Err(e) if e.kind() == std::io::ErrorKind::Other => {
          return Intake::Pending(input.len() - rest.len());
        }
        Err(_) => {
          self.aborted = true;
          return Intake::Failed;
        }
      }
      match self.conn.process_new_packets() {
        Ok(state) => {
          if state.peer_has_closed() {
            self.peer_closed = true;
          }
        }
        Err(_) => {
          self.aborted = true;
          return Intake::Failed;
        }
      }
    }
    Intake::Done
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    if self.aborted {
      return 0; // failure inertness: a fatally-errored session emits nothing further
    }
    let before = out.len();
    while self.conn.wants_write() {
      if self.conn.write_tls(out).is_err() {
        break;
      }
    }
    if !self.conn.wants_write() {
      // Everything queued has been emitted as ciphertext into `out`.
      self.queued_plain = 0;
    }
    out.len() - before
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    if self.aborted {
      return 0; // failure inertness: plaintext from a fatally-errored session is untrustworthy
    }
    // Read decrypted plaintext DIRECTLY into `out`'s tail (resize, read into the spare slice,
    // truncate to what arrived) — one copy out of rustls instead of two via a stack hop. The
    // 16 KiB step matches rustls's received-plaintext cap, so one iteration usually drains it.
    const STEP: usize = 16 * 1024;
    let before = out.len();
    loop {
      let len = out.len();
      out.resize(len + STEP, 0);
      match self.conn.reader().read(&mut out[len..]) {
        Ok(0) => {
          out.truncate(len);
          break;
        }
        Ok(n) => out.truncate(len + n),
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
          out.truncate(len);
          break;
        }
        Err(_) => {
          out.truncate(len);
          break;
        }
      }
    }
    out.len() - before
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    if self.aborted {
      return 0; // failure inertness
    }
    // With the buffer limit set, rustls accepts only what fits — a genuine partial accept the
    // caller's pending buffer absorbs.
    let n = self.conn.writer().write(plaintext).unwrap_or(0);
    self.queued_plain += n;
    n
  }

  fn buffered_outbound(&self) -> usize {
    self.queued_plain
  }

  fn is_handshaking(&self) -> bool {
    self.conn.is_handshaking()
  }

  fn peer_identity(&self) -> Option<&[u8]> {
    None
  }

  fn peer_has_closed(&self) -> bool {
    self.peer_closed
  }

  fn is_secure() -> bool {
    true
  }
}

#[cfg(test)]
mod tests;
