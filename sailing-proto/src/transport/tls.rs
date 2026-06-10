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

/// A rustls connection driven through the Sans-I/O `RecordIo` interface.
pub struct TlsRecords {
  conn: Connection,
  peer_closed: bool,
  aborted: bool,
}

impl TlsRecords {
  /// A client-side TLS record layer authenticating the server as `server_name`.
  pub fn client(
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
  ) -> Result<Self, rustls::Error> {
    Ok(Self {
      conn: rustls::ClientConnection::new(config, server_name)?.into(),
      peer_closed: false,
      aborted: false,
    })
  }

  /// A server-side TLS record layer.
  pub fn server(config: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
    Ok(Self {
      conn: rustls::ServerConnection::new(config)?.into(),
      peer_closed: false,
      aborted: false,
    })
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
        // rustls's receive buffer is full — apply backpressure with the count consumed so far.
        Ok(0) => return Intake::Pending(input.len() - rest.len()),
        Ok(_) => {}
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
    let before = out.len();
    while self.conn.wants_write() {
      if self.conn.write_tls(out).is_err() {
        break;
      }
    }
    out.len() - before
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    let before = out.len();
    let mut buf = [0u8; 4096];
    loop {
      match self.conn.reader().read(&mut buf) {
        Ok(0) => break,
        Ok(n) => out.extend_from_slice(&buf[..n]),
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
        Err(_) => break,
      }
    }
    out.len() - before
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    self.conn.writer().write(plaintext).unwrap_or(0)
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
