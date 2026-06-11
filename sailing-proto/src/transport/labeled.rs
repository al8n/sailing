//! `Labeled<R>`: a cluster + peer-id handshake decorator over any record layer `R`.
//!
//! A one-time binary hello `[magic][version][cluster(16)][peer_id]` authenticates the cluster and
//! binds the peer id before any application frame flows. The hello rides as plaintext through the
//! inner layer, so over `Labeled<TlsRecords>` it is encrypted inside the TLS session.
use super::{
  ClusterId, ConnRole,
  stream::{Intake, RecordIo, sealed},
};
use crate::Instant;
use std::vec::Vec;

const LABEL_MAGIC: u8 = 0xCA;
const LABEL_VERSION: u8 = 1;
/// magic(1) + version(1) + cluster(16) + peer_id_len(2).
const HELLO_HEADER: usize = 1 + 1 + 16 + 2;

/// Construction parameters for a [`Labeled`] layer.
pub struct LabelOptions {
  /// The cluster this node belongs to; a peer advertising a different cluster is rejected.
  pub cluster: ClusterId,
  /// The local node id, already encoded via `NodeId`/`Data` — the bytes carried in the hello.
  pub local_id: Vec<u8>,
}

fn build_hello(cluster: &ClusterId, local_id: &[u8]) -> Vec<u8> {
  let mut h = Vec::with_capacity(HELLO_HEADER + local_id.len());
  h.push(LABEL_MAGIC);
  h.push(LABEL_VERSION);
  h.extend_from_slice(&cluster.0);
  h.extend_from_slice(&(local_id.len() as u16).to_be_bytes());
  h.extend_from_slice(local_id);
  h
}

/// Wraps any record layer with the cluster + peer-id hello. The dialer sends its hello eagerly; the
/// acceptor validates the inbound hello before emitting its own and before any application plaintext
/// is surfaced. A cluster mismatch or malformed hello is terminal ([`Intake::Failed`]).
pub struct Labeled<R> {
  inner: R,
  cluster: ClusterId,
  local_hello: Vec<u8>,
  role: ConnRole,
  /// Inbound plaintext: accumulates the hello pre-validation, then holds application bytes.
  pending: Vec<u8>,
  /// Local hello bytes the inner layer has not yet accepted. `write_plaintext` may accept only a
  /// prefix under backpressure; the tail is retained here and re-offered before ANY application
  /// plaintext, so the hello can never truncate or interleave with app bytes on the wire.
  hello_out: Vec<u8>,
  /// The peer's raw id bytes once the hello validates.
  validated: Option<Vec<u8>>,
  failed: bool,
}

impl<R: RecordIo> Labeled<R> {
  /// The dialer side: queues its hello eagerly into the inner layer.
  pub fn dialer(inner: R, opts: &LabelOptions) -> Self {
    let local_hello = build_hello(&opts.cluster, &opts.local_id);
    let mut this = Self {
      inner,
      cluster: opts.cluster,
      local_hello: local_hello.clone(),
      role: ConnRole::Dialer,
      pending: Vec::new(),
      hello_out: local_hello,
      validated: None,
      failed: false,
    };
    this.flush_hello();
    this
  }

  /// The acceptor side: holds its hello until the inbound hello validates.
  pub fn acceptor(inner: R, opts: &LabelOptions) -> Self {
    let local_hello = build_hello(&opts.cluster, &opts.local_id);
    Self {
      inner,
      cluster: opts.cluster,
      local_hello,
      role: ConnRole::Acceptor,
      pending: Vec::new(),
      hello_out: Vec::new(),
      validated: None,
      failed: false,
    }
  }

  /// Offer pending hello bytes to the inner layer, advancing by exactly the count it accepts.
  fn flush_hello(&mut self) {
    while !self.hello_out.is_empty() {
      let n = self.inner.write_plaintext(&self.hello_out);
      if n == 0 {
        break;
      }
      self.hello_out.drain(..n);
    }
  }

  /// Parse + validate the hello at the front of `pending`. `Failed` on a foreign/malformed hello;
  /// `Done` otherwise (validated, or still waiting for more bytes).
  fn advance_handshake(&mut self) -> Intake {
    if self.validated.is_some() || self.pending.len() < HELLO_HEADER {
      return Intake::Done;
    }
    if self.pending[0] != LABEL_MAGIC || self.pending[1] != LABEL_VERSION {
      self.failed = true;
      return Intake::Failed;
    }
    if self.pending[2..18] != self.cluster.0 {
      self.failed = true;
      return Intake::Failed;
    }
    let peer_id_len = u16::from_be_bytes([self.pending[18], self.pending[19]]) as usize;
    let hello_end = HELLO_HEADER + peer_id_len;
    if self.pending.len() < hello_end {
      return Intake::Done; // need the rest of the peer id
    }
    let peer_id = self.pending[HELLO_HEADER..hello_end].to_vec();
    self.pending.drain(..hello_end);
    self.validated = Some(peer_id);
    if self.role == ConnRole::Acceptor {
      let hello = self.local_hello.clone();
      self.hello_out.extend_from_slice(&hello);
      self.flush_hello();
    }
    Intake::Done
  }
}

impl<R> sealed::Sealed for Labeled<R> {}

impl<R: RecordIo> RecordIo for Labeled<R> {
  fn handle_transport_data(&mut self, input: &[u8], now: Instant) -> Intake {
    if self.failed {
      return Intake::Failed;
    }
    let intake = self.inner.handle_transport_data(input, now);
    if intake == Intake::Failed {
      self.failed = true;
      return Intake::Failed;
    }
    self.inner.read_plaintext(&mut self.pending);
    if self.validated.is_none() && self.advance_handshake() == Intake::Failed {
      return Intake::Failed;
    }
    intake
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.flush_hello();
    self.inner.poll_transport_transmit(out)
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    if self.validated.is_none() {
      return 0; // no application plaintext until the peer is bound
    }
    let n = self.pending.len();
    out.extend_from_slice(&self.pending);
    self.pending.clear();
    n
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    // The hello must FULLY precede any application plaintext: while a hello tail is pending, accept
    // nothing (the caller's own pending buffer holds the app bytes), else app bytes could splice
    // into the middle of the hello on the wire.
    self.flush_hello();
    if !self.hello_out.is_empty() {
      return 0;
    }
    self.inner.write_plaintext(plaintext)
  }

  fn buffered_outbound(&self) -> usize {
    self.hello_out.len() + self.inner.buffered_outbound()
  }

  fn is_handshaking(&self) -> bool {
    self.inner.is_handshaking() || self.validated.is_none()
  }

  fn peer_identity(&self) -> Option<&[u8]> {
    self.validated.as_deref()
  }

  fn peer_has_closed(&self) -> bool {
    self.inner.peer_has_closed()
  }

  fn is_secure() -> bool {
    R::is_secure()
  }
}

#[cfg(test)]
mod tests;
