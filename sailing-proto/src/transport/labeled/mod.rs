//! `Labeled<R>`: a cluster + peer-id handshake decorator over any record layer `R`.
//!
//! A one-time binary hello `[magic][version][cluster(16)][peer_id]` authenticates the cluster and
//! binds the peer id before any application frame flows. The hello rides as plaintext through the
//! inner layer, so over `Labeled<TlsRecords>` it is encrypted inside the TLS session.
use super::{
  ClusterId, ConnRole, TransportError,
  stream::{Intake, RecordIo, sealed},
};
use crate::Instant;
use std::vec::Vec;

const LABEL_MAGIC: u8 = 0xCA;
/// The hello wire version. CONTRACT: any change to the transport wire format — the hello layout,
/// the frame format, or the `Message` codec itself — MUST bump this byte, so mixed-version nodes
/// reject each other at the handshake instead of mis-decoding consensus traffic. The same fence
/// covers a field whose MEANING a peer must not under-populate: version 3 is the failover precise
/// commit-anchor — the first CONSUMER of `SnapshotMeta.max_wall_plus_window` and
/// `max_unwalled_lease_window` (added inert at version 2). A pre-anchor peer that does not fold those
/// floors would feed a successor an under-sized release bound (a stale read), so it must be rejected at
/// the handshake, not tolerated as a forward-compatible additive field. Version 4 adds the `SetReadMode`
/// entry kind (mid-life read-mode migration): a pre-version-4 peer would reject it as an unknown
/// `EntryKind` and close the connection, so the handshake must fence such a peer instead. Version 5
/// adds chunked `InstallSnapshot` (`offset`/`total_len`) and `SnapshotResponse.acked_through`: a
/// pre-version-5 peer would mis-stage a partial chunk as a whole blob, so it must be fenced.
const LABEL_VERSION: u8 = 5;
/// magic(1) + version(1) + cluster(16) + peer_id_len(2).
pub(super) const HELLO_HEADER: usize = 1 + 1 + 16 + 2;
/// The largest peer-id encoding accepted in a hello. Real `NodeId` encodings are a few bytes
/// (a u64 is 8); the u16 length field would otherwise admit ~64 KiB of pre-authentication
/// buffering chosen by an unauthenticated peer.
pub(super) const MAX_PEER_ID_LEN: usize = 1024;

/// Construction parameters for a [`Labeled`] layer.
///
/// `serde` (optional) derives `Serialize`/`Deserialize` directly: both fields are cluster/peer
/// IDENTITY, so neither is defaulted — a missing field is correctly a deserialize error (an
/// identity-less hello layer is not a sensible default). `clap` is intentionally NOT applied: both
/// fields are opaque bytes (`ClusterId` is a raw `[u8; 16]`, `local_id` an already-encoded
/// `NodeId`/`Data` blob), neither of which has a natural CLI/string surface, so forcing a hex
/// `value_parser` would only add an ugly, error-prone flag form. Construct it programmatically or
/// from a config file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LabelOptions {
  /// The cluster this node belongs to; a peer advertising a different cluster is rejected.
  pub cluster: ClusterId,
  /// The local node id, already encoded via `NodeId`/`Data` — the bytes carried in the hello.
  pub local_id: Vec<u8>,
}

pub(super) fn build_hello(cluster: &ClusterId, local_id: &[u8]) -> Vec<u8> {
  let mut h = Vec::with_capacity(HELLO_HEADER + local_id.len());
  h.push(LABEL_MAGIC);
  h.push(LABEL_VERSION);
  h.extend_from_slice(&cluster.0);
  h.extend_from_slice(&(local_id.len() as u16).to_be_bytes());
  h.extend_from_slice(local_id);
  h
}

/// TOTAL parse of one COMPLETE hello against `our` cluster, returning the peer-id bytes.
///
/// The QUIC transport's control preface is exactly one hello delivered as one complete frame, so —
/// unlike [`Labeled`]'s incremental byte-stream parse, where a short prefix legitimately waits for
/// more bytes — every violation here is terminal: a magic/version/cluster mismatch, an id length
/// outside `1..=MAX_PEER_ID_LEN`, a frame shorter than the advertised id, or TRAILING bytes after it
/// (the id must consume the frame exactly; a "valid hello plus junk" is a framing violation, not a
/// hello). Returns `None` for all of them.
#[cfg(feature = "quic")]
pub(super) fn parse_hello_frame<'a>(frame: &'a [u8], our: &ClusterId) -> Option<&'a [u8]> {
  if frame.len() < HELLO_HEADER {
    return None;
  }
  if frame[0] != LABEL_MAGIC || frame[1] != LABEL_VERSION {
    return None;
  }
  if frame[2..18] != our.0 {
    return None;
  }
  let peer_id_len = u16::from_be_bytes([frame[18], frame[19]]) as usize;
  if peer_id_len == 0 || peer_id_len > MAX_PEER_ID_LEN {
    return None;
  }
  if frame.len() != HELLO_HEADER + peer_id_len {
    return None;
  }
  Some(&frame[HELLO_HEADER..])
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
  ///
  /// Errors with [`TransportError::InvalidLocalId`] if the local id encoding is outside
  /// `1..=MAX_PEER_ID_LEN` bytes — the same bound the inbound side enforces. An oversized id
  /// would otherwise wrap through the hello's `u16` length field, so the peer would parse a
  /// truncated id and treat the remaining id bytes as application plaintext.
  pub fn dialer(inner: R, opts: &LabelOptions) -> Result<Self, TransportError> {
    Self::check_local_id(&opts.local_id)?;
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
    Ok(this)
  }

  /// The acceptor side: holds its hello until the inbound hello validates.
  ///
  /// Errors with [`TransportError::InvalidLocalId`] under the same local-id bound as
  /// [`Self::dialer`].
  pub fn acceptor(inner: R, opts: &LabelOptions) -> Result<Self, TransportError> {
    Self::check_local_id(&opts.local_id)?;
    let local_hello = build_hello(&opts.cluster, &opts.local_id);
    Ok(Self {
      inner,
      cluster: opts.cluster,
      local_hello,
      role: ConnRole::Acceptor,
      pending: Vec::new(),
      hello_out: Vec::new(),
      validated: None,
      failed: false,
    })
  }

  /// The OUTBOUND mirror of `advance_handshake`'s peer-id bound: a hello we would reject on
  /// receipt must never be constructed, let alone sent.
  fn check_local_id(local_id: &[u8]) -> Result<(), TransportError> {
    if local_id.is_empty() || local_id.len() > MAX_PEER_ID_LEN {
      return Err(TransportError::InvalidLocalId);
    }
    Ok(())
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
    // An empty id is no identity at all, and an oversized one is unauthenticated buffer growth —
    // both are malformed hellos, rejected before any id byte is retained.
    if peer_id_len == 0 || peer_id_len > MAX_PEER_ID_LEN {
      self.failed = true;
      return Intake::Failed;
    }
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
    if self.failed {
      return 0; // failure inertness: never emit (not even our hello) on a rejected stream
    }
    self.flush_hello();
    self.inner.poll_transport_transmit(out)
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    if self.failed || self.validated.is_none() {
      return 0; // no application plaintext until the peer is bound (and none after a failure)
    }
    let n = self.pending.len();
    out.extend_from_slice(&self.pending);
    self.pending.clear();
    super::shrink_excess(&mut self.pending);
    n
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> usize {
    if self.failed {
      return 0; // failure inertness
    }
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
