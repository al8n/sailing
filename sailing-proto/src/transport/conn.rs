//! `Conn<I, R>`: a per-connection lifecycle state machine over a record layer `R`.
//!
//! It feeds inbound wire bytes to the record layer, binds the peer once the handshake (record-layer
//! and label) settles (`Handshaking` → `Validated`), and only then decodes application frames into
//! `Message<I>`s. Any transport-layer fault closes the connection (`Closed`) without ever touching
//! the consensus `Endpoint`.
use super::{
  TransportError,
  frame::{FrameDecoder, encode_frame},
  stream::{Intake, RecordIo},
};
use crate::{Data, Instant, Message, NodeId};
use std::vec::Vec;

enum ConnState<I> {
  Handshaking,
  Validated { peer: I },
  Closed,
}

/// The most outbound framed bytes a connection may hold un-accepted by its record layer (64 MiB).
/// A peer that has stalled long enough to leave this much consensus traffic undrained is broken;
/// exceeding the cap closes the connection (an honest signal — the router drops the route and the
/// retry-driven consensus layer re-sends once a fresh connection binds) rather than growing memory
/// without bound.
const MAX_CONN_OUT_BUF: usize = 64 * 1024 * 1024;

/// A single transport connection: a record layer + a frame decoder + a lifecycle state.
pub struct Conn<I, R> {
  record: R,
  decoder: FrameDecoder,
  state: ConnState<I>,
  /// Framed outbound bytes not yet accepted by the record layer. `write_plaintext` may accept only
  /// a prefix under backpressure, so the unwritten tail is retained here and re-offered on the next
  /// drain — a frame is never truncated on the wire (which could otherwise let a later frame's bytes
  /// complete a short one and decode as a corrupted-but-valid message). Bounded by `max_out`.
  out_plain: Vec<u8>,
  /// The `out_plain` bound (the [`MAX_CONN_OUT_BUF`] constant; overridable in tests).
  max_out: usize,
}

impl<I: NodeId, R: RecordIo> Conn<I, R> {
  /// A new connection in the `Handshaking` state.
  pub fn new(record: R) -> Self {
    Self {
      record,
      decoder: FrameDecoder::new(),
      state: ConnState::Handshaking,
      out_plain: Vec::new(),
      max_out: MAX_CONN_OUT_BUF,
    }
  }

  /// Feed inbound wire bytes (with `eof` set when the peer half-closed). Advances the handshake,
  /// buffers decoded application frames, and binds the peer on validation. Closes the connection on
  /// a record-layer failure or an undecodable peer id.
  pub fn handle_data(
    &mut self,
    bytes: &[u8],
    eof: bool,
    now: Instant,
  ) -> Result<(), TransportError> {
    if matches!(self.state, ConnState::Closed) {
      return Ok(());
    }
    let mut input = bytes;
    loop {
      match self.record.handle_transport_data(input, now) {
        Intake::Failed => {
          self.state = ConnState::Closed;
          return Err(TransportError::Record);
        }
        Intake::Done => input = &[],
        Intake::Pending(consumed) => {
          if consumed == 0 {
            break; // no progress this pass — avoid spinning
          }
          input = &input[consumed..];
        }
      }
      // Surface application plaintext (post-handshake/label) into the frame decoder.
      let mut plain = Vec::new();
      self.record.read_plaintext(&mut plain);
      if !plain.is_empty() {
        self.decoder.push(&plain);
      }
      self.try_validate()?;
      if input.is_empty() {
        break;
      }
    }
    // Both close signals end the connection: the out-of-band socket EOF, and the record layer's
    // in-band close (a TLS close_notify arrives as ordinary ciphertext, never as an EOF).
    if eof || self.record.peer_has_closed() {
      self.state = ConnState::Closed;
    }
    Ok(())
  }

  /// Bind the peer once the record layer reports the handshake complete and an identity is available.
  fn try_validate(&mut self) -> Result<(), TransportError> {
    if !matches!(self.state, ConnState::Handshaking) || self.record.is_handshaking() {
      return Ok(());
    }
    let peer_bytes = self.record.peer_identity().map(|b| b.to_vec());
    if let Some(bytes) = peer_bytes {
      // Require the id to consume the WHOLE identity field — a prefix decode that leaves trailing
      // bytes is a malformed hello, not a valid peer.
      match I::decode(&bytes) {
        Ok((n, peer)) if n == bytes.len() => self.state = ConnState::Validated { peer },
        _ => {
          self.state = ConnState::Closed;
          return Err(TransportError::Decode);
        }
      }
    }
    Ok(())
  }

  /// Decode any complete application frames into `out`. Yields nothing until the connection is
  /// `Validated`; a frame that is not exactly one `Message` closes the connection.
  pub fn poll_decoded(&mut self, out: &mut Vec<Message<I>>) -> Result<(), TransportError> {
    if !matches!(self.state, ConnState::Validated { .. }) {
      return Ok(());
    }
    let mut frame = Vec::new();
    while self.decoder.poll(&mut frame)? {
      match Message::<I>::decode(&frame) {
        Ok((n, msg)) if n == frame.len() => out.push(msg),
        _ => {
          self.state = ConnState::Closed;
          return Err(TransportError::Decode);
        }
      }
    }
    Ok(())
  }

  /// Encode + frame `msg` and queue it for transmission. A closed connection drops the message (it
  /// has no route); the router clears the peer binding so the consensus layer re-routes/retries.
  /// If queuing would exceed the outbound cap (the peer has stopped draining), the connection
  /// closes instead of growing without bound.
  pub fn send_message(&mut self, msg: &Message<I>) {
    if matches!(self.state, ConnState::Closed) {
      return;
    }
    let mut payload = Vec::new();
    msg.encode(&mut payload);
    encode_frame(&payload, &mut self.out_plain);
    self.drain_out();
    if self.out_plain.len() > self.max_out {
      self.state = ConnState::Closed;
      self.out_plain = Vec::new();
    }
  }

  /// Feed pending framed bytes into the record layer, advancing by exactly the count it accepts. A
  /// short accept (backpressure) leaves the remainder buffered for the next drain.
  fn drain_out(&mut self) {
    while !self.out_plain.is_empty() {
      let n = self.record.write_plaintext(&self.out_plain);
      if n == 0 {
        break;
      }
      self.out_plain.drain(..n);
    }
  }

  /// Drain queued outbound wire bytes into `out`, returning the number written.
  pub fn poll_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.drain_out();
    self.record.poll_transport_transmit(out)
  }

  /// The bound peer, once `Validated`.
  pub fn peer(&self) -> Option<I> {
    match self.state {
      ConnState::Validated { peer } => Some(peer),
      _ => None,
    }
  }

  /// Whether the connection is terminally closed.
  pub fn is_closed(&self) -> bool {
    matches!(self.state, ConnState::Closed)
  }

  /// Whether the connection is still handshaking (not yet validated, not closed).
  pub fn is_handshaking(&self) -> bool {
    matches!(self.state, ConnState::Handshaking)
  }

  /// Test-only: inject raw plaintext bytes into the record layer, bypassing `send_message`'s
  /// encoding — used to feed a peer a deliberately malformed frame.
  #[cfg(test)]
  pub(crate) fn record_write_for_test(&mut self, bytes: &[u8]) {
    self.record.write_plaintext(bytes);
  }

  /// Test-only: shrink the outbound cap so the cap-exceeded close is testable without 64 MiB.
  #[cfg(test)]
  pub(crate) fn set_max_out_for_test(&mut self, max: usize) {
    self.max_out = max;
  }
}

#[cfg(test)]
mod tests;
