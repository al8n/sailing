//! `Conn<I, R>`: a per-connection lifecycle state machine over a record layer `R`.
//!
//! It feeds inbound wire bytes to the record layer, binds the peer once the handshake (record-layer
//! and label) settles (`Handshaking` → `Validated`), and only then decodes application frames into
//! `Message<I>`s. Any transport-layer fault closes the connection (`Closed`) without ever touching
//! the consensus `Endpoint`.
use super::{
  CoalescedEntry, TransportError,
  frame::{
    COALESCED_FRAME_BUDGET, FrameDecoder, MAX_FRAME_LEN, encode_frame, is_coalesced_frame,
    split_coalesced, split_group_header, write_coalesced_entry, write_coalesced_marker,
    write_group_header,
  },
  stream::{Intake, RecordIo},
};
use crate::{CheapClone, Instant, Message, NodeId};
use std::vec::Vec;

enum ConnState<I> {
  Handshaking,
  Validated {
    peer: I,
  },
  /// Terminal. A CLEAN close (EOF / in-band `close_notify` / outbound-cap stall) retains the
  /// validated peer so frames that arrived in the same read as the close still decode and deliver
  /// (one final drain). An INTEGRITY-SUSPECT close (record-layer failure, frame/message decode
  /// error, malformed peer id) carries `None`: buffered bytes are untrustworthy and are dropped.
  Closed {
    peer: Option<I>,
  },
}

/// The most outbound framed bytes a connection may hold un-accepted by its record layer. A peer
/// that has stalled long enough to leave this much consensus traffic undrained is broken; exceeding
/// the cap closes the connection (an honest signal — the router drops the route and the retry-driven
/// consensus layer re-sends once a fresh connection binds) rather than growing memory without bound.
///
/// COHERENCE: strictly greater than [`MAX_FRAME_LEN`] (2×), so the largest frame the RECEIVER's
/// decoder admits is always sendable from an empty buffer — were the two caps equal, a maximum-size
/// frame could never be emitted (occupancy + header would exceed the cap) and the consensus entry
/// behind it would wedge in a permanent send/close flap.
const MAX_CONN_OUT_BUF: usize = 2 * MAX_FRAME_LEN;

/// A single transport connection: a record layer + a frame decoder + a lifecycle state.
pub struct Conn<I, R> {
  record: R,
  decoder: FrameDecoder,
  state: ConnState<I>,
  /// Framed outbound bytes not yet accepted by the record layer. `write_plaintext` may accept only
  /// a prefix under backpressure, so the unwritten tail is retained here (from `out_pos` on) and
  /// re-offered on the next drain — a frame is never truncated on the wire (which could otherwise
  /// let a later frame's bytes complete a short one and decode as a corrupted-but-valid message).
  /// Occupancy (`len - out_pos`) is bounded by `max_out`.
  out_plain: Vec<u8>,
  /// Read cursor into `out_plain`: bytes before it were already accepted by the record layer.
  /// Draining advances the cursor instead of `Vec::drain` (which would memmove the whole remaining
  /// backlog on every partial accept); the buffer is reset once fully drained.
  out_pos: usize,
  /// The outbound-occupancy bound (the [`MAX_CONN_OUT_BUF`] constant; overridable in tests).
  max_out: usize,
  /// Reusable intake scratch: plaintext drained from the record layer on its way into the frame
  /// decoder. A field (cleared per pass) rather than a per-iteration `Vec::new()` — `handle_data`
  /// runs once per socket read, so the allocation churn would be steady per-chunk overhead.
  scratch: Vec<u8>,
  /// Per-connection outbound encoder: caches the snapshot-transfer meta so a chunked `InstallSnapshot`
  /// encodes its (identical) `SnapshotMeta` once per transfer rather than once per chunk. Byte-identical
  /// to the stateless `wire::encode_message`.
  encoder: crate::wire::MessageEncoder<I>,
}

impl<I: NodeId, R: RecordIo> Conn<I, R> {
  /// A new connection in the `Handshaking` state.
  pub fn new(record: R) -> Self {
    Self {
      record,
      decoder: FrameDecoder::new(),
      state: ConnState::Handshaking,
      out_plain: Vec::new(),
      out_pos: 0,
      max_out: MAX_CONN_OUT_BUF,
      scratch: Vec::new(),
      encoder: crate::wire::MessageEncoder::new(),
    }
  }

  /// Feed inbound wire bytes (with `eof` set when the peer half-closed). Advances the handshake,
  /// buffers decoded application frames, and binds the peer on validation. Closes the connection on
  /// a record-layer failure, an undecodable peer id, or a wedged record layer (backpressure that no
  /// longer makes progress — accepting the silent loss of the remaining bytes would desync framing).
  pub fn handle_data(
    &mut self,
    bytes: &[u8],
    eof: bool,
    now: Instant,
  ) -> Result<(), TransportError> {
    if matches!(self.state, ConnState::Closed { .. }) {
      return Ok(());
    }
    let mut input = bytes;
    loop {
      let consumed_all = match self.record.handle_transport_data(input, now) {
        Intake::Failed => {
          self.close_suspect();
          return Err(TransportError::Record);
        }
        Intake::Done => {
          input = &[];
          true
        }
        Intake::Pending(consumed) => {
          input = &input[consumed..];
          consumed > 0
        }
      };
      // Surface decoded plaintext (post-handshake/label) into the frame decoder via the reusable
      // scratch buffer.
      self.scratch.clear();
      let drained = self.record.read_plaintext(&mut self.scratch);
      if !self.scratch.is_empty() {
        self.decoder.push(&self.scratch);
      }
      self.try_validate()?;
      if input.is_empty() {
        break;
      }
      // Backpressure progress check: the record layer consumed nothing AND surfaced no plaintext —
      // re-offering the same input cannot advance. Dropping the tail silently would leave the frame
      // stream desynced (the next read would splice mid-frame), so the connection fails instead.
      if !consumed_all && drained == 0 {
        self.close_suspect();
        return Err(TransportError::Record);
      }
    }
    // Both CLEAN close signals end the connection: the out-of-band socket EOF, and the record
    // layer's in-band close (a TLS close_notify arrives as ordinary ciphertext, never as an EOF).
    // The validated peer is retained so frames from this final read still deliver (the router
    // performs one last `poll_decoded` before dropping the route).
    if eof || self.record.peer_has_closed() {
      self.state = ConnState::Closed {
        peer: self.current_peer(),
      };
      self.release_out();
    }
    Ok(())
  }

  /// Bind the peer once the record layer reports the handshake complete and an identity is available.
  fn try_validate(&mut self) -> Result<(), TransportError> {
    if !matches!(self.state, ConnState::Handshaking) || self.record.is_handshaking() {
      return Ok(());
    }
    let peer_bytes = self
      .record
      .peer_identity()
      .map(bytes::Bytes::copy_from_slice);
    if let Some(bytes) = peer_bytes {
      // The id must consume the WHOLE identity field (`decode_exact`) — a prefix decode that
      // leaves trailing bytes is a malformed hello, not a valid peer.
      match I::decode_exact(bytes) {
        Ok(peer) => self.state = ConnState::Validated { peer },
        Err(_) => {
          self.close_suspect();
          return Err(TransportError::Decode);
        }
      }
    }
    Ok(())
  }

  /// Decode any complete application frames into `out` as `(group_id_bytes, entry_flags, message)`
  /// triples. Yields nothing until a peer is bound (`Validated`, or a CLEAN `Closed` retaining the
  /// peer for the final drain); a frame that is not a group header plus exactly one `Message` — or
  /// a well-formed coalesced control frame — closes the connection as integrity-suspect. The group
  /// id (empty for a single-group host) selects the target Raft group; a single-message frame
  /// yields flags `0`, while a coalesced frame expands to one triple per entry carrying that
  /// entry's flags (the QUIESCE bit — a multi-group control surface a single-group owner never
  /// sees, since every coalesced entry's tag is non-empty and the empty-tag-only policy closes
  /// first).
  pub fn poll_decoded(
    &mut self,
    out: &mut Vec<(bytes::Bytes, u8, Message<I>)>,
  ) -> Result<(), TransportError> {
    if self.peer().is_none() {
      return Ok(());
    }
    loop {
      // A latched decoder fault (an oversized frame → `FrameTooLarge`) is a transport fault: close the
      // connection as integrity-suspect — the SAME close path as a malformed envelope below — BEFORE
      // propagating, so the close-on-transport-fault invariant holds for any owner (it clears the
      // encoder cache and sets `Closed`), not only the router that drops the `Conn` after an `Err`.
      let frame = match self.decoder.poll() {
        Ok(Some(frame)) => frame,
        Ok(None) => break,
        Err(e) => {
          self.close_suspect();
          return Err(e);
        }
      };
      if is_coalesced_frame(&frame) {
        // A coalesced control frame: expand its entries in order, each the same zero-copy
        // (group, flags, message) shape as a single-message frame. Any malformed entry poisons
        // the whole frame (the same integrity-suspect close as a malformed envelope).
        let entries = match split_coalesced(frame) {
          Ok(entries) => entries,
          Err(e) => {
            self.close_suspect();
            return Err(e);
          }
        };
        for (flags, group, message) in entries {
          match crate::wire::decode_message::<I>(message) {
            Ok(msg) => out.push((group, flags, msg)),
            Err(_) => {
              self.close_suspect();
              return Err(TransportError::Decode);
            }
          }
        }
        continue;
      }
      // Split the transport's group-demux header off the front; the remainder is the encoded
      // `Message`. The group id selects the target Raft group in a multi-group host; a single-group
      // owner sends an empty tag and ignores it here.
      let (group, message) = match split_group_header(frame) {
        Ok(split) => split,
        Err(e) => {
          self.close_suspect();
          return Err(e);
        }
      };
      // ZERO-COPY: the message bytes are a shared slice of the decoder's buffer, and the wire decode
      // slices the message's `Bytes` fields (entry payloads, blobs, contexts, encoded ids) out
      // of the SAME allocation; a frame must carry exactly one well-formed envelope.
      match crate::wire::decode_message::<I>(message) {
        Ok(msg) => out.push((group, 0, msg)),
        Err(_) => {
          self.close_suspect();
          return Err(TransportError::Decode);
        }
      }
    }
    Ok(())
  }

  /// Encode + frame `msg` and queue it for transmission. A closed connection drops the message (it
  /// has no route); the router clears the peer binding so the consensus layer re-routes/retries.
  /// Bounds are enforced per frame by [`emit_frame`](Self::emit_frame) before anything is queued.
  pub fn send_message(&mut self, group: &[u8], msg: &Message<I>) {
    if matches!(self.state, ConnState::Closed { .. }) {
      return;
    }
    let mut payload = Vec::new();
    write_group_header(group, &mut payload);
    self.encoder.encode_message(msg, &mut payload);
    self.emit_frame(&payload);
  }

  /// Encode + frame a batch of `(flags, encoded_group, message)` entries as COALESCED control
  /// frames and queue them for transmission — the multi-group heartbeat path (the frame layer is
  /// payload-agnostic; the caller's policy is what restricts the batch to the heartbeat pair). A
  /// closed connection drops the batch exactly as [`send_message`](Self::send_message) drops one
  /// message.
  ///
  /// The batch is flushed into a new frame before its payload would exceed
  /// [`COALESCED_FRAME_BUDGET`] — a HARD cap, because the receiver rejects any coalesced frame
  /// past the budget. An entry whose encoded size alone exceeds it (the multi coordinators
  /// pre-size and divert these, so reaching this is a caller bug) falls back to a NORMAL
  /// single-message frame rather than emitting a coalesced frame every receiver would reject;
  /// its flags cannot ride a normal frame and are dropped — the safe direction (no false
  /// quiesce), debug-asserted against. Messages are encoded through the per-connection
  /// [`crate::wire::MessageEncoder`] as everywhere else (its snapshot-meta cache is simply never
  /// exercised by heartbeats).
  pub fn send_coalesced(&mut self, entries: &[CoalescedEntry<I>]) {
    if matches!(self.state, ConnState::Closed { .. }) || entries.is_empty() {
      return;
    }
    let mut payload = Vec::new();
    write_coalesced_marker(&mut payload);
    let mut msg_scratch = Vec::new();
    for (flags, group, msg) in entries {
      msg_scratch.clear();
      self.encoder.encode_message(msg, &mut msg_scratch);
      let entry_len = 1 + 2 + group.len() + 4 + msg_scratch.len();
      if entry_len > COALESCED_FRAME_BUDGET {
        debug_assert!(
          *flags == 0,
          "a flagged entry must be pre-sized by its coordinator"
        );
        let mut normal = Vec::with_capacity(2 + group.len() + msg_scratch.len());
        write_group_header(group, &mut normal);
        normal.extend_from_slice(&msg_scratch);
        if !self.emit_frame(&normal) {
          return;
        }
        continue;
      }
      // Flush the frame under construction before this entry would push it past the budget (a
      // frame always carries at least one entry — the marker alone is 2 bytes).
      if payload.len() > 2 && payload.len() + entry_len > COALESCED_FRAME_BUDGET {
        if !self.emit_frame(&payload) {
          return; // the frame tripped a bound and closed the connection
        }
        payload.clear();
        write_coalesced_marker(&mut payload);
      }
      write_coalesced_entry(*flags, group, &msg_scratch, &mut payload);
    }
    if payload.len() > 2 {
      self.emit_frame(&payload);
    }
  }

  /// Queue an empty coalesced control frame — a no-op keep-alive PROBE. Its only effect is to put a
  /// few bytes on the wire so a send-idle connection keeps a silence-reaping peer's clock refreshed;
  /// it decodes to ZERO messages, so it never wakes a quiesced group. A closed connection drops it,
  /// exactly as [`send_message`](Self::send_message) drops one message.
  pub fn send_probe(&mut self) {
    if matches!(self.state, ConnState::Closed { .. }) {
      return;
    }
    let mut payload = Vec::new();
    write_coalesced_marker(&mut payload);
    self.emit_frame(&payload);
  }

  /// Frame `payload` and queue it, enforcing the per-frame bounds BEFORE anything is queued.
  /// Returns whether the frame was accepted (`false` = the connection closed):
  /// - a payload over [`MAX_FRAME_LEN`] closes the connection (the receiver's frame bound would
  ///   reject it and kill the connection on every resend — failing at the source avoids a permanent
  ///   connect/kill flap loop, and keeps the `u32` length prefix exact);
  /// - a send that would push outbound occupancy past the cap closes the connection (the peer has
  ///   stopped draining) instead of growing without bound.
  fn emit_frame(&mut self, payload: &[u8]) -> bool {
    // The bound covers EVERY layer of outbound buffering: this connection's pending frames PLUS
    // whatever the record layer (and its inner layers) already hold — `buffered_outbound` is the
    // occupancy projection that keeps the cap from drifting per layer.
    let occupancy = (self.out_plain.len() - self.out_pos) + self.record.buffered_outbound();
    if payload.len() > MAX_FRAME_LEN || occupancy + 4 + payload.len() > self.max_out {
      // A stall/oversize close, not an integrity fault: inbound frames already decoded are fine.
      self.state = ConnState::Closed {
        peer: self.current_peer(),
      };
      self.release_out();
      return false;
    }
    encode_frame(payload, &mut self.out_plain);
    self.drain_out();
    true
  }

  /// Feed pending framed bytes into the record layer, advancing the cursor by exactly the count it
  /// accepts. A short accept (backpressure) leaves the remainder buffered for the next drain.
  fn drain_out(&mut self) {
    while self.out_pos < self.out_plain.len() {
      let n = self.record.write_plaintext(&self.out_plain[self.out_pos..]);
      if n == 0 {
        return;
      }
      self.out_pos += n;
    }
    // Fully drained: reset the buffer (excess burst capacity is released; the steady-state
    // retention stays so ordinary traffic never reallocates).
    self.out_plain.clear();
    self.out_pos = 0;
    super::shrink_excess(&mut self.out_plain);
  }

  /// Release the outbound and scratch buffers on close (nothing further will be transmitted).
  fn release_out(&mut self) {
    self.out_plain = Vec::new();
    self.out_pos = 0;
    self.scratch = Vec::new();
    // The encoder's snapshot-meta cache is an outbound resource; drop it on close so a closed-but-not-yet-
    // reaped connection retains no cached metadata (the size bound + completion clear already bound it).
    self.encoder.clear();
  }

  /// Drain queued outbound wire bytes into `out`, returning the number written.
  pub fn poll_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.drain_out();
    self.record.poll_transport_transmit(out)
  }

  /// The peer this connection authenticated as: bound while `Validated`, and retained through a
  /// CLEAN close so the final frames can still be attributed and delivered.
  pub fn peer(&self) -> Option<I> {
    match &self.state {
      ConnState::Validated { peer } => Some(peer.cheap_clone()),
      ConnState::Closed { peer } => peer.cheap_clone(),
      ConnState::Handshaking => None,
    }
  }

  /// The peer if currently validated (close-transition helper).
  fn current_peer(&self) -> Option<I> {
    match &self.state {
      ConnState::Validated { peer } => Some(peer.cheap_clone()),
      _ => None,
    }
  }

  /// Transition to an integrity-suspect close: buffered inbound bytes are untrustworthy, so the
  /// peer is NOT retained (no final drain) and the outbound buffer is released.
  fn close_suspect(&mut self) {
    self.state = ConnState::Closed { peer: None };
    self.release_out();
  }

  /// Whether the connection is terminally closed.
  pub fn is_closed(&self) -> bool {
    matches!(self.state, ConnState::Closed { .. })
  }

  /// Whether the connection is still handshaking (not yet validated, not closed). Test-only: the
  /// router tracks handshake progress through its own deadline map.
  #[cfg(test)]
  pub(crate) fn is_handshaking(&self) -> bool {
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
