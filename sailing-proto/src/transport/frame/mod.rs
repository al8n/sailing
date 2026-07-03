//! Length-prefixed framing: `[u32 big-endian length][payload]`, with a bounded decoder.
use super::TransportError;
use bytes::{Bytes, BytesMut};
use std::vec::Vec;

/// The largest single frame payload accepted (64 MiB). A length prefix above this is rejected
/// rather than buffered, bounding a connection's decode memory. Derives from the single source of
/// truth in [`crate::wire::MAX_FRAME_BYTES`], which the core's snapshot-chunk sizer also reads so each
/// chunked `InstallSnapshot` (metadata + chunk) is bounded below this limit.
pub(crate) const MAX_FRAME_LEN: usize = crate::wire::MAX_FRAME_BYTES;

/// Append a length-prefixed frame (`[u32 BE len][payload]`) to `out`.
pub(crate) fn encode_frame(payload: &[u8], out: &mut Vec<u8>) {
  out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
  out.extend_from_slice(payload);
}

/// Prepend the multi-Raft group-demux header `[u16 BE group_len][group bytes]` to a frame payload
/// being built. An empty group (`group_len == 0`) is the single-group / default tag. The caller
/// guarantees `group.len() <= crate::wire::MAX_GROUP_ID_LEN` (a `GroupId` encoding is bounded by
/// that; the single-group path passes an empty slice).
pub(crate) fn write_group_header(group: &[u8], out: &mut Vec<u8>) {
  debug_assert!(group.len() <= crate::wire::MAX_GROUP_ID_LEN);
  out.extend_from_slice(&(group.len() as u16).to_be_bytes());
  out.extend_from_slice(group);
}

/// Split the group-demux header off a decoded frame, returning `(group_id_bytes, message_bytes)` as
/// zero-copy slices of the same buffer. `Err(Decode)` if the header is truncated or declares a group
/// past [`crate::wire::MAX_GROUP_ID_LEN`] — which includes the [`COALESCED_MARKER`] (0xFFFF), so a
/// coalesced frame handed to the single-message parser errors instead of aliasing as a group tag.
pub(crate) fn split_group_header(frame: Bytes) -> Result<(Bytes, Bytes), TransportError> {
  if frame.len() < 2 {
    return Err(TransportError::Decode);
  }
  let group_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
  if group_len > crate::wire::MAX_GROUP_ID_LEN || frame.len() < 2 + group_len {
    return Err(TransportError::Decode);
  }
  Ok((frame.slice(2..2 + group_len), frame.slice(2 + group_len..)))
}

/// The `u16` big-endian value opening a COALESCED control frame's payload, where a single-message
/// frame carries its group length. 0xFFFF is impossible as a group length — the bound is
/// [`crate::wire::MAX_GROUP_ID_LEN`] (1024) — so the two payload forms are disjoint at the first two
/// bytes: a pre-coalescing peer's parser rejects a coalesced frame outright rather than mis-reading
/// it (and the `LABEL_VERSION` hello fence rejects such a peer before any frame flows anyway).
pub(crate) const COALESCED_MARKER: u16 = 0xFFFF;

// COHERENCE (compile-time): the marker sits OUTSIDE the group-length range, so the two payload
// forms stay disjoint at their first two bytes.
const _: () = assert!(COALESCED_MARKER as usize > crate::wire::MAX_GROUP_ID_LEN);

/// Entry-flags bit 0: the sender QUIESCES this entry's group after this beat — the receiver's driver
/// may stop arming that group's timers until traffic or a connection loss wakes it. All other bits
/// must be zero on encode and are ignored on decode (forward room).
pub(crate) const COALESCED_FLAG_QUIESCE: u8 = 0b0000_0001;

/// Senders flush a coalesced frame before its payload would exceed this budget (64 KiB — thousands
/// of heartbeats per frame). Deliberately nowhere near [`MAX_FRAME_LEN`] (1/1024 of it), so the
/// append/snapshot frame-fit sizer math is untouched: a coalesced control frame can never contend
/// with a data frame for the frame bound.
pub(crate) const COALESCED_FRAME_BUDGET: usize = 64 * 1024;

// COHERENCE (compile-time): the budget stays 1/1024 of the frame bound, keeping the doc's
// never-near-the-sizer-math claim honest if either constant moves.
const _: () = assert!(COALESCED_FRAME_BUDGET <= MAX_FRAME_LEN / 1024);

/// Whether a decoded frame payload is a coalesced control frame (opens with [`COALESCED_MARKER`]).
pub(crate) fn is_coalesced_frame(frame: &[u8]) -> bool {
  frame.len() >= 2 && frame[..2] == COALESCED_MARKER.to_be_bytes()
}

/// Open a coalesced frame payload being built: the [`COALESCED_MARKER`], then one or more
/// [`write_coalesced_entry`] records.
pub(crate) fn write_coalesced_marker(out: &mut Vec<u8>) {
  out.extend_from_slice(&COALESCED_MARKER.to_be_bytes());
}

/// Append one coalesced entry `[u8 flags][u16 BE group_len][group bytes][u32 BE msg_len][msg]` to a
/// payload opened by [`write_coalesced_marker`]. The caller guarantees a NON-empty group within
/// [`crate::wire::MAX_GROUP_ID_LEN`] (coalescing is a multi-group feature — the empty single-group
/// tag never rides here) and only defined flag bits.
pub(crate) fn write_coalesced_entry(flags: u8, group: &[u8], msg_bytes: &[u8], out: &mut Vec<u8>) {
  debug_assert!(!group.is_empty() && group.len() <= crate::wire::MAX_GROUP_ID_LEN);
  debug_assert_eq!(flags & !COALESCED_FLAG_QUIESCE, 0, "undefined flag bits");
  out.push(flags);
  out.extend_from_slice(&(group.len() as u16).to_be_bytes());
  out.extend_from_slice(group);
  out.extend_from_slice(&(msg_bytes.len() as u32).to_be_bytes());
  out.extend_from_slice(msg_bytes);
}

/// Split a coalesced frame into its `(flags, group_id_bytes, message_bytes)` entries, each a
/// zero-copy slice of the frame (like [`split_group_header`]). `Err(Decode)` on a missing marker, a
/// truncated entry, a group length of zero or past [`crate::wire::MAX_GROUP_ID_LEN`], a message
/// length overrunning the frame, or an empty entry list — the payload must be exactly one or more
/// complete entries, so any trailing remainder after the last complete one rejects too.
pub(crate) fn split_coalesced(frame: Bytes) -> Result<Vec<(u8, Bytes, Bytes)>, TransportError> {
  if !is_coalesced_frame(&frame) {
    return Err(TransportError::Decode);
  }
  // The send budget is ALSO the receive bound: a conforming sender never exceeds it, and without
  // this check one otherwise-valid frame of minimal entries (8 bytes each under the 64 MiB frame
  // cap) would materialize millions of slice tuples before the first message decode could reject
  // — an authenticated-peer allocation amplification. Bounding the payload bounds the entry list.
  if frame.len() > 2 + COALESCED_FRAME_BUDGET {
    return Err(TransportError::Decode);
  }
  let mut entries = Vec::new();
  let mut at = 2usize;
  while at < frame.len() {
    // [u8 flags][u16 BE group_len] — the fixed entry prefix.
    if frame.len() - at < 3 {
      return Err(TransportError::Decode);
    }
    let flags = frame[at];
    let group_len = u16::from_be_bytes([frame[at + 1], frame[at + 2]]) as usize;
    if group_len == 0 || group_len > crate::wire::MAX_GROUP_ID_LEN {
      return Err(TransportError::Decode);
    }
    at += 3;
    if frame.len() - at < group_len + 4 {
      return Err(TransportError::Decode);
    }
    let group = frame.slice(at..at + group_len);
    at += group_len;
    let msg_len =
      u32::from_be_bytes([frame[at], frame[at + 1], frame[at + 2], frame[at + 3]]) as usize;
    at += 4;
    if frame.len() - at < msg_len {
      return Err(TransportError::Decode);
    }
    let msg = frame.slice(at..at + msg_len);
    at += msg_len;
    entries.push((flags, group, msg));
  }
  if entries.is_empty() {
    return Err(TransportError::Decode);
  }
  Ok(entries)
}

/// Reassembles length-prefixed frames from a byte stream that may arrive in arbitrary chunks.
///
/// [`push`](Self::push) walks the input frame by frame and validates each frame's length header
/// the moment its fourth byte arrives — BEFORE accepting any of that frame's payload — so a frame
/// declaring more than [`MAX_FRAME_LEN`] never has its payload buffered or copied, even when it
/// follows valid frames inside the same input chunk. An oversized header latches the decoder
/// failed (a terminal stream error: the whole buffer is released and every subsequent
/// [`poll`](Self::poll) reports `FrameTooLarge`; the caller closes the connection — frames already
/// buffered are dropped, which is equivalent to in-flight loss and safe for the retry-driven
/// consensus layer above).
///
/// [`poll`](Self::poll) yields each frame's payload as a ZERO-COPY shared [`Bytes`] slice of the
/// accumulation buffer (`BytesMut::split_to` + freeze — O(1), no memmove), so a decoded `Message`'s
/// `Bytes` fields (entry payloads, snapshot blobs, contexts) share the buffer end-to-end. The
/// deliberate flip side: a long-retained slice (an entry payload living in the log) keeps its
/// buffer GENERATION alive — `BytesMut` starts a fresh allocation when it must grow while old
/// slices still exist, so a generation's footprint is one burst, not the connection's lifetime.
pub(crate) struct FrameDecoder {
  /// Complete frames awaiting [`poll`](Self::poll), then the bytes of the trailing partial frame.
  buf: BytesMut,
  /// Bytes of the trailing partial frame present at the tail of `buf` (header + payload so far).
  fill: usize,
  /// Total size (4 + declared payload length) of the trailing partial frame, once its header has
  /// arrived and been validated. `None` while the header is still incomplete.
  expect: Option<usize>,
  /// Latched once an oversized length prefix is seen; the stream is terminally broken.
  failed: bool,
}

impl FrameDecoder {
  /// A decoder with an empty buffer.
  pub(crate) fn new() -> Self {
    Self {
      buf: BytesMut::new(),
      fill: 0,
      expect: None,
      failed: false,
    }
  }

  /// Feed received bytes (any chunking) into the decoder.
  pub(crate) fn push(&mut self, bytes: &[u8]) {
    if self.failed {
      return;
    }
    let mut input = bytes;
    while !input.is_empty() {
      match self.expect {
        None => {
          // Accumulate ONLY the 4 header bytes of the trailing frame, then validate the declared
          // length before a single payload byte is accepted.
          let need = 4 - self.fill;
          let take = need.min(input.len());
          self.buf.extend_from_slice(&input[..take]);
          self.fill += take;
          input = &input[take..];
          if self.fill < 4 {
            return; // header still incomplete; wait for more bytes
          }
          let h = &self.buf[self.buf.len() - 4..];
          let len = u32::from_be_bytes([h[0], h[1], h[2], h[3]]) as usize;
          if len > MAX_FRAME_LEN {
            self.failed = true;
            self.buf = BytesMut::new();
            self.fill = 0;
            self.expect = None;
            return;
          }
          if len == 0 {
            // A zero-length frame is complete at its header — surface it now (the payload arm
            // below never runs when no payload bytes follow).
            self.fill = 0;
          } else {
            self.expect = Some(4 + len);
          }
        }
        Some(total) => {
          // The header is validated; accept payload bytes up to this frame's declared end.
          let need = total - self.fill;
          let take = need.min(input.len());
          self.buf.extend_from_slice(&input[..take]);
          self.fill += take;
          input = &input[take..];
          if self.fill == total {
            // Frame complete: it now sits among the complete frames; start reading the next header.
            self.fill = 0;
            self.expect = None;
          }
        }
      }
    }
  }

  /// Pop the next complete frame's payload as a zero-copy shared [`Bytes`] slice.
  ///
  /// Returns `Ok(Some(payload))` if a frame was produced, `Ok(None)` if more bytes are needed, and
  /// `Err(FrameTooLarge)` once the decoder has latched the terminal oversized-frame failure.
  pub(crate) fn poll(&mut self) -> Result<Option<Bytes>, TransportError> {
    if self.failed {
      return Err(TransportError::FrameTooLarge);
    }
    // Complete frames precede the trailing partial one (`fill` bytes at the tail).
    let complete = self.buf.len() - self.fill;
    if complete < 4 {
      return Ok(None);
    }
    let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
    debug_assert!(len <= MAX_FRAME_LEN, "headers are validated at push time");
    let end = 4 + len;
    if complete < end {
      return Ok(None);
    }
    // O(1): split the frame off the front (pointer arithmetic, no memmove) and drop the header.
    let frame = self.buf.split_to(end).freeze();
    Ok(Some(frame.slice(4..)))
  }

  /// Test-only: whether the decoder has latched the terminal oversized-frame failure.
  #[cfg(test)]
  pub(crate) fn is_failed_for_test(&self) -> bool {
    self.failed
  }

  /// Test-only: the decoder's current buffered byte count (to assert rejected payloads are never
  /// retained).
  #[cfg(test)]
  pub(crate) fn buffered_for_test(&self) -> usize {
    self.buf.len()
  }
}

#[cfg(test)]
mod tests;
