//! Length-prefixed framing: `[u32 big-endian length][payload]`, with a bounded decoder.
use super::TransportError;
use std::vec::Vec;

/// The largest single frame payload accepted (64 MiB). A length prefix above this is rejected
/// rather than buffered, bounding a connection's decode memory.
///
/// NOTE: an `InstallSnapshot` blob larger than this cannot ride a single frame; chunked snapshot
/// transfer is out of scope for this transport layer (see the design spec).
pub(crate) const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Append a length-prefixed frame (`[u32 BE len][payload]`) to `out`.
pub(crate) fn encode_frame(payload: &[u8], out: &mut Vec<u8>) {
  out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
  out.extend_from_slice(payload);
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
pub(crate) struct FrameDecoder {
  /// Complete frames awaiting [`poll`](Self::poll), then the bytes of the trailing partial frame.
  buf: Vec<u8>,
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
      buf: Vec::new(),
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
            self.buf = Vec::new();
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

  /// Pop the next complete frame's payload into `out` (which is cleared first).
  ///
  /// Returns `Ok(true)` if a frame was produced, `Ok(false)` if more bytes are needed, and
  /// `Err(FrameTooLarge)` once the decoder has latched the terminal oversized-frame failure.
  pub(crate) fn poll(&mut self, out: &mut Vec<u8>) -> Result<bool, TransportError> {
    if self.failed {
      return Err(TransportError::FrameTooLarge);
    }
    // Only complete frames precede the trailing partial one (`fill` bytes at the tail).
    let complete = self.buf.len() - self.fill;
    if complete < 4 {
      return Ok(false);
    }
    let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
    debug_assert!(len <= MAX_FRAME_LEN, "headers are validated at push time");
    let end = 4 + len;
    if complete < end {
      return Ok(false);
    }
    out.clear();
    out.extend_from_slice(&self.buf[4..end]);
    self.buf.drain(..end);
    Ok(true)
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
