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
/// The buffer only ever holds the bytes of the frame currently being reassembled (completed frames
/// are drained on [`poll`](Self::poll)), and a length prefix above [`MAX_FRAME_LEN`] is rejected
/// before any payload is buffered — so a peer cannot drive unbounded memory growth.
pub(crate) struct FrameDecoder {
  buf: Vec<u8>,
  /// Latched once an oversized length prefix is seen; the stream is terminally broken.
  failed: bool,
}

impl FrameDecoder {
  /// A decoder with an empty buffer.
  pub(crate) fn new() -> Self {
    Self {
      buf: Vec::new(),
      failed: false,
    }
  }

  /// The declared payload length of the frame at the front of the buffer, once the 4-byte header
  /// has arrived.
  fn declared_len(&self) -> Option<usize> {
    (self.buf.len() >= 4)
      .then(|| u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize)
  }

  /// Feed received bytes (any chunking) into the decoder. As soon as the length header reveals a
  /// frame larger than [`MAX_FRAME_LEN`], the decoder latches failed and frees its buffer — so an
  /// oversized declared length is never accumulated across calls.
  pub(crate) fn push(&mut self, bytes: &[u8]) {
    if self.failed {
      return;
    }
    self.buf.extend_from_slice(bytes);
    if self.declared_len().is_some_and(|len| len > MAX_FRAME_LEN) {
      self.failed = true;
      self.buf = Vec::new();
    }
  }

  /// Pop the next complete frame's payload into `out` (which is cleared first).
  ///
  /// Returns `Ok(true)` if a frame was produced, `Ok(false)` if more bytes are needed, and
  /// `Err(FrameTooLarge)` if the length prefix exceeds [`MAX_FRAME_LEN`] (a terminal stream error;
  /// the caller closes the connection).
  pub(crate) fn poll(&mut self, out: &mut Vec<u8>) -> Result<bool, TransportError> {
    if self.failed {
      return Err(TransportError::FrameTooLarge);
    }
    let Some(len) = self.declared_len() else {
      return Ok(false);
    };
    if len > MAX_FRAME_LEN {
      self.failed = true;
      self.buf = Vec::new();
      return Err(TransportError::FrameTooLarge);
    }
    let end = 4 + len;
    if self.buf.len() < end {
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
}

#[cfg(test)]
mod tests;
