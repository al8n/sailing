//! `Data`/`DataRef`: the per-element wire codec for the generic plug-in types. Values are
//! length-known and self-describing enough to nest inside a buffa `bytes` field. The codec
//! here is deliberately minimal (fixed/var width primitives); message envelopes use buffa.
use std::vec::Vec;

/// A value that can be encoded to and decoded from bytes.
pub trait Data: Sized {
  /// Append the encoding of `self` to `buf`.
  fn encode(&self, buf: &mut Vec<u8>);
  /// Decode a value from the front of `buf`, returning `(bytes_consumed, value)`.
  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError>;
}

/// A zero-copy borrowed view decoded from a wire buffer (the GAT-free M0 form;
/// promoted to a `Data::Ref<'a>` GAT when zero-copy message views are added).
pub trait DataRef<'a>: Sized {
  /// Decode a borrowed view from the front of `buf`.
  fn decode_ref(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>;
}

/// Wire-decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
  /// The buffer ended before a complete value was read.
  #[error("unexpected end of buffer")]
  UnexpectedEof,
  /// A field held a value outside its valid domain.
  #[error("invalid value for {0}")]
  Invalid(&'static str),
}

impl Data for u64 {
  #[inline]
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&self.to_le_bytes());
  }
  #[inline]
  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let bytes: [u8; 8] = buf
      .get(..8)
      .ok_or(DecodeError::UnexpectedEof)?
      .try_into()
      .map_err(|_| DecodeError::UnexpectedEof)?;
    Ok((8, u64::from_le_bytes(bytes)))
  }
}

impl Data for bool {
  #[inline]
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.push(*self as u8);
  }
  #[inline]
  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    match buf.first() {
      Some(0) => Ok((1, false)),
      Some(1) => Ok((1, true)),
      Some(_) => Err(DecodeError::Invalid("bool")),
      None => Err(DecodeError::UnexpectedEof),
    }
  }
}

impl Data for () {
  #[inline(always)]
  fn encode(&self, _buf: &mut Vec<u8>) {}
  #[inline(always)]
  fn decode(_buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    Ok((0, ()))
  }
}

impl Data for bytes::Bytes {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    buf.extend_from_slice(self);
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let (n, len) = u64::decode(buf)?;
    let end = n
      .checked_add(len as usize)
      .ok_or(DecodeError::UnexpectedEof)?;
    let slice = buf.get(n..end).ok_or(DecodeError::UnexpectedEof)?;
    Ok((end, bytes::Bytes::copy_from_slice(slice)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::vec::Vec;

  fn roundtrip<T: Data + PartialEq + core::fmt::Debug>(v: T) {
    let mut buf = Vec::new();
    v.encode(&mut buf);
    let (read, decoded) = T::decode(&buf).expect("decode");
    assert_eq!(read, buf.len());
    assert_eq!(decoded, v);
  }

  #[test]
  fn unit_roundtrip() {
    roundtrip(());
  }

  #[test]
  fn primitive_roundtrips() {
    roundtrip(0u64);
    roundtrip(u64::MAX);
    roundtrip(true);
    roundtrip(false);
  }

  #[test]
  fn bytes_roundtrips() {
    let mut buf = std::vec::Vec::new();
    let b = bytes::Bytes::from_static(b"hello");
    b.encode(&mut buf);
    let (n, back) = bytes::Bytes::decode(&buf).unwrap();
    assert_eq!(n, buf.len());
    assert_eq!(back, b);
  }
}
