//! `Data`: the per-element wire codec for the generic plug-in types. Values are
//! length-known and self-describing enough to nest inside a `bytes` field. The codec
//! here is deliberately minimal (fixed/var width primitives).
//!
//! Decoding is ZERO-COPY for `Bytes` fields: [`Data::decode`] reads from a [`ByteCursor`] over a
//! shared [`bytes::Bytes`], so a `Bytes` field decodes as an O(1) refcount slice of the input
//! buffer rather than a fresh allocation + memcpy. Entry payloads, snapshot blobs, and read
//! contexts therefore share the frame's allocation end-to-end. The flip side (deliberate, the
//! standard `bytes` trade-off): a decoded slice keeps the WHOLE backing buffer alive — callers
//! that retain a tiny slice of a large frame for a long time pin the frame's allocation.
use bytes::Bytes;
use std::{collections::BTreeSet, vec::Vec};

/// A read cursor over a shared [`Bytes`] buffer.
///
/// Each read bounds-checks against the remaining input and advances past exactly the bytes
/// consumed; [`take_bytes`](Self::take_bytes) hands out an O(1) shared slice (no copy). Decoders
/// cannot read past their input or miscount offsets — the cursor owns the position.
pub struct ByteCursor {
  buf: Bytes,
  pos: usize,
}

impl ByteCursor {
  /// Start decoding at the front of `buf`.
  #[inline]
  pub fn new(buf: Bytes) -> Self {
    Self { buf, pos: 0 }
  }

  /// Bytes not yet consumed.
  #[inline]
  pub fn remaining(&self) -> usize {
    self.buf.len() - self.pos
  }

  /// Whether the input is fully consumed.
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.remaining() == 0
  }

  /// Consume exactly `N` bytes as a fixed-size array (the primitive-width read).
  #[inline]
  pub(crate) fn take_array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
    let end = self.pos.checked_add(N).ok_or(DecodeError::UnexpectedEof)?;
    let slice = self
      .buf
      .get(self.pos..end)
      .ok_or(DecodeError::UnexpectedEof)?;
    let arr: [u8; N] = slice.try_into().map_err(|_| DecodeError::UnexpectedEof)?;
    self.pos = end;
    Ok(arr)
  }

  /// Consume one byte.
  #[inline]
  pub(crate) fn take_u8(&mut self) -> Result<u8, DecodeError> {
    let b = *self.buf.get(self.pos).ok_or(DecodeError::UnexpectedEof)?;
    self.pos += 1;
    Ok(b)
  }

  /// Consume exactly `len` bytes as a shared, zero-copy slice of the underlying buffer.
  #[inline]
  pub fn take_bytes(&mut self, len: usize) -> Result<Bytes, DecodeError> {
    let end = self
      .pos
      .checked_add(len)
      .ok_or(DecodeError::UnexpectedEof)?;
    if end > self.buf.len() {
      return Err(DecodeError::UnexpectedEof);
    }
    let out = self.buf.slice(self.pos..end);
    self.pos = end;
    Ok(out)
  }
}

/// A value that can be encoded to and decoded from bytes.
pub trait Data: Sized {
  /// Append the encoding of `self` to `buf`.
  fn encode(&self, buf: &mut Vec<u8>);
  /// Decode a value from the cursor, advancing it past exactly the bytes consumed.
  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError>;

  /// Decode a value that must occupy the WHOLE buffer — trailing bytes are an error.
  ///
  /// The cursor decoder above is right for reading through a struct's fields; this is the form
  /// for a self-contained payload (an entry's command, a snapshot blob, a conf-change record),
  /// where trailing garbage means the payload is malformed — accepting it would let two distinct
  /// byte strings decode to the same value (non-canonical input).
  fn decode_exact(buf: Bytes) -> Result<Self, DecodeError> {
    let mut cur = ByteCursor::new(buf);
    let value = Self::decode(&mut cur)?;
    if !cur.is_empty() {
      return Err(DecodeError::Invalid("trailing bytes after value"));
    }
    Ok(value)
  }
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

/// Decode a `u64` length/count prefix and narrow it to `usize` — the single safe conversion point for
/// every length-prefixed decode. A length that exceeds `usize::MAX` (only reachable on a sub-64-bit
/// target) is rejected as `Invalid(what)` rather than silently truncated by `as usize`: truncation
/// (e.g. `2^32` → `0` on a 32-bit target) would let an oversized prefix decode as a *different,
/// shorter* value instead of failing. All collection/bytes decoders MUST route their length through
/// here so the bound cannot regress per-site.
pub(crate) fn decode_len(cur: &mut ByteCursor, what: &'static str) -> Result<usize, DecodeError> {
  let raw = u64::decode(cur)?;
  usize::try_from(raw).map_err(|_| DecodeError::Invalid(what))
}

impl Data for u64 {
  #[inline]
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&self.to_le_bytes());
  }
  #[inline]
  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    Ok(u64::from_le_bytes(cur.take_array::<8>()?))
  }
}

impl Data for bool {
  #[inline]
  fn encode(&self, buf: &mut Vec<u8>) {
    buf.push(*self as u8);
  }
  #[inline]
  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    match cur.take_u8()? {
      0 => Ok(false),
      1 => Ok(true),
      _ => Err(DecodeError::Invalid("bool")),
    }
  }
}

impl Data for () {
  #[inline(always)]
  fn encode(&self, _buf: &mut Vec<u8>) {}
  #[inline(always)]
  fn decode(_cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    Ok(())
  }
}

impl Data for Bytes {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    buf.extend_from_slice(self);
  }

  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    let len = decode_len(cur, "bytes length")?;
    // Zero-copy: an O(1) shared slice of the input buffer (bounds-checked by the cursor).
    cur.take_bytes(len)
  }
}

impl<T: Data> Data for Vec<T> {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    for item in self {
      item.encode(buf);
    }
  }

  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    let len = decode_len(cur, "vec length")?;
    // Do NOT pre-allocate `len` (untrusted): push only what decodes, so an oversized
    // count fails on the first missing element instead of reserving huge memory.
    let mut items = Vec::new();
    for _ in 0..len {
      let before = cur.remaining();
      let item = T::decode(cur)?;
      // Every wire element must consume input — else a huge count of a zero-width element type would
      // spin `len` times (attacker-controlled work) from only the count prefix.
      if cur.remaining() == before {
        return Err(DecodeError::Invalid("zero-width vec element"));
      }
      items.push(item);
    }
    Ok(items)
  }
}

impl<T: Data + Ord> Data for BTreeSet<T> {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    for item in self {
      item.encode(buf);
    }
  }

  fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
    let len = decode_len(cur, "set length")?;
    let mut set = BTreeSet::new();
    for _ in 0..len {
      let before = cur.remaining();
      let elem = T::decode(cur)?;
      if cur.remaining() == before {
        return Err(DecodeError::Invalid("zero-width set element"));
      }
      // Require strictly ascending elements — the unique, canonical wire form of a set. This rejects
      // duplicates and re-orderings, so distinct hostile byte strings cannot decode to the same set
      // (the encode/decode round-trip stays canonical, which `ConfState` snapshot metadata relies on).
      if set.last().is_some_and(|max| elem <= *max) {
        return Err(DecodeError::Invalid(
          "set elements must be strictly ascending",
        ));
      }
      set.insert(elem);
    }
    Ok(set)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::vec::Vec;

  fn roundtrip<T: Data + PartialEq + core::fmt::Debug>(v: T) {
    let mut buf = Vec::new();
    v.encode(&mut buf);
    let decoded = T::decode_exact(Bytes::from(buf)).expect("decode");
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
  fn bytes_roundtrips_zero_copy() {
    let mut buf = std::vec::Vec::new();
    let b = Bytes::from_static(b"hello");
    b.encode(&mut buf);
    let input = Bytes::from(buf);
    let mut cur = ByteCursor::new(input.clone());
    let back = Bytes::decode(&mut cur).unwrap();
    assert!(cur.is_empty());
    assert_eq!(back, b);
    // ZERO-COPY: the decoded Bytes is a slice of the INPUT allocation, not a fresh copy.
    assert_eq!(
      back.as_ptr(),
      input[8..].as_ptr(),
      "the decoded payload must share the input buffer"
    );
  }

  /// A length prefix larger than the buffer can possibly satisfy must DECODE TO AN ERROR — never a
  /// truncated/empty `Bytes`. A `u64::MAX` prefix followed by only a few payload bytes must be
  /// rejected, not silently yield some shorter value — the security property is "oversized →
  /// error, never wrong data".
  #[test]
  fn bytes_decode_rejects_oversized_length() {
    // u64::MAX length prefix (little-endian), then only 3 payload bytes — nowhere near enough.
    let mut buf = Vec::new();
    u64::MAX.encode(&mut buf);
    buf.extend_from_slice(b"abc");
    let res = Bytes::decode_exact(Bytes::from(buf));
    assert!(
      res.is_err(),
      "an oversized length prefix must error, not truncate: got {res:?}"
    );

    // A merely "too large for this buffer" length (fits in usize, exceeds the available bytes) must
    // also error rather than read past the end.
    let mut buf2 = Vec::new();
    1_000_000u64.encode(&mut buf2);
    buf2.extend_from_slice(b"only-a-few");
    assert!(
      Bytes::decode_exact(Bytes::from(buf2)).is_err(),
      "a length exceeding the buffer must error, never return a short/empty Bytes"
    );
  }

  /// [`decode_len`] is the single safe conversion point every length-prefixed decode routes its
  /// u64 length through: a normal length round-trips (consuming the 8-byte prefix and narrowing
  /// to `usize`), matching what the `Bytes` encoder wrote.
  #[test]
  fn decode_len_roundtrips_and_is_used_by_bytes_decoder() {
    let mut buf = Vec::new();
    1234u64.encode(&mut buf);
    let mut cur = ByteCursor::new(Bytes::from(buf));
    let len = decode_len(&mut cur, "test length").expect("normal length narrows");
    assert!(cur.is_empty(), "the u64 length prefix is 8 bytes");
    assert_eq!(len, 1234usize);

    // Cross-check the helper IS the one the Bytes decoder uses: encode a real Bytes, then the prefix
    // `decode_len` reads off the front must equal that payload's length.
    let payload = Bytes::from_static(b"hello, world");
    let mut encoded = Vec::new();
    payload.encode(&mut encoded);
    let mut cur = ByteCursor::new(Bytes::from(encoded));
    let plen = decode_len(&mut cur, "bytes length").expect("bytes prefix narrows");
    assert_eq!(plen, payload.len());
    assert_eq!(cur.remaining(), payload.len());
  }

  /// A `Vec<T>` of a zero-width element type must not spin `len` times from only the count prefix —
  /// each element is required to consume input.
  #[test]
  fn vec_decode_rejects_zero_width_element_spin() {
    let mut buf = Vec::new();
    1_000_000u64.encode(&mut buf); // claim a million () elements, with no following bytes
    assert!(
      <Vec<()>>::decode_exact(Bytes::from(buf)).is_err(),
      "a zero-width element type must not be decodable in bulk from just the count"
    );
    // An empty Vec<()> (count 0) is still fine.
    let mut empty = Vec::new();
    0u64.encode(&mut empty);
    assert_eq!(
      <Vec<()>>::decode_exact(Bytes::from(empty)).unwrap(),
      std::vec![]
    );
  }

  /// A `BTreeSet` decode is canonical: it rejects duplicate and non-ascending elements, so distinct
  /// hostile byte strings cannot decode to the same set.
  #[test]
  fn btreeset_decode_requires_strictly_ascending() {
    // count = 2 with a duplicate element.
    let mut dup = Vec::new();
    2u64.encode(&mut dup);
    7u64.encode(&mut dup);
    7u64.encode(&mut dup);
    assert!(
      <BTreeSet<u64>>::decode_exact(Bytes::from(dup)).is_err(),
      "a duplicate set element must be rejected, not silently collapsed"
    );
    // count = 2 in descending order.
    let mut desc = Vec::new();
    2u64.encode(&mut desc);
    9u64.encode(&mut desc);
    3u64.encode(&mut desc);
    assert!(
      <BTreeSet<u64>>::decode_exact(Bytes::from(desc)).is_err(),
      "non-ascending set elements must be rejected"
    );
    // A canonical ascending set round-trips.
    let mut ok = Vec::new();
    3u64.encode(&mut ok);
    1u64.encode(&mut ok);
    5u64.encode(&mut ok);
    9u64.encode(&mut ok);
    let set = <BTreeSet<u64>>::decode_exact(Bytes::from(ok)).expect("ascending set decodes");
    assert_eq!(set, BTreeSet::from([1, 5, 9]));
  }
}
