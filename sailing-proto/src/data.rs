//! `Data`: the per-element wire codec for the generic plug-in types. Values are
//! length-known and self-describing enough to nest inside a `bytes` field. The codec
//! here is deliberately minimal (fixed/var width primitives).
use std::{collections::BTreeSet, vec::Vec};

/// A value that can be encoded to and decoded from bytes.
pub trait Data: Sized {
  /// Append the encoding of `self` to `buf`.
  fn encode(&self, buf: &mut Vec<u8>);
  /// Decode a value from the front of `buf`, returning `(bytes_consumed, value)`.
  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError>;
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
/// every length-prefixed decode. Returns `(bytes_consumed, len)`. A length that exceeds `usize::MAX`
/// (only reachable on a sub-64-bit target) is rejected as `Invalid(what)` rather than silently
/// truncated by `as usize`: truncation (e.g. `2^32` → `0` on a 32-bit target) would let an oversized
/// prefix decode as a *different, shorter* value instead of failing. All collection/bytes decoders
/// MUST route their length through here so the bound cannot regress per-site.
pub(crate) fn decode_len(buf: &[u8], what: &'static str) -> Result<(usize, usize), DecodeError> {
  let (n, raw) = u64::decode(buf)?;
  let len = usize::try_from(raw).map_err(|_| DecodeError::Invalid(what))?;
  Ok((n, len))
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
    let (n, len) = decode_len(buf, "bytes length")?;
    let end = n.checked_add(len).ok_or(DecodeError::UnexpectedEof)?;
    let slice = buf.get(n..end).ok_or(DecodeError::UnexpectedEof)?;
    Ok((end, bytes::Bytes::copy_from_slice(slice)))
  }
}

/// A forward cursor for decoding a sequence of `Data` fields from one buffer.
///
/// Each [`read`](Self::read) bounds-checks against the remaining input and advances by exactly the
/// bytes the field consumed, so a struct decoder cannot read past its input or miscount offsets.
pub(crate) struct Decoder<'a> {
  buf: &'a [u8],
  pos: usize,
}

impl<'a> Decoder<'a> {
  /// Start decoding at the front of `buf`.
  #[inline]
  pub(crate) fn new(buf: &'a [u8]) -> Self {
    Self { buf, pos: 0 }
  }

  /// Decode the next `T`, advancing past it. Errors if the input is exhausted.
  #[inline]
  pub(crate) fn read<T: Data>(&mut self) -> Result<T, DecodeError> {
    let rest = self.buf.get(self.pos..).ok_or(DecodeError::UnexpectedEof)?;
    let (n, value) = T::decode(rest)?;
    self.pos += n;
    Ok(value)
  }

  /// Total bytes consumed so far — the decoded length of the value.
  #[inline]
  pub(crate) fn pos(&self) -> usize {
    self.pos
  }
}

impl<T: Data> Data for Vec<T> {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    for item in self {
      item.encode(buf);
    }
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let (prefix, len) = decode_len(buf, "vec length")?;
    let rest = buf.get(prefix..).ok_or(DecodeError::UnexpectedEof)?;
    let mut d = Decoder::new(rest);
    // Do NOT pre-allocate `len` (untrusted): push only what decodes, so an oversized
    // count fails on the first missing element instead of reserving huge memory.
    let mut items = Vec::new();
    for _ in 0..len {
      let before = d.pos();
      let item = d.read::<T>()?;
      // Every wire element must consume input — else a huge count of a zero-width element type would
      // spin `len` times (attacker-controlled work) from only the count prefix.
      if d.pos() == before {
        return Err(DecodeError::Invalid("zero-width vec element"));
      }
      items.push(item);
    }
    Ok((prefix + d.pos(), items))
  }
}

impl<T: Data + Ord> Data for BTreeSet<T> {
  fn encode(&self, buf: &mut Vec<u8>) {
    (self.len() as u64).encode(buf);
    for item in self {
      item.encode(buf);
    }
  }

  fn decode(buf: &[u8]) -> Result<(usize, Self), DecodeError> {
    let (prefix, len) = decode_len(buf, "set length")?;
    let rest = buf.get(prefix..).ok_or(DecodeError::UnexpectedEof)?;
    let mut d = Decoder::new(rest);
    let mut set = BTreeSet::new();
    for _ in 0..len {
      let before = d.pos();
      let elem = d.read::<T>()?;
      if d.pos() == before {
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
    Ok((prefix + d.pos(), set))
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

  /// A length prefix larger than the buffer can possibly satisfy must DECODE TO AN ERROR — never a
  /// truncated/empty `Bytes`. A `u64::MAX` prefix followed by only a few payload bytes must be
  /// rejected, not silently yield some shorter value — the security property is "oversized →
  /// error, never wrong data".
  ///
  /// NOTE: the `usize::try_from` rejection inside [`decode_len`] is only *reachable* on a sub-64-bit
  /// target. On this 64-bit host `usize::MAX == u64::MAX`, so `try_from` succeeds and the oversized
  /// length is instead caught by the subsequent `checked_add` / slice-bound (`buf.get`) check in
  /// `<Bytes as Data>::decode`. Either way the contract holds: an oversized prefix can never decode
  /// as a different, shorter value — it always fails.
  #[test]
  fn bytes_decode_rejects_oversized_length() {
    // u64::MAX length prefix (little-endian), then only 3 payload bytes — nowhere near enough.
    let mut buf = Vec::new();
    u64::MAX.encode(&mut buf);
    buf.extend_from_slice(b"abc");
    let res = <bytes::Bytes as Data>::decode(&buf);
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
      <bytes::Bytes as Data>::decode(&buf2).is_err(),
      "a length exceeding the buffer must error, never return a short/empty Bytes"
    );
  }

  /// [`decode_len`] is the single safe conversion point both `Bytes::decode` and `ConfChangeV2::decode`
  /// route their u64 length through. A normal length round-trips (consuming the 8-byte prefix and
  /// narrowing to `usize`); this also pins that the helper is in fact what the `Bytes` decoder uses —
  /// the `(n, len)` it reports for a real encoding matches the prefix the encoder wrote.
  #[test]
  fn decode_len_roundtrips_and_is_used_by_bytes_decoder() {
    // Direct: a normal length narrows cleanly and reports the 8 prefix bytes consumed.
    let mut buf = Vec::new();
    1234u64.encode(&mut buf);
    let (n, len) = decode_len(&buf, "test length").expect("normal length narrows");
    assert_eq!(n, 8, "the u64 length prefix is 8 bytes");
    assert_eq!(len, 1234usize);

    // Cross-check the helper IS the one the Bytes decoder uses: encode a real Bytes, then the prefix
    // `decode_len` reads off the front must equal that payload's length and the same 8-byte consume.
    let payload = bytes::Bytes::from_static(b"hello, world");
    let mut encoded = Vec::new();
    payload.encode(&mut encoded);
    let (pn, plen) = decode_len(&encoded, "bytes length").expect("bytes prefix narrows");
    assert_eq!(pn, 8);
    assert_eq!(plen, payload.len());
  }

  /// A `Vec<T>` of a zero-width element type must not spin `len` times from only the count prefix —
  /// each element is required to consume input.
  #[test]
  fn vec_decode_rejects_zero_width_element_spin() {
    let mut buf = Vec::new();
    1_000_000u64.encode(&mut buf); // claim a million () elements, with no following bytes
    assert!(
      <Vec<()> as Data>::decode(&buf).is_err(),
      "a zero-width element type must not be decodable in bulk from just the count"
    );
    // An empty Vec<()> (count 0) is still fine.
    let mut empty = Vec::new();
    0u64.encode(&mut empty);
    assert_eq!(<Vec<()> as Data>::decode(&empty).unwrap().1, std::vec![]);
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
      <BTreeSet<u64> as Data>::decode(&dup).is_err(),
      "a duplicate set element must be rejected, not silently collapsed"
    );
    // count = 2 in descending order.
    let mut desc = Vec::new();
    2u64.encode(&mut desc);
    9u64.encode(&mut desc);
    3u64.encode(&mut desc);
    assert!(
      <BTreeSet<u64> as Data>::decode(&desc).is_err(),
      "non-ascending set elements must be rejected"
    );
    // A canonical ascending set round-trips.
    let mut ok = Vec::new();
    3u64.encode(&mut ok);
    1u64.encode(&mut ok);
    5u64.encode(&mut ok);
    9u64.encode(&mut ok);
    let (n, set) = <BTreeSet<u64> as Data>::decode(&ok).expect("ascending set decodes");
    assert_eq!(n, ok.len());
    assert_eq!(set, BTreeSet::from([1, 5, 9]));
  }
}
