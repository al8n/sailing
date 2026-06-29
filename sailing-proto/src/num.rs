//! `#[repr(transparent)]` u64 counters. A `Term` and an `Index` are distinct types so
//! the two can never be transposed at a call site.

macro_rules! counter {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        #[repr(transparent)]
        pub struct $name(u64);

        impl $name {
            #[doc = "The zero value."]
            pub const ZERO: Self = Self(0);
            #[doc = "Wrap a raw `u64`."]
            #[inline(always)]
            pub const fn new(value: u64) -> Self { Self(value) }
            #[doc = "The raw `u64`."]
            #[inline(always)]
            pub const fn get(self) -> u64 { self.0 }
            #[doc = "The next value (saturating at `u64::MAX`)."]
            #[inline(always)]
            pub const fn next(self) -> Self { Self(self.0.saturating_add(1)) }
            #[doc = "The next value, or `None` at `u64::MAX` (the counter is exhausted)."]
            #[doc = ""]
            #[doc = "Use this — never `next()` — wherever the result is treated as a STRICTLY-new"]
            #[doc = "slot/term (e.g. allocating a fresh log index or advancing the election term):"]
            #[doc = "`next()` saturates, so at `u64::MAX` it would silently reuse the current value,"]
            #[doc = "letting a crafted/recovered max-value state alias an existing index/term."]
            #[inline(always)]
            pub const fn checked_next(self) -> Option<Self> {
                match self.0.checked_add(1) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }
        }

        impl core::fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                core::fmt::Display::fmt(&self.0, f)
            }
        }

        impl crate::Data for $name {
            #[inline]
            fn encode(&self, buf: &mut std::vec::Vec<u8>) {
                crate::Data::encode(&self.0, buf);
            }
            #[inline]
            fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, crate::DecodeError> {
                Ok(Self(<u64 as crate::Data>::decode(cur)?))
            }
        }
    };
}

counter!(
    /// A Raft election term — monotonically increasing across elections.
    Term
);
counter!(
    /// A 1-based position in the replicated log (index 0 means "before the first entry").
    Index
);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn counter_basics() {
    assert_eq!(Term::ZERO.get(), 0);
    assert_eq!(Term::new(5).get(), 5);
    assert_eq!(Term::new(5).next(), Term::new(6));
    assert!(Index::new(1) < Index::new(2));
    assert_eq!(Index::new(u64::MAX).next(), Index::new(u64::MAX)); // saturating
    // checked_next: strict advance, None at the ceiling.
    assert_eq!(Index::new(5).checked_next(), Some(Index::new(6)));
    assert_eq!(Index::new(u64::MAX).checked_next(), None);
    assert_eq!(Term::new(u64::MAX).checked_next(), None);
    assert_eq!(std::format!("{}", Term::new(7)), "7");
  }

  #[test]
  fn counter_data_roundtrip() {
    use crate::Data;
    use bytes::Bytes;
    // The `Data` impl is the fixed 8-byte little-endian `u64` codec; every value round-trips.
    for &v in &[0u64, 1, 42, u64::MAX] {
      let mut buf = std::vec::Vec::new();
      Term::new(v).encode(&mut buf);
      assert_eq!(
        buf.len(),
        8,
        "a counter encodes as exactly one u64 (8 bytes LE)"
      );
      assert_eq!(Term::decode_exact(Bytes::from(buf)).unwrap(), Term::new(v));
    }
    let mut buf = std::vec::Vec::new();
    Index::new(0x0102_0304_0506_0708).encode(&mut buf);
    assert_eq!(
      Index::decode_exact(Bytes::from(buf)).unwrap(),
      Index::new(0x0102_0304_0506_0708)
    );
  }
}
