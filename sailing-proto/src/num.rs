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
        }

        impl core::fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                core::fmt::Display::fmt(&self.0, f)
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
    assert_eq!(std::format!("{}", Term::new(7)), "7");
  }
}
