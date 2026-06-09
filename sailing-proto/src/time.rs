//! A portable monotonic instant: a `Duration` since an opaque per-process origin the
//! driver chooses at startup. The core never reads a clock — `now` is always supplied.
use core::time::Duration;

/// A monotonic instant, portable across std / embassy / any runtime clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Instant(Duration);

impl Instant {
  /// The origin instant (zero elapsed).
  pub const ORIGIN: Self = Self(Duration::ZERO);

  /// Construct from a `Duration` elapsed since the driver's origin.
  #[inline(always)]
  pub const fn from_origin(elapsed: Duration) -> Self {
    Self(elapsed)
  }

  /// The elapsed `Duration` since the origin.
  #[inline(always)]
  pub const fn since_origin(self) -> Duration {
    self.0
  }

  /// Saturating elapsed time since `earlier` (never panics on out-of-order input).
  #[inline(always)]
  pub fn duration_since(self, earlier: Self) -> Duration {
    self.0.saturating_sub(earlier.0)
  }
}

impl core::ops::Add<Duration> for Instant {
  type Output = Self;
  #[inline(always)]
  fn add(self, d: Duration) -> Self {
    Self(self.0.saturating_add(d))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::time::Duration;

  #[test]
  fn instant_arithmetic_saturates() {
    let o = Instant::ORIGIN;
    let t = o + Duration::from_millis(150);
    assert_eq!(t.duration_since(o), Duration::from_millis(150));
    // out-of-order subtraction saturates to zero, never panics
    assert_eq!(o.duration_since(t), Duration::ZERO);
    assert!(t > o);
  }
}
