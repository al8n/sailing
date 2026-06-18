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

/// A synchronized cluster-epoch WALL reading (nanos since a cluster-wide epoch). A DISTINCT KIND
/// from the per-node monotonic [`Instant`] — never derived from `Instant::since_origin()`, never
/// compared against it. [`Wall::ABSENT`] (0) is the sentinel: outside the LeaseGuard failover tier,
/// and the fail-closed value if a failover caller forgets to supply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Wall(u64);

impl Wall {
  /// The sentinel "no synchronized wall supplied" value (fail-closed-to-Safe on the read path).
  pub const ABSENT: Self = Self(0);

  /// A synchronized wall reading from nanos since the cluster epoch.
  #[inline(always)]
  pub const fn from_nanos(nanos: u64) -> Self {
    Self(nanos)
  }

  /// The reading in nanos since the cluster epoch (`0` when absent).
  #[inline(always)]
  pub const fn as_nanos(self) -> u64 {
    self.0
  }

  /// Whether no synchronized wall was supplied.
  #[inline(always)]
  pub const fn is_absent(self) -> bool {
    self.0 == 0
  }
}

/// The clock input the driver supplies on every call: the monotonic [`Instant`] ALWAYS, plus the
/// synchronized [`Wall`] WHEN the cluster runs the LeaseGuard failover tier. Deliberately carries NO
/// `Ord`/`Add` — callers must reach [`Now::mono`] for any time arithmetic, so the two clock kinds
/// can never be mixed by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Now {
  mono: Instant,
  wall: Wall,
}

impl Now {
  /// Monotonic-only (wall [`Wall::ABSENT`]). For every non-failover call site.
  #[inline(always)]
  pub const fn monotonic(mono: Instant) -> Self {
    Self {
      mono,
      wall: Wall::ABSENT,
    }
  }

  /// The full reading WITH the synchronized wall — the ONLY constructor that yields a present wall.
  #[inline(always)]
  pub const fn synchronized(mono: Instant, wall: Wall) -> Self {
    Self { mono, wall }
  }

  /// The monotonic instant.
  #[inline(always)]
  pub const fn mono(self) -> Instant {
    self.mono
  }

  /// The synchronized wall reading ([`Wall::ABSENT`] outside the failover tier).
  #[inline(always)]
  pub const fn wall(self) -> Wall {
    self.wall
  }
}

/// Ergonomic bridge: a bare monotonic `Instant` is a `Now` with no synchronized wall. Lets every
/// existing `now: Instant` call site compile unchanged.
impl From<Instant> for Now {
  #[inline(always)]
  fn from(mono: Instant) -> Self {
    Self::monotonic(mono)
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

  #[test]
  fn now_split_and_default() {
    let i = Instant::from_origin(Duration::from_nanos(7));
    // A bare Instant is a wall-absent Now (the auto-convert existing call sites rely on).
    let n: Now = i.into();
    assert_eq!(n.mono(), i);
    assert!(n.wall().is_absent());
    // Only `synchronized` yields a present wall.
    let s = Now::synchronized(i, Wall::from_nanos(42));
    assert_eq!(s.mono(), i);
    assert_eq!(s.wall().as_nanos(), 42);
    assert!(!s.wall().is_absent());
    // LOAD-BEARING: a synchronized Now with an ABSENT wall is byte-identical to a monotonic Now. The
    // sailing-compio `Clock` relies on this to build `Now::synchronized(mono, src.now())`
    // unconditionally — its Monotonic source yields ABSENT, reproducing the monotonic-only path
    // exactly. A change that broke this (a nonzero ABSENT sentinel, an added presence flag) would
    // silently alter the driver's non-failover behavior.
    assert_eq!(Now::monotonic(i), Now::synchronized(i, Wall::ABSENT));
  }
}
