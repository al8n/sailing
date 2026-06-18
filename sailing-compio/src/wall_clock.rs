//! The synchronized wall-clock seam for the LeaseGuard failover tier.
//!
//! The driver `Clock<W>` owns one `W: WallClock` (a generic type parameter, default [`Monotonic`]) and
//! reads it once per wake. A source reports a RAW [`WallReading`] — its measured wall plus its OWN
//! worst-case error — and NEVER sees ε_unc: the `Clock` alone gates the reading against the cluster
//! ε_unc (from the proto `Config`), so the one safety threshold lives in exactly one place. Outside the
//! failover tier ε_unc is `0`, so any reading over-bounds and the wall is [`Wall::ABSENT`](sailing_proto::Wall::ABSENT) — the driver
//! is byte-identical to monotonic-only and the proto's failover paths stay inert.

/// A raw synchronized-wall reading: a source's measured wall and the source's OWN worst-case error
/// estimate, both in NANOSECONDS (the wall since the cluster epoch). The source converts its native
/// units (e.g. adjtimex signed µs) to ns HERE; the driver `Clock` compares
/// [`max_error_nanos`](Self::max_error_nanos) to the cluster ε_unc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WallReading {
  wall_nanos: u64,
  max_error_nanos: u64,
}

impl WallReading {
  /// A reading of `wall_nanos` (nanos since the cluster epoch) with a worst-case error of
  /// `max_error_nanos` (nanos).
  #[inline]
  #[must_use]
  pub const fn new(wall_nanos: u64, max_error_nanos: u64) -> Self {
    Self {
      wall_nanos,
      max_error_nanos,
    }
  }

  /// The measured wall, in nanoseconds since the cluster epoch.
  #[inline]
  #[must_use]
  pub const fn wall_nanos(&self) -> u64 {
    self.wall_nanos
  }

  /// The source's worst-case error estimate, in nanoseconds.
  #[inline]
  #[must_use]
  pub const fn max_error_nanos(&self) -> u64 {
    self.max_error_nanos
  }
}

/// A source of the synchronized cluster-epoch wall clock for the LeaseGuard failover tier, supplied as
/// the driver's `W` type parameter (default [`Monotonic`]).
///
/// CONTRACT: a `Some(reading)` ASSERTS that `reading.max_error_nanos()` is an HONEST upper bound on
/// `|W(t) − t|` for this node against the shared cluster epoch. The source NEVER decides whether that
/// error fits the cluster bound — the driver `Clock` gates it against the one ε_unc the proto `Config`
/// carries. Return `None` whenever the source cannot vouch for a reading at all (e.g. the kernel
/// reports the clock unsynchronized). The library cannot verify the estimate's honesty; a reading
/// whose true error exceeds the asserted bound can cause a stale read. Epoch + leap-policy agreement
/// across nodes is as load-bearing as ε_unc.
pub trait WallClock {
  /// Whether this source can ever supply a real synchronized wall. `false` for the monotonic default;
  /// the driver `bind` rejects a failover `Config` paired with a non-supplying source (see
  /// `BindError::MissingWallSource`). A startup PROMISE only — `None` from [`now`](Self::now) is the
  /// runtime truth and degrades to [`Wall::ABSENT`](sailing_proto::Wall::ABSENT) regardless.
  const SUPPLIES_WALL: bool;

  /// The current raw reading, or `None` if the source cannot vouch for one now. Read once per loop
  /// wake; `&mut self` lets a stateful source cache without interior mutability.
  fn now(&mut self) -> Option<WallReading>;
}

/// The default source: never supplies a wall. The failover tier stays inert and the driver behaves
/// byte-identically to a monotonic-only driver.
#[derive(Debug, Clone, Copy, Default)]
pub struct Monotonic;

impl WallClock for Monotonic {
  const SUPPLIES_WALL: bool = false;

  #[inline(always)]
  fn now(&mut self) -> Option<WallReading> {
    None
  }
}

/// `SystemTime::now()` as nanos since the Unix epoch, saturating into `u64` (a ~year-2554 ceiling).
#[cfg_attr(
  all(not(target_os = "linux"), not(feature = "unverified-wall-clock")),
  allow(dead_code)
)]
fn system_wall_nanos() -> u64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// adjtimex `maxerror` (signed MICROSECONDS) to nanoseconds. Clamp BEFORE the `*1000` so a
/// negative/huge value can never wrap. Isolated + unit-tested because a raw µs-vs-ns compare
/// downstream would be a 1000× fail-OPEN bug — here it is a pure unit normalization with NO threshold
/// in scope (the driver `Clock` applies ε_unc).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn maxerror_us_to_ns(maxerror_us: i64) -> u64 {
  (maxerror_us.max(0) as u64).saturating_mul(1_000)
}

/// The disciplined reading from a kernel `timex`, factored out so it is unit-testable WITHOUT a
/// syscall: `None` when unsynchronized, else a reading with the µs→ns-normalized error.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn disciplined_reading(status: i32, maxerror_us: i64, unsync_bit: i32) -> Option<WallReading> {
  if (status & unsync_bit) != 0 {
    return None;
  }
  Some(WallReading::new(
    system_wall_nanos(),
    maxerror_us_to_ns(maxerror_us),
  ))
}

/// The PRODUCTION wall source: reads the OS clock-discipline state (Linux `adjtimex`) and reports a
/// [`WallReading`] with the kernel's worst-case error, or `None` when the clock is unsynchronized
/// (`STA_UNSYNC`) or `adjtimex` errors. The driver `Clock` then degrades to [`Wall::ABSENT`](sailing_proto::Wall::ABSENT) when that
/// error exceeds ε_unc. On non-Linux targets (no `adjtimex` equivalent) it always returns `None` —
/// supply your own [`WallClock`] there.
///
/// A ZST — selected as the driver's `W` type parameter (passed by value to `bind_with_wall_clock`).
/// Selecting it does NOT enable failover by itself: you must ALSO set
/// `Config::bounded_clock_uncertainty`, else the tier is inert (the wall over-bounds against ε_unc 0).
#[derive(Debug, Clone, Copy, Default)]
pub struct NtpDisciplinedClock;

impl NtpDisciplinedClock {
  #[cfg(target_os = "linux")]
  fn read(&self) -> Option<WallReading> {
    // SAFETY: a read-only (modes = 0) adjtimex over a zeroed timex via a valid pointer; the kernel
    // reads our pointer and writes the struct, nothing more.
    let mut t: libc::timex = unsafe { core::mem::zeroed() };
    let ret = unsafe { libc::adjtimex(&mut t) };
    if ret < 0 || ret == libc::TIME_ERROR {
      return None;
    }
    disciplined_reading(t.status, t.maxerror as i64, libc::STA_UNSYNC)
  }

  #[cfg(not(target_os = "linux"))]
  fn read(&self) -> Option<WallReading> {
    None
  }
}

impl WallClock for NtpDisciplinedClock {
  const SUPPLIES_WALL: bool = true;

  fn now(&mut self) -> Option<WallReading> {
    self.read()
  }
}

/// A raw `SystemTime` source with NO discipline check — for TESTS and tightly-disciplined
/// single-region deployments ONLY, behind the non-default `unverified-wall-clock` feature so it cannot
/// be selected in a failover deployment by accident.
///
/// It reports `max_error = 0` ("trust me"), so it ALWAYS passes the driver gate and NEVER self-degrades.
/// `SystemTime` is non-monotonic by contract: a forward step (an NTP step, a leap second, `date -s`, a
/// VM live-migration or suspend) beyond the cross-node margin produces a plausible reading the proto
/// trusts, causing a STALE read. NEVER the documented production path — prefer [`NtpDisciplinedClock`].
#[cfg(feature = "unverified-wall-clock")]
#[derive(Debug, Clone, Copy, Default)]
pub struct UnverifiedSystemClock;

#[cfg(feature = "unverified-wall-clock")]
impl WallClock for UnverifiedSystemClock {
  const SUPPLIES_WALL: bool = true;

  #[inline(always)]
  fn now(&mut self) -> Option<WallReading> {
    Some(WallReading::new(system_wall_nanos(), 0))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn monotonic_never_supplies() {
    let mut c = Monotonic;
    assert!(c.now().is_none());
  }

  #[test]
  fn maxerror_us_to_ns_scales_and_clamps() {
    assert_eq!(maxerror_us_to_ns(50), 50_000); // 50 µs -> 50_000 ns (the 1000x scale)
    assert_eq!(maxerror_us_to_ns(0), 0);
    assert_eq!(maxerror_us_to_ns(-1), 0); // negative clamps to 0, never wraps
    assert_eq!(maxerror_us_to_ns(i64::MAX), u64::MAX); // saturates, never wraps
  }

  #[test]
  fn disciplined_reading_unsync_is_none_else_reports_error() {
    const UNSYNC: i32 = 0x0001;
    assert!(disciplined_reading(UNSYNC, 10, UNSYNC).is_none()); // unsynchronized -> None
    let r = disciplined_reading(0, 50, UNSYNC).expect("synced");
    assert_eq!(r.max_error_nanos(), 50_000); // 50 µs reported as 50_000 ns
    assert!(r.wall_nanos() > 0);
  }

  #[test]
  fn ntp_disciplined_reads_without_panic() {
    let mut c = NtpDisciplinedClock;
    let _ = c.now(); // adjtimex (Linux) or None (non-Linux) — must not panic
    #[cfg(not(target_os = "linux"))]
    assert!(c.now().is_none());
  }
}
