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
/// Serves ONLY the raw `unverified-wall-clock` source now; `disciplined_reading` reads the wall from the
/// adjtimex result instead, so this is dead when that feature is off.
#[cfg_attr(not(feature = "unverified-wall-clock"), allow(dead_code))]
fn system_wall_nanos() -> u64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// adjtimex `maxerror` (signed MICROSECONDS) to nanoseconds, or `None` when it is not a real bound. A
/// NEGATIVE `maxerror` is FAIL-CLOSED: the prior clamp-to-0 turned it into a claimed-PERFECT clock
/// (fail-OPEN — a zero error passes every ε_unc gate), letting a successor pass the precise release
/// early. A valid non-negative value scales by `1000`; `saturating_mul` guards the (unreachable for a
/// real kernel) overflow so nothing ever wraps. Isolated + unit-tested because a raw µs-vs-ns compare
/// downstream would be a 1000× bug; no threshold is in scope here (the driver `Clock` applies ε_unc).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn maxerror_us_to_ns(maxerror_us: i64) -> Option<u64> {
  u64::try_from(maxerror_us)
    .ok()
    .map(|us| us.saturating_mul(1_000))
}

/// The disciplined reading from a kernel `timex`, factored out so it is unit-testable WITHOUT a syscall.
/// Takes the timex-relevant fields — `status`, `maxerror` (µs), the `time` seconds and fractional part,
/// and the `STA_UNSYNC`/`STA_NANO` bit masks — so the WALL and its ERROR BOUND come from the SAME
/// adjtimex result. A separate later `SystemTime::now()` (the prior implementation) could step or be
/// descheduled between the syscall and the wall sample, pairing a fresh wall with a stale-small error:
/// a falsely-low wall that the precise release passes early, overlapping the old leader's live lease.
///
/// `None` when unsynchronized OR any field is invalid — fail CLOSED rather than fabricate a trusted
/// reading: a negative `maxerror` (not a real bound), or a negative/pre-epoch `time`. Never wraps
/// (saturating throughout).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn disciplined_reading(
  status: i32,
  maxerror_us: i64,
  time_sec: i64,
  time_frac: i64,
  unsync_bit: i32,
  nano_bit: i32,
) -> Option<WallReading> {
  // Unsynchronized: the kernel cannot vouch for the clock at all.
  if (status & unsync_bit) != 0 {
    return None;
  }
  // Fail-closed field validation (any `?` below ⇒ `None`, never a fabricated reading).
  let max_error_nanos = maxerror_us_to_ns(maxerror_us)?;
  let sec = u64::try_from(time_sec).ok()?;
  let frac = u64::try_from(time_frac).ok()?;
  // adjtimex reports `time.tv_usec` in NANOSECONDS when STA_NANO is set, else MICROSECONDS (`maxerror`
  // is ALWAYS microseconds, regardless of STA_NANO). Reading a nanosecond fraction as microseconds
  // would inflate the wall by 1000×; the sub-second field is bounded (<1 s), but the scale must match.
  let frac_nanos = if (status & nano_bit) != 0 {
    frac
  } else {
    frac.saturating_mul(1_000)
  };
  let wall_nanos = sec.saturating_mul(1_000_000_000).saturating_add(frac_nanos);
  Some(WallReading::new(wall_nanos, max_error_nanos))
}

/// The PRODUCTION wall source: reads the OS clock-discipline state (Linux `adjtimex`) and reports a
/// [`WallReading`] with the kernel's worst-case error, or `None` when the clock is unsynchronized
/// (`STA_UNSYNC`) or `adjtimex` errors. The driver `Clock` then degrades to [`Wall::ABSENT`](sailing_proto::Wall::ABSENT) when that
/// error exceeds ε_unc. On non-Linux targets (no `adjtimex` equivalent) it always returns `None` —
/// supply your own [`WallClock`] there.
///
/// A ZST, selected as the driver's `W` type parameter. Selecting it does NOT enable failover: you must
/// ALSO set `Config::bounded_clock_uncertainty`, else the tier is inert (the wall over-bounds ε_unc 0).
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
    // Marshal the timex-relevant fields into the pure reader; the wall (`t.time`) and its error
    // (`t.maxerror`) both come from THIS one result. `STA_NANO` selects the `t.time.tv_usec` unit.
    disciplined_reading(
      t.status,
      t.maxerror as i64,
      t.time.tv_sec as i64,
      t.time.tv_usec as i64,
      libc::STA_UNSYNC,
      libc::STA_NANO,
    )
  }

  #[cfg(not(target_os = "linux"))]
  fn read(&self) -> Option<WallReading> {
    None
  }
}

impl WallClock for NtpDisciplinedClock {
  // Only Linux reads the adjtimex sync state; elsewhere `read` always returns `None`, so the source
  // cannot vouch for a wall and the bind guard MUST reject it for a failover config rather than let the
  // tier silently never fire.
  const SUPPLIES_WALL: bool = cfg!(target_os = "linux");

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
  fn maxerror_us_to_ns_scales_or_fails_closed() {
    assert_eq!(maxerror_us_to_ns(50), Some(50_000)); // 50 µs -> 50_000 ns (the 1000x scale)
    assert_eq!(maxerror_us_to_ns(0), Some(0));
    assert_eq!(maxerror_us_to_ns(-1), None); // negative is not a real bound -> fail closed
    assert_eq!(maxerror_us_to_ns(i64::MAX), Some(u64::MAX)); // saturates, never wraps
  }

  // adjtimex status bits used by the pure tests below (the real values live in `libc`).
  const UNSYNC: i32 = 0x0001;
  const NANO: i32 = 0x2000;

  #[test]
  fn disciplined_reading_unsync_is_none_else_reports_error() {
    // STA_UNSYNC set -> None regardless of the other fields.
    assert!(disciplined_reading(UNSYNC, 10, 1, 0, UNSYNC, NANO).is_none());
    // Synced: the µs->ns error, and a wall taken from the injected `time` (100 s, zero fraction).
    let r = disciplined_reading(0, 50, 100, 0, UNSYNC, NANO).expect("synced");
    assert_eq!(r.max_error_nanos(), 50_000); // 50 µs reported as 50_000 ns
    assert_eq!(r.wall_nanos(), 100 * 1_000_000_000);
  }

  /// Fail CLOSED on invalid fields: a NEGATIVE maxerror (the prior clamp-to-0 made it a claimed-PERFECT
  /// clock — fail-OPEN), and a negative/pre-epoch `time` seconds, both yield `None`.
  #[test]
  fn disciplined_reading_fails_closed_on_invalid_fields() {
    assert!(disciplined_reading(0, -1, 100, 0, UNSYNC, NANO).is_none()); // negative maxerror
    assert!(disciplined_reading(0, 50, -1, 0, UNSYNC, NANO).is_none()); // pre-epoch seconds
  }

  /// STA_NANO selects the `time.tv_usec` unit — NANOSECONDS when set, MICROSECONDS when clear — so the
  /// same raw fraction differs by exactly 1000× in the reading's sub-second wall (`maxerror` is
  /// unaffected: always microseconds).
  #[test]
  fn disciplined_reading_honors_sta_nano() {
    let as_micros = disciplined_reading(0, 0, 5, 250, UNSYNC, NANO).expect("synced");
    let as_nanos = disciplined_reading(NANO, 0, 5, 250, UNSYNC, NANO).expect("synced");
    assert_eq!(as_micros.wall_nanos(), 5 * 1_000_000_000 + 250_000); // 250 µs
    assert_eq!(as_nanos.wall_nanos(), 5 * 1_000_000_000 + 250); // 250 ns
    assert_eq!(
      as_micros.wall_nanos() - as_nanos.wall_nanos(),
      250 * 1_000 - 250
    );
  }

  /// The wall equals the INJECTED timex `time` exactly — proving no `SystemTime::now()` sits in the
  /// path (the wall and its error come from the one adjtimex result).
  #[test]
  fn disciplined_reading_wall_is_the_injected_time() {
    let r = disciplined_reading(0, 20, 1_700_000_000, 123_456, UNSYNC, NANO).expect("synced");
    assert_eq!(
      r.wall_nanos(),
      1_700_000_000 * 1_000_000_000 + 123_456 * 1_000
    );
    assert_eq!(r.max_error_nanos(), 20_000);
  }

  #[test]
  fn ntp_disciplined_reads_without_panic() {
    let mut c = NtpDisciplinedClock;
    let _ = c.now(); // adjtimex (Linux) or None (non-Linux) — must not panic
    #[cfg(not(target_os = "linux"))]
    assert!(c.now().is_none());
  }

  /// The unverified clock ALWAYS supplies a reading with zero claimed error ("trust me") — it never
  /// self-degrades, the property that makes it test-only.
  #[cfg(feature = "unverified-wall-clock")]
  #[test]
  fn unverified_clock_always_supplies_zero_error() {
    let mut c = UnverifiedSystemClock;
    let r = c
      .now()
      .expect("the unverified clock always supplies a reading");
    assert_eq!(r.max_error_nanos(), 0, "it reports zero claimed error");
    assert!(r.wall_nanos() > 0);
  }
}
