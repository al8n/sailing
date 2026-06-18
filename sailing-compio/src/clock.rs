//! The crate-`Instant` ↔ wall-clock anchor, the synchronized-`Now` source, and the redial jitter.

use std::time::{Duration, Instant as StdInstant};

use sailing_proto::{Config, Instant, NodeId, Now, Wall};

use crate::{
  BindError,
  wall_clock::{Monotonic, WallClock},
};

/// Anchors the proto's monotonic [`Instant`] to an epoch captured at startup, owns the [`WallClock`]
/// source `W` (default [`Monotonic`]), and holds the cluster `ε_unc` captured from the proto `Config`
/// at bind: `Some(nanos)` INSIDE the LeaseGuard failover tier (including an exact `Some(0)`), `None`
/// outside it.
///
/// The driver reads [`Clock::now`] once per wake for a [`Now`] carrying both the monotonic instant and
/// the synchronized [`Wall`]. The ε_unc gate lives HERE — the SOLE site that compares a source's
/// self-reported error to the cluster bound — so the source never sees ε_unc and the threshold has one
/// owner. The load-bearing identity `Now::synchronized(mono, Wall::ABSENT) == Now::monotonic(mono)`
/// makes the default (`Monotonic` → no reading → `ABSENT`) byte-identical to a monotonic-only driver.
pub struct Clock<W = Monotonic> {
  base: StdInstant,
  wall: W,
  eps_unc_ns: Option<u64>,
}

impl<W: WallClock> Clock<W> {
  /// Anchor the epoch, take the wall source, and capture the cluster `ε_unc` (`Some(nanos)` inside the
  /// failover tier, `None` outside it).
  #[must_use]
  pub fn new(eps_unc_ns: Option<u64>, wall: W) -> Self {
    Self {
      base: StdInstant::now(),
      wall,
      eps_unc_ns,
    }
  }

  /// The synchronized reading: the monotonic [`Instant`] since the epoch PLUS the gated wall. A source
  /// reading is passed through only INSIDE the failover tier (`ε_unc` is `Some`) AND when its
  /// self-reported error is within that `ε_unc`; a `None` reading, an over-bound one, or a `None` ε_unc
  /// (failover off) all collapse to [`Wall::ABSENT`] (fail-closed). An exact `Some(0)` tier admits ONLY
  /// a zero-error reading — never confused with failover-off, which admits nothing at all.
  #[must_use]
  pub fn now(&mut self) -> Now {
    let mono = Instant::from_origin(StdInstant::now().saturating_duration_since(self.base));
    let wall = match self.wall.now() {
      Some(r)
        if self
          .eps_unc_ns
          .is_some_and(|eps| r.max_error_nanos() <= eps) =>
      {
        Wall::from_nanos(r.wall_nanos())
      }
      _ => Wall::ABSENT,
    };
    Now::synchronized(mono, wall)
  }

  /// The bare monotonic [`Instant`] since the epoch, for timer/deadline math (no wall read).
  #[must_use]
  pub fn mono(&self) -> Instant {
    Instant::from_origin(StdInstant::now().saturating_duration_since(self.base))
  }

  /// Map a proto [`Instant`] deadline back to a `std::time::Instant` on the same epoch, for
  /// `compio::time::sleep_until`.
  #[must_use]
  pub fn to_std(&self, at: Instant) -> StdInstant {
    self.base + at.since_origin()
  }
}

/// Validate the proto `Config` and capture the cluster `ε_unc` for the [`Clock`] wall gate — the SOLE
/// copy of the threshold: `Some(nanos)` inside the LeaseGuard FAILOVER tier (`bounded_clock_uncertainty`
/// set, INCLUDING an exact `Some(0)`), `None` outside it — the same `Option` the proto keeps, so a
/// `Some(0)` tier is NOT flattened into failover-off. Rejects a failover tier paired with a wall source
/// `W` that cannot supply a wall — the loud [`BindError::MissingWallSource`], since the tier would
/// otherwise silently never fire. Run at `bind`, BEFORE the socket binds.
pub(crate) fn validate_and_capture_eps<I, W>(config: &Config<I>) -> Result<Option<u64>, BindError>
where
  I: NodeId,
  W: WallClock,
{
  config.validate()?;
  let eps = config.bounded_clock_uncertainty();
  if eps.is_some() && !W::SUPPLIES_WALL {
    return Err(BindError::MissingWallSource);
  }
  Ok(eps.map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)))
}

/// `base` plus up to 25% jitter, decorrelating redial schedules across nodes so a common-mode
/// event (one peer restarting, a network blip) does not produce synchronized dial bursts from
/// every dialer. Sub-millisecond wall-clock entropy is plenty of decorrelation for a backoff
/// schedule — no RNG dependency needed. Monotone in `base` with jitter at most `base / 4`, so a
/// doubled base always schedules strictly later than the previous jittered delay (the strict
/// spacing an exponential redial schedule needs).
// Consumed by the drivers' redial schedules; the allow is removed when the first driver lands.
#[allow(dead_code)]
pub(crate) fn jittered(base: Duration) -> Duration {
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map_or(0, |d| d.subsec_nanos());
  base + base * (nanos % 256) / 1024
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn clock_round_trips_through_the_anchor() {
    let mut clock = Clock::new(None, Monotonic);
    let now = clock.now().mono();
    let later = now + Duration::from_millis(250);
    let std_later = clock.to_std(later);
    // Re-mapping the std deadline's offset recovers the proto instant exactly: the mapping is
    // affine over one shared base.
    let recovered = Instant::from_origin(std_later.duration_since(clock.base));
    assert_eq!(recovered, later);
  }

  #[test]
  fn now_is_monotone_nondecreasing() {
    let mut clock = Clock::new(None, Monotonic);
    let a = clock.now().mono();
    let b = clock.now().mono();
    assert!(b >= a);
  }

  #[test]
  fn monotonic_source_always_yields_absent_wall() {
    let mut clock = Clock::new(None, Monotonic);
    // The Monotonic source reports no reading -> the wall is ABSENT, so the Now is byte-identical to a
    // monotonic-only Now (the load-bearing invariant).
    let n = clock.now();
    assert!(n.wall().is_absent());
    assert_eq!(n, Now::monotonic(n.mono()));
  }

  #[test]
  fn the_eps_gate_passes_within_bound_and_fails_over_bound() {
    use crate::wall_clock::{WallClock, WallReading};
    // A fixture source that reports a fixed error so we can exercise the gate deterministically.
    struct Fixed(u64);
    impl WallClock for Fixed {
      const SUPPLIES_WALL: bool = true;
      fn now(&mut self) -> Option<WallReading> {
        Some(WallReading::new(1_000, self.0))
      }
    }
    // ε_unc Some(50ms): error 40ms <= 50ms -> present wall (1000ns); error 60ms > 50ms -> ABSENT.
    let mut within = Clock::new(Some(50_000_000), Fixed(40_000_000));
    assert_eq!(within.now().wall(), Wall::from_nanos(1_000));
    let mut over = Clock::new(Some(50_000_000), Fixed(60_000_000));
    assert!(over.now().wall().is_absent());
    // An EXACT Some(0) failover tier admits ONLY a zero-error reading — it is NOT failover-off.
    let mut exact_ok = Clock::new(Some(0), Fixed(0));
    assert_eq!(exact_ok.now().wall(), Wall::from_nanos(1_000));
    let mut exact_over = Clock::new(Some(0), Fixed(1));
    assert!(exact_over.now().wall().is_absent());
    // None (failover off) -> ABSENT regardless of the source's error (the gate never opens).
    let mut off = Clock::new(None, Fixed(0));
    assert!(off.now().wall().is_absent());
  }

  #[test]
  fn jitter_is_bounded_and_monotone_across_doubling() {
    for base_ms in [10u64, 50, 100, 400, 1_600] {
      let base = Duration::from_millis(base_ms);
      let j = jittered(base);
      assert!(j >= base, "jitter never schedules earlier than the base");
      assert!(
        j <= base + base / 4,
        "jitter is at most a quarter of the base"
      );
      // A doubled base strictly out-schedules the previous jittered delay even at maximal
      // jitter: 2*base > base + base/4.
      assert!(Duration::from_millis(base_ms * 2) > base + base / 4);
    }
  }

  #[test]
  fn validate_and_capture_eps_rejects_failover_without_wall_source() {
    use crate::wall_clock::{NtpDisciplinedClock, WallClock, WallReading};
    use sailing_proto::ReadOnlyOption;
    // An always-supplying source so the Ok-with-ε case is platform-independent.
    struct AlwaysSupplies;
    impl WallClock for AlwaysSupplies {
      const SUPPLIES_WALL: bool = true;
      fn now(&mut self) -> Option<WallReading> {
        None
      }
    }
    let failover = Config::try_new(
      1u64,
      vec![1u64, 2, 3],
      Duration::from_millis(1_000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_read_only(ReadOnlyOption::LeaseGuard)
    .with_lease_duration(Duration::from_millis(200))
    .with_clock_drift_bound(Duration::from_millis(2))
    .with_bounded_clock_uncertainty(Duration::from_millis(5));
    // a valid failover tier + a non-supplying source -> the loud wedge error (NOT a silent inert tier).
    assert!(matches!(
      validate_and_capture_eps::<u64, Monotonic>(&failover),
      Err(BindError::MissingWallSource)
    ));
    // a valid failover tier + a supplying source -> Ok, ε_unc captured in nanos (5 ms).
    assert_eq!(
      validate_and_capture_eps::<u64, AlwaysSupplies>(&failover).unwrap(),
      Some(5_000_000)
    );
    // NtpDisciplinedClock supplies a wall ONLY on Linux; elsewhere it is non-supplying and a failover
    // config is rejected just like Monotonic (the loud non-Linux startup failure).
    #[cfg(target_os = "linux")]
    assert_eq!(
      validate_and_capture_eps::<u64, NtpDisciplinedClock>(&failover).unwrap(),
      Some(5_000_000)
    );
    #[cfg(not(target_os = "linux"))]
    assert!(matches!(
      validate_and_capture_eps::<u64, NtpDisciplinedClock>(&failover),
      Err(BindError::MissingWallSource)
    ));
    // a non-failover Config + any source -> Ok(0): the gate stays inert, no source required.
    let mono = Config::try_new(
      1u64,
      vec![1u64, 2, 3],
      Duration::from_millis(1_000),
      Duration::from_millis(100),
    )
    .unwrap();
    assert_eq!(
      validate_and_capture_eps::<u64, Monotonic>(&mono).unwrap(),
      None
    );
  }
}
