//! The crate-`Instant` ↔ wall-clock anchor and the redial jitter.

use std::time::{Duration, Instant as StdInstant};

use sailing_proto::Instant;

/// Anchors the proto's monotonic [`Instant`] to an epoch captured at startup.
///
/// The driver holds one `Clock` and reads [`Clock::now`] once per wake to feed the
/// coordinator's `handle_*` methods. A proto [`Instant`] deadline returned by the
/// coordinator's `poll_timeout` maps back to a `std::time::Instant` (for
/// `compio::time::sleep_until`) via [`Clock::to_std`].
pub struct Clock {
  base: StdInstant,
}

impl Clock {
  /// Anchor the epoch to the current instant.
  #[must_use]
  pub fn new() -> Self {
    Self {
      base: StdInstant::now(),
    }
  }

  /// The current proto [`Instant`] — the elapsed time since the epoch.
  #[must_use]
  pub fn now(&self) -> Instant {
    Instant::from_origin(StdInstant::now().saturating_duration_since(self.base))
  }

  /// Map a proto [`Instant`] deadline back to a `std::time::Instant` on the same epoch, for
  /// `compio::time::sleep_until`.
  #[must_use]
  pub fn to_std(&self, at: Instant) -> StdInstant {
    self.base + at.since_origin()
  }
}

impl Default for Clock {
  fn default() -> Self {
    Self::new()
  }
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
    let clock = Clock::new();
    let now = clock.now();
    let later = now + Duration::from_millis(250);
    let std_later = clock.to_std(later);
    // Re-mapping the std deadline's offset recovers the proto instant exactly: the mapping is
    // affine over one shared base.
    let recovered = Instant::from_origin(std_later.duration_since(clock.base));
    assert_eq!(recovered, later);
  }

  #[test]
  fn now_is_monotone_nondecreasing() {
    let clock = Clock::new();
    let a = clock.now();
    let b = clock.now();
    assert!(b >= a);
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
}
