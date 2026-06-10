//! Seeded, faults-as-data network model for the simulator's typed-message bus.
//!
//! [`NetworkFaults`] describes a per-message adversarial delivery schedule — base latency, random
//! jitter, loss, duplication, and (jitter-induced) reordering — and is **all-off by default** so
//! the faultless bus is byte-identical to the original. Faults are applied at the bus-push point in `Cluster::tick`
//! (AFTER the structural oracles, which audit what a node SENDS regardless of delivery fate).
//!
//! Determinism: every random draw comes from a sim-local SplitMix64 ([`crate::store::FaultPrng`])
//! seeded from the cluster seed (a stream distinct from the per-node store seeds). The same seed
//! yields an identical run — NO wall-clock, NO `rand`. This per-message drop/reorder model is what
//! reaches the cross-feature liveness bugs a partition-only sim cannot.
use crate::store::FaultPrng;
use core::time::Duration;

/// Seeded, faults-as-data injection config for the typed-message bus. **All off by default** (a
/// faultless, zero-latency, FIFO bus — byte-identical to the original). Faults are deterministic
/// given the cluster seed and apply per message at the bus-push point.
///
/// Probabilities are expressed in *per mille* (×1000): `drop_per_mille = 150` is a 15% loss. A
/// value `>= 1000` is certainty; `0` is off.
#[derive(Debug, Clone, Copy, Default)]
pub struct NetworkFaults {
  /// Base one-way delivery delay added to every message's `deliver_at`. Default [`Duration::ZERO`]
  /// (immediate — the original zero-latency bus). A nonzero latency alone does NOT reorder
  /// (it shifts every message by the same constant).
  pub latency: Duration,
  /// Maximum EXTRA random delay added per message, drawn seeded-uniform in `[0, jitter]`. Default
  /// [`Duration::ZERO`]. With nonzero jitter, messages can be delivered out of order (the bus
  /// delivers by the `deliver_at` minimum) — unless [`reorder`](Self::reorder) is `false`, which
  /// clamps each (from,to) pair back to FIFO.
  pub jitter: Duration,
  /// Per-message loss probability ×1000 (so `150` ⇒ 15% of messages are dropped, never pushed onto
  /// the bus). Default `0`. Bounded loss still permits liveness (a healthy majority re-replicates).
  pub drop_per_mille: u32,
  /// Per-message duplication probability ×1000 (so `100` ⇒ 10% of messages are pushed TWICE, each
  /// `InFlight` getting an independent jitter draw so the copies may arrive at different times).
  /// Default `0`. Exercises the proto's idempotency (§5.3 conflict-conditional truncation,
  /// duplicate-ack handling).
  pub duplicate_per_mille: u32,
  /// Whether jitter is allowed to REORDER deliveries within a window. Default `false`: deliveries
  /// between the same (from,to) pair stay FIFO (each message's `deliver_at` is clamped to be ≥ the
  /// last-scheduled `deliver_at` for that pair). When `true`, jitter freely varies `deliver_at` so
  /// later sends can overtake earlier ones. (With the default `jitter = ZERO`, FIFO holds either
  /// way; reordering only ever occurs when BOTH `jitter > 0` and `reorder == true`.)
  pub reorder: bool,
}

impl NetworkFaults {
  /// All faults off (the default): zero latency, zero jitter, no loss, no duplication, FIFO. A bus
  /// configured with `none()` is byte-identical to the original bus.
  pub const fn none() -> Self {
    Self {
      latency: Duration::ZERO,
      jitter: Duration::ZERO,
      drop_per_mille: 0,
      duplicate_per_mille: 0,
      reorder: false,
    }
  }

  /// Whether every fault is off (the bus behaves as a faultless, zero-latency, FIFO bus).
  pub const fn is_none(&self) -> bool {
    self.latency.is_zero()
      && self.jitter.is_zero()
      && self.drop_per_mille == 0
      && self.duplicate_per_mille == 0
      && !self.reorder
  }
}

/// A seeded network-fault PRNG: a [`FaultPrng`] plus the per-message decision helpers (drop / dup
/// rolls and a uniform jitter draw). Seeded from the cluster seed on a stream distinct from the
/// per-node store seeds, so the network schedule is reproducible yet independent of storage faults.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NetPrng(FaultPrng);

impl NetPrng {
  /// Seed the network-fault PRNG from the cluster seed (a distinct stream from the store seeds).
  pub(crate) const fn new(seed: u64) -> Self {
    Self(FaultPrng::new(seed))
  }

  /// `true` with probability `per_mille / 1000`, advancing the PRNG. `0` ⇒ never, `>= 1000` ⇒
  /// always. Used for the per-message drop and duplicate rolls. (`per_mille` is `u32` to match the
  /// [`NetworkFaults`] fields; the chance is clamped at 1000 = certainty.)
  #[inline]
  pub(crate) fn chance_per_mille(&mut self, per_mille: u32) -> bool {
    if per_mille == 0 {
      return false;
    }
    if per_mille >= 1000 {
      return true;
    }
    (self.0.next_u64() % 1000) < per_mille as u64
  }

  /// A seeded uniform draw in `[0, max]` (inclusive), advancing the PRNG. `max == ZERO` returns
  /// `ZERO` WITHOUT consuming a draw, so enabling jitter does not perturb the unrelated drop/dup
  /// streams' relationship when jitter is off. Computed in nanoseconds (the sim's virtual clock is
  /// millisecond-scale, far within `u64` nanos).
  #[inline]
  pub(crate) fn jitter_draw(&mut self, max: Duration) -> Duration {
    let max_nanos = max.as_nanos();
    if max_nanos == 0 {
      return Duration::ZERO;
    }
    // `max` is a sim-configured jitter (ms-scale), so it fits comfortably in u64 nanos; clamp
    // defensively rather than panic on an absurd config.
    let span = u64::try_from(max_nanos.saturating_add(1)).unwrap_or(u64::MAX);
    Duration::from_nanos(self.0.next_u64() % span)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn default_is_none_and_byte_identical_shape() {
    let f = NetworkFaults::default();
    assert!(f.is_none());
    assert_eq!(f.latency, Duration::ZERO);
    assert_eq!(f.jitter, Duration::ZERO);
    assert_eq!(f.drop_per_mille, 0);
    assert_eq!(f.duplicate_per_mille, 0);
    assert!(!f.reorder);
    assert!(NetworkFaults::none().is_none());
  }

  #[test]
  fn is_none_false_when_any_fault_set() {
    assert!(
      !NetworkFaults {
        drop_per_mille: 1,
        ..NetworkFaults::none()
      }
      .is_none()
    );
    assert!(
      !NetworkFaults {
        jitter: Duration::from_millis(1),
        ..NetworkFaults::none()
      }
      .is_none()
    );
    assert!(
      !NetworkFaults {
        reorder: true,
        ..NetworkFaults::none()
      }
      .is_none()
    );
  }

  #[test]
  fn chance_per_mille_bounds_and_determinism() {
    // 0 ⇒ never, >=1000 ⇒ always (no draw consumed at the bounds, so stream stays aligned).
    let mut p = NetPrng::new(1);
    for _ in 0..100 {
      assert!(!p.chance_per_mille(0));
      assert!(p.chance_per_mille(1000));
      assert!(p.chance_per_mille(5000));
    }
    // Same seed ⇒ identical sequence.
    let seq = |seed: u64| -> Vec<bool> {
      let mut p = NetPrng::new(seed);
      (0..256).map(|_| p.chance_per_mille(300)).collect()
    };
    assert_eq!(seq(42), seq(42));
    // A 30% rate produces a mix (proves it actually fires both ways).
    let s = seq(42);
    assert!(s.iter().any(|&x| x) && s.iter().any(|&x| !x));
  }

  #[test]
  fn jitter_draw_zero_consumes_nothing_and_is_bounded() {
    // ZERO jitter returns ZERO without advancing the PRNG: the next roll matches a fresh PRNG.
    let mut p = NetPrng::new(7);
    assert_eq!(p.jitter_draw(Duration::ZERO), Duration::ZERO);
    let mut fresh = NetPrng::new(7);
    assert_eq!(p.chance_per_mille(500), fresh.chance_per_mille(500));

    // Nonzero jitter stays within [0, max] and is deterministic given the seed.
    let draws = |seed: u64| -> Vec<Duration> {
      let mut p = NetPrng::new(seed);
      (0..256)
        .map(|_| p.jitter_draw(Duration::from_millis(50)))
        .collect()
    };
    let d = draws(9);
    assert_eq!(d, draws(9), "same seed ⇒ same jitter sequence");
    assert!(d.iter().all(|&x| x <= Duration::from_millis(50)));
    // Non-vacuity: at least one nonzero draw.
    assert!(d.iter().any(|&x| x > Duration::ZERO));
  }
}
