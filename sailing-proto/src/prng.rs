//! A small deterministic PRNG (SplitMix64) for randomized election timeouts. The core
//! never reads platform entropy — determinism is what makes the simulator reproducible.
use core::time::Duration;

/// SplitMix64 PRNG, seeded at `Endpoint` construction.
#[derive(Debug, Clone)]
pub struct Prng(u64);

impl Prng {
  /// Seed the generator.
  #[inline(always)]
  pub const fn new(seed: u64) -> Self {
    Self(seed)
  }

  /// Next pseudo-random `u64`.
  #[inline]
  pub fn next_u64(&mut self) -> u64 {
    self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.0;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }

  /// A randomized election timeout in `[base, 2*base)` (Raft's spread that breaks split votes).
  #[inline]
  pub fn election_timeout(&mut self, base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let extra = if base_ms == 0 {
      0
    } else {
      self.next_u64() % base_ms
    };
    base + Duration::from_millis(extra)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::time::Duration;

  #[test]
  fn deterministic_and_in_range() {
    let mut a = Prng::new(7);
    let mut b = Prng::new(7);
    assert_eq!(a.next_u64(), b.next_u64()); // same seed → same stream
    let base = Duration::from_millis(1000);
    for _ in 0..1000 {
      let t = a.election_timeout(base);
      assert!(t >= base && t < base * 2); // [T, 2T)
    }
  }
}
