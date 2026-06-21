//! A small deterministic PRNG (SplitMix64) for randomized election timeouts. The core
//! never reads platform entropy — determinism is what makes the simulator reproducible.
use core::time::Duration;

/// SplitMix64 PRNG, the default `Endpoint` RNG (seeded at construction).
#[derive(Debug, Clone)]
pub struct Prng(u64);

impl Prng {
  /// Seed the generator.
  #[inline(always)]
  pub const fn new(seed: u64) -> Self {
    Self(seed)
  }
}

impl rand_core::RngCore for Prng {
  /// Next pseudo-random `u64`.
  #[inline]
  fn next_u64(&mut self) -> u64 {
    self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.0;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
  }

  #[inline]
  fn next_u32(&mut self) -> u32 {
    self.next_u64() as u32
  }

  #[inline]
  fn fill_bytes(&mut self, dst: &mut [u8]) {
    let mut chunks = dst.chunks_exact_mut(8);
    for chunk in &mut chunks {
      chunk.copy_from_slice(&self.next_u64().to_le_bytes());
    }
    let rem = chunks.into_remainder();
    if !rem.is_empty() {
      let bytes = self.next_u64().to_le_bytes();
      rem.copy_from_slice(&bytes[..rem.len()]);
    }
  }
}

/// A randomized election timeout in `[base, 2*base)` (Raft's spread that breaks split votes).
///
/// Draws EXACTLY one `next_u64()` when `base` is non-zero — the sole RNG draw on the live path,
/// so the default [`Prng`] reproduces the simulator's byte-identical stream.
#[inline]
pub(crate) fn election_timeout<R: rand::Rng>(rng: &mut R, base: Duration) -> Duration {
  let base_ms = base.as_millis() as u64;
  let extra = if base_ms == 0 {
    0
  } else {
    rng.next_u64() % base_ms
  };
  base + Duration::from_millis(extra)
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::time::Duration;
  use rand_core::RngCore;

  #[test]
  fn deterministic_and_in_range() {
    let mut a = Prng::new(7);
    let mut b = Prng::new(7);
    assert_eq!(a.next_u64(), b.next_u64()); // same seed → same stream
    let base = Duration::from_millis(1000);
    for _ in 0..1000 {
      let t = election_timeout(&mut a, base);
      assert!(t >= base && t < base * 2); // [T, 2T)
    }
  }
}
