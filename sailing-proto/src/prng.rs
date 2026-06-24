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

// rand_core 0.10 renamed `RngCore` to the infallible `Rng`, which is a blanket impl over
// `TryRng<Error = Infallible>` — so the generator implements `TryRng` and gets `Rng` (with the
// `next_u64`/`fill_bytes` methods the consumers call) for free.
impl rand_core::TryRng for Prng {
  type Error = core::convert::Infallible;

  /// Next pseudo-random `u64`.
  #[inline]
  fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
    self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = self.0;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    Ok(z ^ (z >> 31))
  }

  #[inline]
  fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
    Ok(self.try_next_u64()? as u32)
  }

  #[inline]
  fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
    let mut chunks = dst.chunks_exact_mut(8);
    for chunk in &mut chunks {
      chunk.copy_from_slice(&self.try_next_u64()?.to_le_bytes());
    }
    let rem = chunks.into_remainder();
    if !rem.is_empty() {
      let bytes = self.try_next_u64()?.to_le_bytes();
      rem.copy_from_slice(&bytes[..rem.len()]);
    }
    Ok(())
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
  use rand_core::Rng;

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
