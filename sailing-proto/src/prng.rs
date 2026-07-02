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
  if base_ms >= 2 {
    // Realistic election timeouts: millisecond-granular jitter, kept byte-identical to the historical
    // behavior so it does not perturb the simulator's RNG-derived schedules (`next_u64() % base_ms`
    // has a meaningful non-degenerate range once `base_ms >= 2`).
    base + Duration::from_millis(rng.next_u64() % base_ms)
  } else {
    // Sub-2ms timeouts: `base_ms` is 0 or 1, so `x % base_ms` collapses to 0, pinning the timeout to
    // exactly `base` and defeating Raft's [T, 2T) split-vote randomization. Draw at NANOSECOND
    // granularity instead. `next_u64()` is the u64 dividend, so its remainder modulo any positive
    // `base_ns` fits back into u64 (the `as u64` is lossless). A degenerate zero base draws nothing.
    let base_ns = base.as_nanos();
    if base_ns == 0 {
      base
    } else {
      base + Duration::from_nanos((rng.next_u64() as u128 % base_ns) as u64)
    }
  }
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

  #[test]
  fn next_u32_is_deterministic_and_truncates_u64() {
    let mut a = Prng::new(7);
    let mut b = Prng::new(7);
    assert_eq!(a.next_u32(), b.next_u32()); // same seed → same u32 stream
    // `next_u32` advances the same state as `next_u64` and returns its low 32 bits.
    let mut c = Prng::new(123);
    let mut d = Prng::new(123);
    assert_eq!(c.next_u32(), d.next_u64() as u32);
  }

  #[test]
  fn fill_bytes_covers_chunks_and_remainder() {
    // 20 bytes = two full 8-byte chunks + a 4-byte remainder (both branches of `try_fill_bytes`).
    let mut p = Prng::new(42);
    let mut buf = [0u8; 20];
    p.fill_bytes(&mut buf);

    // Reproduce byte-for-byte from the raw u64 stream of an identically seeded generator.
    let mut q = Prng::new(42);
    let w0 = q.next_u64().to_le_bytes();
    let w1 = q.next_u64().to_le_bytes();
    let w2 = q.next_u64().to_le_bytes();
    let mut expected = [0u8; 20];
    expected[0..8].copy_from_slice(&w0);
    expected[8..16].copy_from_slice(&w1);
    expected[16..20].copy_from_slice(&w2[..4]); // remainder is the LE prefix of the next word
    assert_eq!(buf, expected);
  }

  #[test]
  fn fill_bytes_exact_multiple_has_no_remainder() {
    // 16 bytes = exactly two chunks: the `!rem.is_empty()` branch must NOT fire.
    let mut p = Prng::new(99);
    let mut buf = [0u8; 16];
    p.fill_bytes(&mut buf);
    let mut q = Prng::new(99);
    let mut expected = [0u8; 16];
    expected[0..8].copy_from_slice(&q.next_u64().to_le_bytes());
    expected[8..16].copy_from_slice(&q.next_u64().to_le_bytes());
    assert_eq!(buf, expected);
  }

  #[test]
  fn sub_millisecond_base_still_jitters() {
    // A sub-millisecond base rounds to 0 ms, so a millisecond-granular jitter would draw `% 0/1`
    // (always 0) and return a FIXED `base` for every seed — collapsing the [T, 2T) spread. At
    // nanosecond granularity the same base still spreads across seeds while staying in range.
    let base = Duration::from_micros(800);
    let first = election_timeout(&mut Prng::new(0), base);
    let mut all_equal = true;
    for seed in 0..1000u64 {
      let t = election_timeout(&mut Prng::new(seed), base);
      assert!(t >= base && t < base * 2, "still within [T, 2T)");
      if t != first {
        all_equal = false;
      }
    }
    assert!(
      !all_equal,
      "a sub-millisecond base must still produce distinct jittered timeouts"
    );
  }

  #[test]
  fn election_timeout_zero_base_returns_zero_without_drawing() {
    // A zero base short-circuits the modulo (which would panic on `% 0`) AND draws no randomness —
    // the documented "EXACTLY one draw when base is non-zero" contract.
    let mut a = Prng::new(1);
    let mut untouched = a.clone();
    assert_eq!(election_timeout(&mut a, Duration::ZERO), Duration::ZERO);
    // No draw consumed state: the generator still produces its first value.
    assert_eq!(a.next_u64(), untouched.next_u64());
  }
}
