use super::*;

// ─── Fault rolls (seeded intensities) ────────────────────────────────────────────────────────────

/// Roll a seed-chosen [`NetworkFaults`] intensity. `calm == true` returns a near-faultless bus (used
/// inside calm windows / quiesce so liveness is achievable); otherwise a modest-to-spicy adversarial
/// schedule whose drop/jitter stay BOUNDED (a healthy majority can still re-replicate and beat the
/// election timeout — the heartbeat is 100 ms, the election timeout 1000 ms).
pub(crate) fn roll_network_faults(prng: &mut FaultPrng, calm: bool) -> NetworkFaults {
  if calm {
    return NetworkFaults::none();
  }
  // Latency 0..=8 ms, jitter 0..=30 ms — bounded well under the 100 ms heartbeat so liveness holds.
  let latency = Duration::from_millis(prng.next_u64() % 9);
  let jitter = Duration::from_millis(prng.next_u64() % 31);
  // Drop up to ~18%, dup up to ~12% — bounded loss the proto re-replicates through.
  let drop_per_mille = (prng.next_u64() % 181) as u32;
  let duplicate_per_mille = (prng.next_u64() % 121) as u32;
  let reorder = prng.next_u64().is_multiple_of(2);
  NetworkFaults {
    latency,
    jitter,
    drop_per_mille,
    duplicate_per_mille,
    reorder,
  }
}

/// Roll a seed-chosen [`StorageFaults`] intensity — LOW transient-read / torn-write rates so the
/// poison/recovery paths are reachable without permanently disabling a quorum. Bounded at a few
/// per-mille: a high transient-read rate would poison nodes faster than they recover.
pub(crate) fn roll_storage_faults(prng: &mut FaultPrng) -> StorageFaults {
  // 0..=6 per-mille transient read (the poison path), 0..=10 per-mille torn write (re-sync path).
  let transient_read_per_mille = (prng.next_u64() % 7) as u16;
  let torn_write_per_mille = (prng.next_u64() % 11) as u16;
  StorageFaults {
    transient_read_per_mille,
    torn_write_per_mille,
    ..StorageFaults::none()
  }
}
