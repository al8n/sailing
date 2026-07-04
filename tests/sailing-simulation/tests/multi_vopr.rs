#![allow(missing_docs)]
//! Multi-group VOPR sweeps: run the weighted multi-group fuzzer across seeds and assert every run
//! holds (per-group safety oracles, cross-talk, one-identity, calm-window liveness, quiesce)
//! while the band as a whole is non-vacuous.
//!
//! Replaying a failure: a panic prints `seed=<S> tick=<T>`. Reproduce with
//! `run_multi_vopr(S, ticks, MultiProfile::default_multi())` and inspect tick `T`.

use sailing_simulation::{MultiProfile, run_multi_vopr};

#[test]
fn same_seed_same_report() {
  let p = MultiProfile::default_multi();
  assert_eq!(
    run_multi_vopr(42, 3_000, p),
    run_multi_vopr(42, 3_000, p),
    "run_multi_vopr must be a pure function of (seed, ticks, profile)"
  );
}

#[test]
fn default_band_is_nonvacuous() {
  for seed in 0..8u64 {
    let r = run_multi_vopr(seed, 4_000, MultiProfile::default_multi());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
  }
}

/// The long band: 64 seeds × 40_000 ticks under the default profile — the M1 exit-criterion
/// depth. `#[ignore]` so the everyday gate stays fast; shardable via env exactly like the
/// single-group `vopr_long_sweep` (`MULTI_VOPR_SEED_{START,END}` + `MULTI_VOPR_TICKS`), so CI or
/// a local run can slice the band. Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "long band — run explicitly. Band via MULTI_VOPR_SEED_{START,END} + MULTI_VOPR_TICKS."]
fn long_band() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("MULTI_VOPR_SEED_START", 0);
  let end = env_u64("MULTI_VOPR_SEED_END", 64);
  let ticks = env_u64("MULTI_VOPR_TICKS", 40_000) as usize;
  assert!(end > start, "empty band: {start} >= {end}");
  for seed in start..end {
    let r = run_multi_vopr(seed, ticks, MultiProfile::default_multi());
    assert!(
      r.groups_created >= 2 && r.committed > 0,
      "seed {seed} vacuous: {r:?}"
    );
  }
}
