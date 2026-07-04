#![allow(missing_docs)]
//! Multi-group VOPR sweeps: run the weighted multi-group fuzzer across seeds and assert every run
//! holds (per-group safety oracles, cross-talk, one-identity, calm-window liveness, quiesce)
//! while the band as a whole is non-vacuous.
//!
//! Replaying a failure: a panic prints `seed=<S> tick=<T>`. Reproduce with
//! `run_multi_vopr(S, ticks, MultiProfile::default_multi())` and inspect tick `T`.
//!
//! Regression seeds below were found by a short local sweep — run a few fast-band seeds and read
//! the report counters for the target shape (the drift-sweep pattern):
//! `for seed in 0..16 { dbg!(run_multi_vopr(seed, 4_000, MultiProfile::default_multi())); }`

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

/// Seed 2's schedule retires groups steadily while client load keeps committing (66 removals /
/// 568 committed in the sweep) — the removal-during-load shape, with every oracle armed: the
/// run completing IS the assertion that no cross-talk, one-identity, or per-group safety
/// violation rode the churn.
#[test]
fn regression_seed_2_removal_during_load() {
  let r = run_multi_vopr(2, 4_000, MultiProfile::default_multi());
  assert!(
    r.groups_removed > 0 && r.committed > 0,
    "the shape requires removal under live load: {r:?}"
  );
}

/// Seed 11's schedule recreates retired gids under load harder than its peers (38 recreations
/// in the sweep) — the gen+1 same-logical-group rejoin shape, crossing the one-identity
/// oracle's incarnation boundary on every recreation.
#[test]
fn regression_seed_11_recreate_under_load() {
  let r = run_multi_vopr(11, 4_000, MultiProfile::default_multi());
  assert!(
    r.groups_recreated > 0 && r.committed > 0,
    "the shape requires recreation under live load: {r:?}"
  );
}

/// Seed 36 pins the ZOMBIE-REAP shape: a voter is removed while partitioned, the farewell
/// append is lost in the fault soup, and the victim lingers believing itself leader at a stale
/// term — at etcd-parity defaults (pre-vote/check-quorum off) higher-term peers silently ignore
/// its beats, so only the harness's catalog model (the departed sweep) retires it. A
/// first-Leader-in-id-order scan once let this zombie shadow the live quorum's leader and
/// turned the healthy schedule into a false calm-window livelock at these exact tick counts;
/// the leader view must anchor on the highest term. Run under `--release` when iterating —
/// 24k ticks in debug is minutes.
#[test]
fn regression_seed_36_removed_zombie_leader_is_reaped() {
  let r = run_multi_vopr(36, 24_000, MultiProfile::default_multi());
  assert!(
    r.groups_removed > 0 && r.committed > 0,
    "the shape requires removal under load: {r:?}"
  );
}

/// The M1 exit-criterion soak: 64 seeds x 40_000 ticks under the default profile. `#[ignore]`
/// so the everyday gate stays fast; ALWAYS run it `--release` (the band in debug wastes hours):
///
/// ```text
/// cargo test --release -p sailing-simulation --test multi_vopr -- --ignored soak_default_profile
/// ```
///
/// Shardable via env exactly like the single-group `vopr_long_sweep`
/// (`MULTI_VOPR_SEED_{START,END}` + `MULTI_VOPR_TICKS`), so CI or a local run can slice the
/// band. Deterministic per (seed, ticks): failures replay anywhere.
#[test]
#[ignore = "the M1 soak — run explicitly, --release. Shard via MULTI_VOPR_SEED_{START,END} + MULTI_VOPR_TICKS."]
fn soak_default_profile() {
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
