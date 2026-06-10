//! VOPR seed sweep: run the fault-injecting fuzzer across a band of seeds and assert every run
//! holds (no safety-oracle violation, no livelock, no quiesce failure) while the band as a whole
//! exercises the hard paths (crashes, partitions, conf-changes, committed load).
//!
//! Each [`run_vopr`] is deterministic in `(seed, ticks)` and runs the per-tick safety-oracle suite
//! on every tick, panicking with `seed`+`tick` on any violation. So a green sweep is a proof that
//! the consensus core held under thousands of composed crash + partition + lossy-network +
//! membership schedules.
//!
//! Replaying a failure: a panic from [`run_vopr`] prints `seed=<S> tick=<T>`. Reproduce it with
//! `run_vopr(S, ticks)` (the same `ticks` the sweep used for that band) and inspect tick `T`.
#![allow(missing_docs)]

use sailing_simulation::{VoprReport, run_vopr};
use std::panic;

/// Run `seeds` through [`run_vopr`] at `ticks`, returning the per-seed reports. A run that panics
/// (a safety-oracle violation, a calm-window livelock, or a quiesce failure) fails the test with the
/// seed for replay — the fuzzer never swallows a real defect.
fn sweep(seeds: impl Iterator<Item = u64>, ticks: usize) -> Vec<VoprReport> {
  let prev = panic::take_hook();
  panic::set_hook(std::boxed::Box::new(|_| {})); // the sweep reports failures itself
  let mut reports = Vec::new();
  for seed in seeds {
    match panic::catch_unwind(|| run_vopr(seed, ticks)) {
      Ok(report) => reports.push(report),
      Err(e) => {
        panic::set_hook(prev);
        let msg = e
          .downcast_ref::<String>()
          .cloned()
          .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
          .unwrap_or_else(|| "<non-string panic>".to_string());
        panic!(
          "VOPR sweep FAILED at seed {seed} (ticks={ticks}) — replay with run_vopr({seed}, {ticks}):\n{}",
          msg.lines().take(4).collect::<Vec<_>>().join("\n"),
        );
      }
    }
  }
  panic::set_hook(prev);
  reports
}

/// Aggregate non-vacuity counters across a band of reports.
struct Coverage {
  crashes: u64,
  partitions: u64,
  conf_changes: u64,
  committed: u64,
  max_term: u64,
  faults_fired: u64,
}

fn coverage(reports: &[VoprReport]) -> Coverage {
  Coverage {
    crashes: reports.iter().map(|r| r.crashes).sum(),
    partitions: reports.iter().map(|r| r.partitions).sum(),
    conf_changes: reports.iter().map(|r| r.conf_changes).sum(),
    committed: reports.iter().map(|r| r.committed).sum(),
    max_term: reports.iter().map(|r| r.max_term_seen).max().unwrap_or(0),
    faults_fired: reports.iter().map(|r| r.faults_fired).sum(),
  }
}

/// The gated band: every seed in `0..24` holds at a modest tick budget, and the band collectively
/// exercises every hard path (so a future regression that silently stops crashing / partitioning /
/// reconfiguring is caught as a coverage drop, not just an outright failure).
#[test]
fn vopr_seed_band_holds_and_is_non_vacuous() {
  let reports = sweep(0..24, 200);
  let cov = coverage(&reports);
  assert!(
    cov.committed > 0,
    "band committed nothing — vacuous coverage: {:?}",
    (cov.committed, cov.crashes, cov.partitions)
  );
  assert!(cov.crashes > 0, "band never crashed a node");
  assert!(cov.partitions > 0, "band never partitioned a node");
  assert!(cov.conf_changes > 0, "band never reconfigured");
  assert!(cov.faults_fired > 0, "band never fired a seeded fault");
  std::eprintln!(
    "vopr band 0..24 @200: committed={} crashes={} partitions={} conf_changes={} max_term={} faults={}",
    cov.committed,
    cov.crashes,
    cov.partitions,
    cov.conf_changes,
    cov.max_term,
    cov.faults_fired,
  );
}

/// The exhaustive sweep: a wide seed band at a deep tick budget. `#[ignore]` so the everyday gate
/// stays fast. The band and depth are CONFIGURABLE via env vars so CI can SHARD it across many
/// runners (each shard a disjoint seed slice, run in parallel):
///
/// - `VOPR_SEED_START` — first seed, inclusive (default `0`)
/// - `VOPR_SEED_END`   — last seed, exclusive  (default `256`)
/// - `VOPR_TICKS`      — ticks per run          (default `2000`)
///
/// Run locally with `cargo test -p sailing-simulation --test vopr vopr_long_sweep -- --ignored
/// --nocapture`. Each run is deterministic in `(seed, ticks)` and platform/opt-level independent, so
/// a failure printed here (`seed=<S> ticks=<T>`) replays anywhere with `run_vopr(S, T)`.
#[test]
#[ignore = "long sweep — run explicitly (nightly / deep-sweep CI). Band via VOPR_SEED_{START,END} + VOPR_TICKS."]
fn vopr_long_sweep() {
  fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
      .ok()
      .and_then(|v| v.parse().ok())
      .unwrap_or(default)
  }
  let start = env_u64("VOPR_SEED_START", 0);
  let end = env_u64("VOPR_SEED_END", 256);
  let ticks = env_u64("VOPR_TICKS", 2_000) as usize;
  assert!(
    end > start,
    "empty band: VOPR_SEED_START={start} >= VOPR_SEED_END={end}"
  );
  let n = end - start;

  let reports = sweep(start..end, ticks);
  let cov = coverage(&reports);
  // Non-vacuity, SCALED to the band size so any shard (or the full sweep) still rejects a vacuous
  // run. Thresholds sit well below the observed rates (0..256 @2000 typically yields crashes≈350,
  // partitions≈215, conf_changes≈69, committed≈5000 — i.e. >5× headroom), so a real regression that
  // silently stops crashing / partitioning / reconfiguring shows up as a coverage drop, not just an
  // outright failure.
  assert!(
    cov.crashes >= n / 5,
    "sweep [{start},{end}) under-exercised crashes: {} (< {})",
    cov.crashes,
    n / 5
  );
  assert!(
    cov.partitions >= n / 5,
    "sweep [{start},{end}) under-exercised partitions: {} (< {})",
    cov.partitions,
    n / 5
  );
  assert!(
    cov.conf_changes * 12 >= n,
    "sweep [{start},{end}) under-exercised conf-changes: {} (< {})",
    cov.conf_changes,
    n / 12
  );
  assert!(
    cov.committed >= n * 4,
    "sweep [{start},{end}) committed too little: {} (< {})",
    cov.committed,
    n * 4
  );
  std::eprintln!(
    "vopr long sweep {start}..{end} @{ticks}: committed={} crashes={} partitions={} conf_changes={} max_term={} faults={}",
    cov.committed,
    cov.crashes,
    cov.partitions,
    cov.conf_changes,
    cov.max_term,
    cov.faults_fired,
  );
}
