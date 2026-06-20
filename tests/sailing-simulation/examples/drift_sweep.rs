//! A parallel VOPR seed sweep — runs `run_vopr` across a band of seeds on a worker pool, catching
//! each run's panic so one failing seed does not abort the rest, and reports the failing seeds plus
//! the clock-drift non-vacuity witnesses (reads confirmed, superseded-leader serves, partitions). Not
//! a gated test (it is unbounded by design); the gated coverage lives in the `vopr::tests` module.
//! Use it for deep confidence sweeps and for finding fresh LeaseGuard-drift seeds.
//!
//! Usage: cargo run --release --example drift_sweep -- <start> <end> <ticks> <threads>
use std::{
  panic,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  },
};

fn main() {
  let a: Vec<String> = std::env::args().collect();
  let start: u64 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
  let end: u64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(64);
  let ticks: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1500);
  let threads: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(12);

  panic::set_hook(Box::new(|_| {})); // quiet — only our summary prints

  let failures: Arc<Mutex<Vec<(u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
  let next = Arc::new(AtomicU64::new(start));
  let done = Arc::new(AtomicU64::new(0));
  // Aggregate non-vacuity witnesses across the sweep.
  let confirmed = Arc::new(AtomicU64::new(0));
  let superseded = Arc::new(AtomicU64::new(0));
  let superseded_seeds = Arc::new(AtomicU64::new(0));
  let partitions = Arc::new(AtomicU64::new(0));
  // FAILOVER sub-mode witnesses: seeds that drew it, seeds whose precise anchor actually fired, and the
  // total precise early-releases + offset re-syncs.
  let failover_seeds = Arc::new(AtomicU64::new(0));
  let precise_seeds = Arc::new(AtomicU64::new(0));
  let precise_total = Arc::new(AtomicU64::new(0));
  let resync_total = Arc::new(AtomicU64::new(0));
  // Inherited-serve witness: total inherited serves recorded across the sweep.
  let inherited_serves_total = Arc::new(AtomicU64::new(0));
  std::thread::scope(|scope| {
    for _ in 0..threads {
      let failures = Arc::clone(&failures);
      let next = Arc::clone(&next);
      let done = Arc::clone(&done);
      let confirmed = Arc::clone(&confirmed);
      let superseded = Arc::clone(&superseded);
      let superseded_seeds = Arc::clone(&superseded_seeds);
      let partitions = Arc::clone(&partitions);
      let failover_seeds = Arc::clone(&failover_seeds);
      let precise_seeds = Arc::clone(&precise_seeds);
      let precise_total = Arc::clone(&precise_total);
      let resync_total = Arc::clone(&resync_total);
      let inherited_serves_total = Arc::clone(&inherited_serves_total);
      scope.spawn(move || {
        loop {
          let seed = next.fetch_add(1, Ordering::Relaxed);
          if seed >= end {
            break;
          }
          let res = panic::catch_unwind(|| sailing_simulation::run_vopr(seed, ticks));
          match res {
            Ok(rep) => {
              confirmed.fetch_add(rep.reads_confirmed, Ordering::Relaxed);
              superseded.fetch_add(rep.reads_served_by_superseded_leader, Ordering::Relaxed);
              if rep.reads_served_by_superseded_leader > 0 {
                superseded_seeds.fetch_add(1, Ordering::Relaxed);
              }
              partitions.fetch_add(rep.partitions, Ordering::Relaxed);
              if rep.reads_served_by_superseded_leader > 0 {
                eprintln!(
                  "  SUPERSEDED-SERVE seed {seed}: count={} drifted={} failover={} confirmed={}",
                  rep.reads_served_by_superseded_leader,
                  rep.drifted,
                  rep.failover,
                  rep.reads_confirmed
                );
              }
              if rep.failover {
                failover_seeds.fetch_add(1, Ordering::Relaxed);
                resync_total.fetch_add(rep.offset_resyncs, Ordering::Relaxed);
                precise_total.fetch_add(rep.precise_releases, Ordering::Relaxed);
                inherited_serves_total.fetch_add(rep.inherited_serves, Ordering::Relaxed);
                if rep.precise_releases > 0 {
                  precise_seeds.fetch_add(1, Ordering::Relaxed);
                  eprintln!(
                    "  FAILOVER-PRECISE seed {seed}: precise_releases={} offset_resyncs={} inherited_serves={} committed={} confirmed={}",
                    rep.precise_releases, rep.offset_resyncs, rep.inherited_serves, rep.committed, rep.reads_confirmed
                  );
                }
                // ASYMMETRIC sub-mode witness (the backward-only wall-contract violation): a failover seed
                // that FIRED a violating resync AND reached the inherited-serve path under it — the
                // candidates for `vopr_exercises_asymmetric_wall_injection`'s pinned seed set.
                if rep.violating_resyncs > 0 && rep.inherited_serves > 0 && rep.committed > 0 {
                  eprintln!(
                    "  ASYMMETRIC seed {seed}: violating_resyncs={} inherited_serves={} committed={} confirmed={}",
                    rep.violating_resyncs, rep.inherited_serves, rep.committed, rep.reads_confirmed
                  );
                }
              }
            }
            Err(payload) => {
              let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "<non-string panic>".into());
              let head: String = msg.lines().take(6).collect::<Vec<_>>().join("\n  ");
              failures.lock().unwrap().push((seed, head));
            }
          }
          done.fetch_add(1, Ordering::Relaxed);
        }
      });
    }
  });

  let mut f = failures.lock().unwrap();
  f.sort();
  eprintln!(
    "drift sweep [{start}..{end}) ticks={ticks}: {} runs, {} FAILURES",
    done.load(Ordering::Relaxed),
    f.len()
  );
  eprintln!(
    "  non-vacuity: reads_confirmed={} superseded_leader_serves={} (in {} seeds) partitions={}",
    confirmed.load(Ordering::Relaxed),
    superseded.load(Ordering::Relaxed),
    superseded_seeds.load(Ordering::Relaxed),
    partitions.load(Ordering::Relaxed),
  );
  eprintln!(
    "  failover: seeds={} (precise-firing={}) precise_releases={} offset_resyncs={} inherited_serves={}",
    failover_seeds.load(Ordering::Relaxed),
    precise_seeds.load(Ordering::Relaxed),
    precise_total.load(Ordering::Relaxed),
    resync_total.load(Ordering::Relaxed),
    inherited_serves_total.load(Ordering::Relaxed),
  );
  for (seed, msg) in f.iter() {
    eprintln!("  seed {seed}: {msg}");
  }
  if !f.is_empty() {
    std::process::exit(1);
  }
}
