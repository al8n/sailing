//! A parallel reshape-profile seed sweep — runs `run_multi_vopr` under one of the four phase-biased
//! storm profiles across a band of seeds on a worker pool, catching each run's panic so one failing
//! seed does not abort the rest, and reporting the failing seeds alongside the reshape non-vacuity
//! witnesses and the tracked under-hosted parked-absorb (#106) exemption count. Not a gated test (it
//! is unbounded by design); the gated coverage lives in `tests/multi_vopr_profiles.rs`. Use it for
//! deep confidence sweeps and for finding fresh reshape seeds.
//!
//! A COMPLETED seed carrying `tracked_merge_wedges_exempted > 0` hit the FILED #106 shape and was
//! certified past — it is reported under "tracked-exempt seeds", NOT as a failure. A seed that
//! PANICS is a genuine failure to adjudicate (a novel wedge or oracle trip), reported with its
//! replay head.
//!
//! Usage: cargo run --release --example reshape_sweep -- <profile> <start> <end> <ticks> <threads>
//!   <profile> ∈ { split_storm, merge_storm, mixed_reshape, lifecycle_churn_reshape }
use sailing_simulation::{MultiProfile, run_multi_vopr};
use std::{
  panic,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
  },
};

/// Resolve a storm profile by name (the four phase-biased profiles; unknown ⇒ `None`).
fn resolve(name: &str) -> Option<MultiProfile> {
  Some(match name {
    "split_storm" => MultiProfile::split_storm(),
    "merge_storm" => MultiProfile::merge_storm(),
    "mixed_reshape" => MultiProfile::mixed_reshape(),
    "lifecycle_churn_reshape" | "lifecycle" => MultiProfile::lifecycle_churn_reshape(),
    _ => return None,
  })
}

fn main() {
  let a: Vec<String> = std::env::args().collect();
  let name = a.get(1).cloned().unwrap_or_default();
  let start: u64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
  let end: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(64);
  let ticks: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1_500);
  let threads: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(4);

  let Some(profile) = resolve(&name) else {
    eprintln!(
      "unknown profile {name:?}\nusage: reshape_sweep <profile> <start> <end> <ticks> <threads>\n  \
       <profile> ∈ {{ split_storm, merge_storm, mixed_reshape, lifecycle_churn_reshape }}"
    );
    std::process::exit(2);
  };

  // Reject the degenerate configurations that would otherwise exit GREEN having run nothing: an empty
  // band (`end <= start`), a zero tick budget (vacuous runs), or a zero-thread pool (no worker ever
  // draws a seed). A silent no-op sweep must never read as a clean pass.
  if end <= start || ticks == 0 || threads == 0 {
    eprintln!(
      "invalid sweep bounds: start={start} end={end} ticks={ticks} threads={threads}\n  \
       require end > start, ticks > 0, threads > 0\nusage: reshape_sweep <profile> <start> <end> \
       <ticks> <threads>"
    );
    std::process::exit(2);
  }

  panic::set_hook(Box::new(|_| {})); // quiet — only our summary prints

  let failures: Arc<Mutex<Vec<(u64, String)>>> = Arc::new(Mutex::new(Vec::new()));
  // Seeds that COMPLETED carrying the tracked #106 exemption — reported, not failed.
  let exempt_seeds: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
  let next = Arc::new(AtomicU64::new(start));
  let done = Arc::new(AtomicU64::new(0));
  // Reshape non-vacuity witnesses aggregated across the sweep.
  let splits = Arc::new(AtomicU64::new(0));
  let merges_registered = Arc::new(AtomicU64::new(0));
  let merges_prepared = Arc::new(AtomicU64::new(0));
  let aborts = Arc::new(AtomicU64::new(0));
  let groups_removed = Arc::new(AtomicU64::new(0));
  let groups_recreated = Arc::new(AtomicU64::new(0));
  let committed = Arc::new(AtomicU64::new(0));
  // The two FILED merge-liveness classes, counted and seed-listed APART so neither hides the other.
  let exempted = Arc::new(AtomicU64::new(0)); // #106 under-hosted
  let forkfence = Arc::new(AtomicU64::new(0)); // #110 fork-fence coupling
  let forkfence_seeds: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

  std::thread::scope(|scope| {
    for _ in 0..threads {
      let failures = Arc::clone(&failures);
      let exempt_seeds = Arc::clone(&exempt_seeds);
      let next = Arc::clone(&next);
      let done = Arc::clone(&done);
      let splits = Arc::clone(&splits);
      let merges_registered = Arc::clone(&merges_registered);
      let merges_prepared = Arc::clone(&merges_prepared);
      let aborts = Arc::clone(&aborts);
      let groups_removed = Arc::clone(&groups_removed);
      let groups_recreated = Arc::clone(&groups_recreated);
      let committed = Arc::clone(&committed);
      let exempted = Arc::clone(&exempted);
      let forkfence = Arc::clone(&forkfence);
      let forkfence_seeds = Arc::clone(&forkfence_seeds);
      scope.spawn(move || {
        loop {
          let seed = next.fetch_add(1, Ordering::Relaxed);
          if seed >= end {
            break;
          }
          let res = panic::catch_unwind(|| run_multi_vopr(seed, ticks, profile));
          match res {
            Ok(rep) => {
              splits.fetch_add(rep.splits_applied, Ordering::Relaxed);
              merges_registered.fetch_add(rep.merges_registered, Ordering::Relaxed);
              merges_prepared.fetch_add(rep.merges_prepared, Ordering::Relaxed);
              aborts.fetch_add(
                rep.merges_rolled_back + rep.merges_aborted,
                Ordering::Relaxed,
              );
              groups_removed.fetch_add(rep.groups_removed, Ordering::Relaxed);
              groups_recreated.fetch_add(rep.groups_recreated, Ordering::Relaxed);
              committed.fetch_add(rep.committed, Ordering::Relaxed);
              if rep.tracked_merge_wedges_exempted > 0 {
                exempted.fetch_add(rep.tracked_merge_wedges_exempted, Ordering::Relaxed);
                exempt_seeds.lock().unwrap().push(seed);
                eprintln!(
                  "  UNDERHOSTED-EXEMPT seed {seed}: tracked_merge_wedges_exempted={} (filed #106) \
                   registered={} committed={}",
                  rep.tracked_merge_wedges_exempted, rep.merges_registered, rep.committed
                );
              }
              if rep.fork_fence_couplings_exempted > 0 {
                forkfence.fetch_add(rep.fork_fence_couplings_exempted, Ordering::Relaxed);
                forkfence_seeds.lock().unwrap().push(seed);
                eprintln!(
                  "  FORK-FENCE-EXEMPT seed {seed}: fork_fence_couplings_exempted={} (filed #110) \
                   registered={} committed={}",
                  rep.fork_fence_couplings_exempted, rep.merges_registered, rep.committed
                );
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
  let mut ex = exempt_seeds.lock().unwrap();
  ex.sort();
  let mut ff = forkfence_seeds.lock().unwrap();
  ff.sort();
  eprintln!(
    "reshape sweep [{name}] [{start}..{end}) ticks={ticks}: {} runs, {} FAILURES, {} underhosted-exempt (#106), {} fork-fence-exempt (#110)",
    done.load(Ordering::Relaxed),
    f.len(),
    ex.len(),
    ff.len(),
  );
  eprintln!(
    "  reshape witnesses: splits_applied={} merges_registered={} merges_prepared={} aborts={} \
     groups_removed={} groups_recreated={} committed={}",
    splits.load(Ordering::Relaxed),
    merges_registered.load(Ordering::Relaxed),
    merges_prepared.load(Ordering::Relaxed),
    aborts.load(Ordering::Relaxed),
    groups_removed.load(Ordering::Relaxed),
    groups_recreated.load(Ordering::Relaxed),
    committed.load(Ordering::Relaxed),
  );
  eprintln!(
    "  underhosted #106: exemptions={} in seeds {:?}",
    exempted.load(Ordering::Relaxed),
    ex.as_slice(),
  );
  eprintln!(
    "  fork-fence #110: exemptions={} in seeds {:?}",
    forkfence.load(Ordering::Relaxed),
    ff.as_slice(),
  );
  for (seed, msg) in f.iter() {
    eprintln!("  FAIL seed {seed}: {msg}");
  }
  if !f.is_empty() {
    std::process::exit(1);
  }
  // Never report success without having actually swept the whole band: every seed in `[start, end)`
  // must have completed. Restore the default panic hook first so this safety net is visible if it
  // ever fires (the validated bounds above make it a `should-never-happen`, not a routine path).
  let _ = panic::take_hook();
  let completed = done.load(Ordering::Relaxed);
  assert_eq!(
    completed,
    end - start,
    "sweep incomplete: {completed} of {} seeds ran — refusing the green exit",
    end - start,
  );
}
