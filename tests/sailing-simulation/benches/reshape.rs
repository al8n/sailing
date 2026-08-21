//! Reshaping-cost microbenchmarks: the per-op price of the two state-machine reshaping seams and
//! of a fork's manufactured-baseline install, isolated from consensus and I/O.
//!
//! Three groups, all over the sim's keyed [`LogSm`] (the FSM whose `split` partitions its applied
//! record by a `u16` key point and whose `absorb` folds a source's record back in):
//!
//!   - `fork_latency/N` times [`StateMachine::split`] ALONE on an `N`-cell FSM — the O(N) partition
//!     walk (a `decode_gkv` + side test per cell, plus the moved half's allocation).
//!   - `absorb_cost/N` times [`StateMachine::absorb`] ALONE folding an `N`-cell source into a fresh
//!     target — the O(N) record append.
//!   - `fork_e2e/N` times [`MultiRaft::create_group_from_fork`] on a bare one-host container — the
//!     manufactured-baseline install (persist the `N`-cell blob + restart the child endpoint) that
//!     a committed split drives on every unoccupied host.
//!
//! # Sizes
//!
//! `fork_latency`/`absorb_cost` run `{1e3, 1e5, 1e6}` and `fork_e2e` runs `{1e3, 1e5}`. The plan's
//! top size for the first pair was `1e7`; it is dropped to `1e6` because `iter_batched` clones the
//! `N`-cell FSM once per iteration, and a 1e7-cell `LogSm` (~0.6 GB per clone) makes the batched
//! setup memory- and time-pathological well past the 60s-per-sample-set budget. 1e6 already shows
//! the linear cost cleanly.

#![allow(missing_docs)] // criterion_group! generates an undocumented public item

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use sailing_proto::{Config, Index, Instant, MultiRaft, StateMachine};
use sailing_simulation::{LogSm, MemLog, MemStable};
use std::{hint::black_box, time::Duration};

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// The per-group gkv key domain, mirrored from the sim's `multi::NUM_KEYS` (that constant is
/// crate-private, so the bench pins the same value): keyed cells cycle `0..NUM_KEYS`, and a split
/// point at the midpoint moves ~half the record to the child.
const NUM_KEYS: u16 = 8;
/// The parent group id every populated cell is tagged for (`gkv`'s tag), and the fork child id.
const GID: u64 = 100;
const CHILD: u64 = 200;
const SEED: u64 = 0xB0A7;
/// The split point: keys `>= MID` move to the child, so with keys spread over `0..NUM_KEYS` a split
/// hands the child ~half the record — a realistic partition, not a degenerate all/nothing one.
const MID: u16 = NUM_KEYS / 2;

/// The sim's 18-byte gid-tagged keyed-value command: `gid` (8 LE) ++ `key` (2 LE) ++ `value`
/// (8 LE). Replicated here (the sim's `encode_gkv` is crate-private) so `LogSm::split`'s
/// `decode_gkv` recognises each cell and partitions by its key.
fn gkv(gid: u64, key: u16, value: u64) -> Bytes {
  let mut buf = Vec::with_capacity(18);
  buf.extend_from_slice(&gid.to_le_bytes());
  buf.extend_from_slice(&key.to_le_bytes());
  buf.extend_from_slice(&value.to_le_bytes());
  Bytes::from(buf)
}

/// An `n`-cell keyed `LogSm`: `n` committed gkv cells, keys cycling `0..NUM_KEYS` so a midpoint
/// split partitions ~half. Indices are `1..=n`, exactly as an applied log would carry them.
fn keyed_logsm(n: u64) -> LogSm {
  let mut sm = LogSm::new();
  for i in 0..n {
    let key = (i % u64::from(NUM_KEYS)) as u16;
    sm.apply(Index::new(i + 1), gkv(GID, key, i))
      .expect("LogSm::apply is infallible on a well-formed command");
  }
  sm
}

/// A single-voter parent config on node 0 — the one-host container shape the fork e2e drives
/// (`create_group_from_fork` needs a valid config; no election runs).
fn single_voter_config() -> Config<u64> {
  Config::try_new(0, std::vec![0], ELECTION, HEARTBEAT).expect("valid single-voter config")
}

/// `StateMachine::split` alone, on an `N`-cell FSM cloned fresh per iteration (so each timed call
/// partitions a pristine record, and the child it produces is dropped outside the measurement).
fn bench_fork_latency(c: &mut Criterion) {
  let mut g = c.benchmark_group("fork_latency");
  for n in [1_000u64, 100_000, 1_000_000] {
    g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
      let base = keyed_logsm(n);
      let point = MID.to_le_bytes();
      b.iter_batched(
        || base.clone(),
        |mut sm| black_box(sm.split(black_box(&point))),
        BatchSize::LargeInput,
      );
    });
  }
  g.finish();
}

/// `StateMachine::absorb` alone: fold an `N`-cell source into a fresh (empty) target. Both are
/// cloned per iteration; the source is consumed by the absorb, the mutated target black-boxed.
fn bench_absorb_cost(c: &mut Criterion) {
  let mut g = c.benchmark_group("absorb_cost");
  for n in [1_000u64, 100_000, 1_000_000] {
    g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
      let source = keyed_logsm(n);
      b.iter_batched(
        || (LogSm::new(), source.clone()),
        |(mut target, source)| {
          black_box(target.absorb(black_box(source)));
          black_box(target)
        },
        BatchSize::LargeInput,
      );
    });
  }
  g.finish();
}

/// `MultiRaft::create_group_from_fork` on a bare one-host container: install an `N`-cell
/// manufactured baseline (persist the blob + restart the child endpoint) into virgin stores. The
/// `N`-cell FSM and its snapshot blob are built once; each iteration gets a fresh host + virgin
/// stores (the call consumes the FSM/blob and admits the child), so it times the install alone.
fn bench_fork_e2e(c: &mut Criterion) {
  let mut g = c.benchmark_group("fork_e2e");
  for n in [1_000u64, 100_000] {
    g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
      let fsm = keyed_logsm(n);
      let snapshot = fsm.snapshot().expect("LogSm snapshots infallibly");
      b.iter_batched(
        || {
          (
            MultiRaft::<u64, u64, LogSm>::default(),
            fsm.clone(),
            snapshot.clone(),
            MemLog::new(),
            MemStable::<u64>::new(),
          )
        },
        |(mut host, fsm, snapshot, mut log, mut stable)| {
          host
            .create_group_from_fork(
              CHILD,
              1,
              single_voter_config(),
              Instant::ORIGIN,
              SEED,
              fsm,
              snapshot,
              None,
              1,
              &mut log,
              &mut stable,
            )
            .expect("fork materialization admits on virgin stores");
          black_box(host);
        },
        BatchSize::LargeInput,
      );
    });
  }
  g.finish();
}

criterion_group!(
  benches,
  bench_fork_latency,
  bench_absorb_cost,
  bench_fork_e2e
);
criterion_main!(benches);
