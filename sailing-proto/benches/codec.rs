//! Wire-codec benchmarks: a baseline for the buffa protobuf envelope and the `Data` id seam.
//!
//! Encode and decode of representative messages across the sizes that matter — the small
//! steady-state heartbeat, the replication `AppendEntries` at growing entry counts (where the
//! varint-vs-fixed-width cost delta shows), and the large `InstallSnapshot`. A regression here
//! (or a buffa upgrade that shifts the cost profile) shows as a criterion change.

#![allow(missing_docs)] // criterion_group! generates an undocumented public item

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use sailing_proto::{
  AppendEntries, Data, Entry, EntryKind, Heartbeat, HeartbeatResp, Index, InstallSnapshot, Message,
  RequestVote, SnapshotMeta, Term, conf::ConfState, wire,
};
use std::{collections::BTreeSet, hint::black_box, time::Duration};

/// One small Normal entry with a `bytes`-byte command.
fn entry(i: u64, bytes: usize) -> Entry {
  Entry::new(
    Term::new(7),
    Index::new(i),
    EntryKind::Normal,
    Bytes::from(vec![0xAB; bytes]),
  )
}

fn append_entries(n: usize) -> Message<u64> {
  let entries = (1..=n as u64).map(|i| entry(i, 16)).collect();
  Message::AppendEntries(AppendEntries::new(
    Term::new(7),
    1,
    Index::new(100),
    Term::new(6),
    entries,
    Index::new(99),
  ))
}

fn install_snapshot() -> Message<u64> {
  // A joint-config snapshot with a 4 KiB blob — the largest envelope shape.
  let conf = ConfState::new(
    std::vec![1u64, 2, 3, 4, 5],
    std::vec![9u64],
    std::vec![1u64, 2, 3],
    std::vec![7u64],
    true,
  );
  Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(7),
    1,
    SnapshotMeta::new(Index::new(5000), Term::new(6), conf),
    Bytes::from(vec![0xCD; 4096]),
  ))
}

fn corpus() -> Vec<(&'static str, Message<u64>)> {
  std::vec![
    (
      "heartbeat",
      Message::Heartbeat(Heartbeat::new(
        Term::new(7),
        1,
        Index::new(99),
        Bytes::new()
      )),
    ),
    (
      "heartbeat_resp_lease",
      Message::HeartbeatResp(
        HeartbeatResp::new(Term::new(7), 2, Bytes::from_static(b"ctx"))
          .with_lease_round(3)
          .with_lease_support(Duration::from_millis(150)),
      ),
    ),
    (
      "request_vote",
      Message::RequestVote(RequestVote::new(
        Term::new(8),
        1,
        Index::new(100),
        Term::new(7),
        true,
        false,
      )),
    ),
    ("append_0", append_entries(0)),
    ("append_8", append_entries(8)),
    ("append_64", append_entries(64)),
    ("install_snapshot", install_snapshot()),
  ]
}

fn bench_encode(c: &mut Criterion) {
  let mut g = c.benchmark_group("encode_message");
  for (name, msg) in corpus() {
    g.bench_function(name, |b| {
      b.iter(|| {
        let mut buf = std::vec::Vec::new();
        wire::encode_message(black_box(&msg), &mut buf);
        buf
      });
    });
  }
  g.finish();
}

fn bench_decode(c: &mut Criterion) {
  let mut g = c.benchmark_group("decode_message");
  for (name, msg) in corpus() {
    let mut buf = std::vec::Vec::new();
    wire::encode_message(&msg, &mut buf);
    let frame = Bytes::from(buf);
    g.bench_function(name, |b| {
      // The `Bytes` clone is an O(1) refcount bump (the decode consumes its handle); it does
      // not dominate the measured decode work.
      b.iter(|| wire::decode_message::<u64>(black_box(frame.clone())));
    });
  }
  g.finish();
}

fn bench_data_seam(c: &mut Criterion) {
  fn roundtrip<T: Data + Clone>(
    g: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    v: T,
  ) {
    g.bench_function(format!("encode/{name}"), |b| {
      b.iter(|| {
        let mut buf = std::vec::Vec::new();
        black_box(&v).encode(&mut buf);
        buf
      });
    });
    let mut buf = std::vec::Vec::new();
    v.encode(&mut buf);
    let bytes = Bytes::from(buf);
    g.bench_function(format!("decode/{name}"), |b| {
      b.iter(|| T::decode_exact(black_box(bytes.clone())));
    });
  }

  let mut g = c.benchmark_group("data");
  roundtrip(&mut g, "u64", 0x0123_4567_89AB_CDEFu64);
  roundtrip(&mut g, "bytes_64", Bytes::from(vec![0u8; 64]));
  roundtrip(&mut g, "vec_u64_8", (0..8u64).collect::<Vec<u64>>());
  roundtrip(&mut g, "vec_u64_256", (0..256u64).collect::<Vec<u64>>());
  roundtrip(
    &mut g,
    "btreeset_u64_64",
    (0..64u64).collect::<BTreeSet<u64>>(),
  );
  g.finish();
}

criterion_group!(benches, bench_encode, bench_decode, bench_data_seam);
criterion_main!(benches);
