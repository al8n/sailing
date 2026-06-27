# sailing benchmarks

Two cluster-throughput benchmarks for sailing-proto. Both use the in-memory log/state stores from
`sailing-simulation` and a counting state machine (`LogSm`), so what they measure is consensus work
— append → replicate → commit → apply — not disk or wire I/O.

| bench       | what it measures                                          | runtime           |
| ----------- | --------------------------------------------------------- | ----------------- |
| `parity`    | cluster throughput the way openraft's harness measures it | tokio, N+N tasks  |
| `pure_core` | the Sans-I/O consensus core's raw cost, nothing else      | synchronous, 1 thread |

Build both:

```sh
cargo build --release -p sailing-benchmark --bins
```

---

## `parity` — apples-to-apples with openraft

Mirrors the method of openraft's [`benchmarks/minimal`](https://github.com/databendlabs/openraft/tree/main/benchmarks)
so the two numbers are directly comparable:

- **N concurrent node tasks.** One tokio task per node owns its `Endpoint` + in-memory stores and
  hand-drives the Sans-I/O crank (sailing nodes are not self-driving): on each wake it feeds the
  inbound message or fires a due timer, pumps storage to quiescence (persist-before-ack / -vote),
  routes the produced messages to peers, and drains the applied events.
- **A typed-message channel "network".** Peers exchange `Message<u64>` over per-node channels — no
  serialization, no sockets. This is the same shortcut openraft takes by calling the peer's `Raft`
  handle directly; it isolates consensus cost from transport cost.
- **N client tasks.** Each proposes to the leader and awaits the commit+apply of its own write
  (`-b` pipelines `batch` proposals before awaiting). A leader is elected first; throughput is the
  committed put/s measured only over the load window.

Arguments mirror openraft's (`-c` clients, `-n` operations, `-m` members 1/3/5, `-b` batch; counts
accept `_` separators and `k`/`m`/`g` suffixes):

```sh
cargo run --release -p sailing-benchmark --bin parity -- -c 16 -n 200000 -m 3
cargo run --release -p sailing-benchmark --bin parity -- -c 4096 -n 20m -m 3 -b 4
```

### Reference: openraft's published numbers

openraft's minimal harness reports roughly **33k put/s at 1 client**, scaling to **~3.5M put/s at
4096 clients**, and **~5.6M put/s with batching** (`-b 4`).

### What `parity` shows for sailing

Indicative single-machine results (`-n 200000 -m 3`, dev laptop — absolute numbers are
hardware- and load-dependent; treat them as a methodology comparison, not a leaderboard):

| config                  | put/s    |
| ----------------------- | -------- |
| `-c 1`                  | ~185k    |
| `-c 16`                 | ~360–390k|
| `-c 256`                | ~440k    |
| `-c 16 -b 4`            | ~530k    |
| `-c 16 -m 1`            | ~1.3M    |
| `-c 16 -m 5`            | ~245k    |

The shape is what the architecture predicts: smaller clusters and larger batches go faster, and
single-client latency is low. sailing's parity harness drives each node from one serial task loop,
so throughput saturates against that per-node loop sooner than openraft (whose per-node Raft fans
work across several internal worker threads) — i.e. it scales less steeply with client concurrency
while starting from a higher single-client point.

---

## `pure_core` — the Sans-I/O core in isolation

Strips the async framework entirely: it drives an N-node cluster synchronously on one thread, with no
runtime, no channels, and no wire codec. After electing a leader it **freezes virtual time** and
loops propose → drain, so the measured put/s is bounded purely by how fast the core can exchange
messages and apply entries — the lower bound on per-op consensus cost, free of scheduler and channel
overhead.

```sh
cargo run --release -p sailing-benchmark --bin pure_core -- -m 3 -n 200000
cargo run --release -p sailing-benchmark --bin pure_core -- -m 3 -n 500000 -b 16
```

`-b` is the in-flight depth (`1` = latency-bound, one proposal outstanding; larger = pipelined).

Indicative single-machine results: **~530k put/s** at `-m 3 -b 1` (the ~500k–600k single-threaded
in-process range), and **~1.9M put/s** at `-m 1`. Because this harness runs everything on one thread
with virtual time frozen, it is the cleanest read on the consensus logic's intrinsic cost; the
`parity` numbers then show what that core sustains once a real async runtime, channels, and client
concurrency are layered on top.
