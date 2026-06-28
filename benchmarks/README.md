# sailing benchmarks

Two cluster-throughput benchmarks for sailing-proto. Both use the in-memory log/state stores from
`sailing-simulation` and a bench-local counting state machine (`CountSm`) whose snapshot is O(1), so
what they measure is consensus work — append → replicate → commit → apply — not disk or wire I/O, and
not an FSM bookkeeping artifact.

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
  (`-b` pipelines `batch` proposals before awaiting). A single leader is elected and confirmed
  stable first; throughput is the committed put/s measured only over the load window. The window
  requires one stable leader and all `members` nodes alive throughout — a leadership change *or* any
  task (node or client) dying aborts the run as invalid (loud panic) rather than silently
  miscounting it (e.g. a degraded quorum reported under the full `members` label). The reported
  put/s is computed from the clients' *observed* committed-write count (equal to the configured
  total on a valid run), so the printed number is honest by construction. In the no-fault in-process
  cluster nothing aborts.
- **A pinned runtime (`-w`).** The bench builds its tokio runtime with an explicit worker count
  instead of `#[tokio::main]`'s default of one worker per CPU. This harness drives each node from a
  single serial task loop, so extra workers buy no parallelism — they only add cross-thread futex
  wakeups and work-stealing migration. Fewer workers measure consensus throughput rather than
  scheduler churn; on a busy multi-core box the per-CPU default is the *worst* case. `-w 2` is
  typically fastest here; openraft's harness runs ~16 worker threads, so sweep `-w` to compare
  like-for-like. Default `4`.

Arguments mirror openraft's (`-c` clients, `-n` operations, `-m` members 1/3/5, `-b` batch; counts
accept `_` separators and `k`/`m`/`g` suffixes) plus `-w` worker threads and `-g` independent groups
(sharding — see [below](#sharding-aggregate-throughput-with--g)):

```sh
cargo run --release -p sailing-benchmark --bin parity -- -c 16 -n 1m -m 3 -w 2
cargo run --release -p sailing-benchmark --bin parity -- -c 256 -n 2m -m 3 -b 4 -w 2
cargo run --release -p sailing-benchmark --bin parity -- -g 8 -c 16 -n 16m -m 3 -w 8  # 8-way sharding
```

**Measurement notes.** Two harness-only choices keep the number a read on consensus throughput:

- *Release profile.* The workspace root sets `[profile.release]` `lto = true` + `codegen-units = 1`,
  so the benches build with the same optimizations openraft's do (a workspace profile affects only
  this workspace — never a downstream embedder).
- *O(1)-snapshot FSM, compaction left on.* The benches run an FSM (`CountSm`) that keeps only a
  running count, so `snapshot()` is a fixed ~8 bytes. The simulation's `LogSm` instead re-encodes its
  whole (never-truncated) applied history on every `snapshot()` — an O(n) cost that compounds to O(n²)
  over a long run. That artifact is the *harness*, not sailing's proto (whose snapshot transfer is
  correctness-tested elsewhere). With the O(1) FSM the benches keep normal log compaction **on** at
  the default `snapshot_threshold`, so the log stays bounded to ~one threshold of entries: a bounded
  steady-state measurement whose put/s is stable across `-n` and comparable to a long openraft run
  (which also compacts) — rather than an unbounded log that drifts into an allocator/cache benchmark.

### Reference: openraft's published numbers

openraft's minimal harness reports roughly **33k put/s at 1 client**, **~1.8M put/s at 256
clients**, scaling to **~3.5M put/s at 4096 clients**, and **~5.6M put/s with batching** (`-b 4`).

### What `parity` shows for sailing

Indicative single-machine results (`-m 3`, `-w 2`, ~10-core dev machine — absolute numbers are
hardware- and load-dependent; treat them as a methodology comparison, not a leaderboard):

| config              | put/s  |
| ------------------- | ------ |
| `-c 1`              | ~305k  |
| `-c 16`             | ~600k  |
| `-c 256`            | ~560k  |
| `-c 256 -b 4`       | ~820k  |
| `-c 16 -m 1`        | ~2.3M  |
| `-c 16 -m 5`        | ~410k  |

The shape is what the architecture predicts: smaller clusters go faster, single-client latency is
low, and batching pays off once enough clients are in flight (`-c 256`: ~560k → ~820k at `-b 4`).
sailing's parity harness drives each node from one serial task loop, so throughput saturates against
that per-node loop sooner than openraft (whose per-node Raft fans work across several internal worker
threads) — i.e. it scales less steeply with client concurrency while starting from a far higher
single-client point (~305k vs ~33k).

The runtime worker count matters because of that serial-per-node loop. Sweeping `-w` at `-c 16 -m 3`
(all else equal):

| `-w`                          | put/s  |
| ----------------------------- | ------ |
| `1`                           | ~600k  |
| `2`                           | ~620k  |
| `4`                           | ~575k  |
| `8`                           | ~490k  |
| `10` (one worker per CPU)     | ~480k  |

Fewer workers win: the old `#[tokio::main]` per-CPU default (~10 workers here) spent extra time on
cross-thread wakeups and work-stealing, so pinning to `-w 2` is ~1.3× faster. Tune `-w` to the box —
openraft runs ~16. (Together with the O(1)-snapshot FSM and the release profile, these measurement
fixes lifted the original `-c 16 -m 3` number — which also carried the now-removed O(n²)-snapshot
artifact — from ~250k to ~600k.)

### Sharding: aggregate throughput with `-g`

sailing's consensus is **serial per Raft group** — the Sans-I/O core turns one crank at a time, so a
single group is worth roughly one core of consensus (the `-w` sweep above shows extra workers don't
speed *one* group up). Production multi-Raft systems (TiKV, CockroachDB) don't chase higher throughput
by parallelizing one group; they **shard** the keyspace into many independent Raft groups and run them
concurrently, ~one per core. `-g K` measures exactly that: K fully independent groups — each its own
node set, channels, leader, and integrity guards — driven concurrently over **one shared timed
window**. `-n` is the total op budget across all groups (distributed *exactly* — the remainder is
spread across the first groups and, within a group, across its clients, so the aggregate committed
count equals `-n` with no rounding loss) and the reported `put/s` is the **aggregate**: every group's
committed ops over the one wall clock. Every group stays guarded *and* running for the whole shared
window — a finished group's nodes keep running and its guards stay armed until the single global
elapsed timestamp is taken — so any single group's leader change or node death (at any point in the
window) aborts the whole run, and the aggregate is summed from the groups' *observed* commits, so the
number stays honest by construction.

Weak-scaling sweep — each group commits a constant 2M ops (so `-n = 2M × K`), `-c 16 -m 3`, one worker
per group (`-w = K`), on a ~10-core dev machine (8 performance + 2 efficiency cores). Absolute numbers
are machine- and load-dependent; past the core count, scheduler oversubscription makes the aggregate
noisy, so a representative range from repeated samples is shown there:

| groups `-g` | `-w` | aggregate put/s     | per-group put/s |
| ----------- | ---- | ------------------- | --------------- |
| `1`         | `1`  | ~0.52M              | ~0.52M          |
| `2`         | `2`  | ~1.0M               | ~0.5M           |
| `4`         | `4`  | ~1.8M               | ~0.45M          |
| `8`         | `8`  | ~2.3–2.8M           | ~0.3M           |
| `12`        | `12` | ~2.1–3.1M (noisy)   | saturated       |
| `16`        | `16` | ~2.6–3.4M (noisy)   | saturated       |

```sh
cargo run --release -p sailing-benchmark --bin parity -- -g 8 -c 16 -n 16m -m 3 -w 8
```

Aggregate throughput scales with the group count while spare cores exist — **1 group ~0.52M → 4 groups
~1.8M (~3.5×)** — because each group's serial leader gets its own core. Around the core count (`-g 8`
on this 10-core box) it reaches ~2.3–2.8M; pushing further oversubscribes the scheduler, so `-g 12`/
`-g 16` saturate at a noisy ~2–3.4M peak — per-group throughput collapses under work-stealing churn and
the aggregate varies run-to-run. The knee is the hardware, not the protocol: give each group its own
core (`-w = K`) and the aggregate tracks K until the cores run out. (`-g 1`, the default, is exactly
the single-group benchmark above.)

This is how sailing reaches toward **openraft-class aggregate throughput**: openraft's minimal harness
peaks around ~3.5M put/s by fanning *one* group's work across several internal worker threads; sailing's
deliberately serial core approaches the same few-million range by **sharding into independent groups
across the cores** — the architecture production multi-Raft deployments actually run, and the answer to
high concurrency that a Sans-I/O design is built for.

---

## `pure_core` — the Sans-I/O core in isolation

Strips the async framework entirely: it drives an N-node cluster synchronously on one thread, with no
runtime, no channels, and no wire codec. After electing a leader it **freezes virtual time** and
loops propose → drain, so the measured put/s is bounded purely by how fast the core can exchange
messages and apply entries — the lower bound on per-op consensus cost, free of scheduler and channel
overhead.

```sh
cargo run --release -p sailing-benchmark --bin pure_core -- -m 3 -n 1m
cargo run --release -p sailing-benchmark --bin pure_core -- -m 3 -n 2m -b 16
```

`-b` is the in-flight depth (`1` = latency-bound, one proposal outstanding; larger = pipelined).

Indicative single-machine results (~10-core dev machine):

| config         | put/s  |
| -------------- | ------ |
| `-m 3 -b 1`    | ~935k  |
| `-m 3 -b 16`   | ~985k  |
| `-m 3 -b 256`  | ~625k  |
| `-m 1 -b 1`    | ~5.3M  |
| `-m 1 -b 256`  | ~3.8M  |

A single thread commits ~935k put/s through a 3-node cluster, peaking near ~985k at `-b 16`; very
large batches trade back down as each message carries more entries to process. A single-node cluster
reaches ~5.3M. With compaction left on (the O(1)-snapshot FSM) the log stays bounded, so these
numbers are stable across `-n` — `-m 3 -b 1` holds ~935k from `-n 500k` to `-n 5m`. Because this
harness runs everything on one thread with virtual time frozen, it is the cleanest read on the
consensus logic's intrinsic cost; the `parity` numbers then show what that core sustains once a real
async runtime, channels, and client concurrency are layered on top.
