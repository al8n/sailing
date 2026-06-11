<div align="center">
<h1>sailing</h1>
</div>
<div align="center">

A Sans-I/O [Raft](https://raft.github.io/) consensus library for Rust — `no_std` + `alloc`, deterministic, and fuzz-hardened.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/sailing-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2F327b2a8aef9003246e45c6e47fe63937%2Fraw%2Fsailing" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/sailing/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/sailing?style=for-the-badge&logo=codecov" height="22">][codecov-url]
<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0/MIT-blue.svg?style=for-the-badge" height="22">

English | [简体中文][zh-cn-url]

</div>

`sailing-proto` is the consensus core: a pure state machine with **no I/O, no clock, no
threads, and no runtime** — the embedding application supplies storage, a monotonic `now`, and
message delivery, and drives the endpoint through a small `handle_*` / `poll_*` surface. The same
state machine therefore runs identically under tokio, compio, embassy, bare metal, or a
deterministic simulator.

## Status

**Pre-release.** The consensus core and the TCP/TLS stream transport are implemented and heavily
tested (see *Correctness* below); the QUIC transport and a reference async driver are in progress.
The wire format and public API may still change until 0.1.

## Features

- **Full Raft**: leader election (with PreVote and CheckQuorum), log replication with flow
  control and byte-capped batching, joint-consensus membership changes (etcd-style `ConfChangeV2`),
  leadership transfer, and snapshot install/restore.
- **Linearizable reads**: ReadIndex (leader and follower-forwarded) and an optional,
  self-validating lease-based fast path.
- **Crash-safe by construction**: persist-before-ack durability ordering throughout — a node
  never externalizes state (votes, acks, lease promises, snapshot re-baselines) before the backing
  write is durable, and restart reconciles durable state through a pure, exhaustively-tested
  recovery function. Unrecoverable faults fail-stop (poison) rather than corrupt.
- **Sans-I/O transport** (feature-gated): a framed reliable-stream "coordinator" that drives the
  endpoint over TCP (`tcp`, itself `no_std`-capable) or TLS 1.2/1.3 via rustls (`tls`), with a
  cluster/peer-identity handshake, per-peer routing, bounded buffers everywhere, and
  driver-friendly connection-lifecycle reporting.
- **`no_std` + `alloc`** core with zero required dependencies beyond `bytes`/`thiserror`/
  `derive_more`; `std` is only needed by the TLS/QUIC transports.

## Correctness

The library is developed against a deterministic simulation harness
(`tests/sailing-simulation`) in the spirit of FoundationDB/TigerBeetle:

- a **VOPR-style fuzzer** composes crashes, fsync-window loss, network partitions, message
  drop/duplication/reorder, and membership churn from a single seed, with a **per-tick safety
  oracle suite** (agreement, append-before-ack, quorum-durable commits, monotonicity, no committed
  rewrites) asserting on every tick — any failure replays bit-identically from `seed`;
- a **data-driven interaction corpus** ported from etcd's raft tests pins golden traces;
- the **wire codec is fuzzed in-path**: with the harness's `wire` feature, every message the
  fuzzer delivers round-trips through the real encode/decode;
- dozens of adversarial review rounds (with mutation-verified regression tests) hardened the
  durability ordering, the read paths, and the transport against hostile input.

## Example (shape of the API)

```rust,ignore
use sailing_proto::{Config, Endpoint, Instant};

let config = Config::try_new(my_id, voters, election_timeout, heartbeat_interval)?;
let mut node = Endpoint::new(config, Instant::ORIGIN, seed, my_state_machine);

// The driver loop: feed inputs, drain outputs.
node.handle_message(now, &mut log, &mut stable, from, msg); // a peer's message arrived
node.handle_timeout(now, &mut log, &mut stable);            // a deadline fired
node.handle_storage(now, &mut log, &mut stable);            // a storage write completed
while let Some(out) = node.poll_message() { /* send out.to() <- out.message() */ }
while let Some(ev) = node.poll_event() { /* committed entries, read states, ... */ }
let next_deadline = node.poll_timeout();
```

With the `tcp`/`tls` features, `StreamCoordinator` wraps the endpoint together with framing,
record layers (`Passthrough`, `TlsRecords`), the identity handshake (`Labeled`), and per-peer
connection routing — the driver only moves bytes between sockets and the coordinator.

## Feature flags

| flag | what it enables | std? |
|------|-----------------|------|
| `std` *(default)* | std for the core | std |
| `alloc` | the `no_std` heap tier | no_std |
| `tcp` | the framed stream transport (no crypto) | **no_std + alloc OK** |
| `tls` | `tcp` + rustls record layer (+ `tls-ring` / `tls-aws-lc-rs` providers) | std |
| `quic` | reserved — the QUIC coordinator (quinn-proto) is not yet implemented | std |

The byte-level wire format is pinned in
[`sailing-proto/WIRE.md`](sailing-proto/WIRE.md) (normative, with golden vectors in the tests).

MSRV: **1.85**.

#### License

`sailing` is under the terms of both the MIT license and the Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/sailing/
[CI-url]: https://github.com/al8n/sailing/actions/workflows/ci.yml
[codecov-url]: https://app.codecov.io/gh/al8n/sailing/
[zh-cn-url]: https://github.com/al8n/sailing/tree/main/README-zh_CN.md
