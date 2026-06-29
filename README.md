<div align="center">

<img src="https://raw.githubusercontent.com/al8n/sailing/main/art/logo.png" height = "200px">

<h1>sailing</h1>
</div>
<div align="center">

A Sans-I/O [Raft](https://raft.github.io/) consensus library for Rust — `no_std` + `alloc`, deterministic, and fuzz-hardened.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/sailing-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
<img alt="LoC" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fgist.githubusercontent.com%2Fal8n%2F327b2a8aef9003246e45c6e47fe63937%2Fraw%2Fatomic-time" height="22">
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/sailing/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/sailing?style=for-the-badge&token=6R3QFWRWHL&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-sailing-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/sailing?style=for-the-badge&logo=rust" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/sailing?color=critical&logo=rust&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-MPL%202.0-blue.svg?style=for-the-badge&fontColor=white&logoColor=ffffff&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPHN2ZyBpZD0iX+WbvuWxgl8xIiBkYXRhLW5hbWU9IuWbvuWxgiAxIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0NzMuNDcgMjU1LjEyMiI+CiAgPGRlZnM+CiAgICA8c3R5bGU+CiAgICAgIC5jbHMtMSB7CiAgICAgICAgZmlsbDogI2ZmZjsKICAgICAgICBzdHJva2Utd2lkdGg6IDBweDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPHBvbHlnb24gY2xhc3M9ImNscy0xIiBwb2ludHM9IjM0MC4wNjUgLjQ4NCAzMzQuMzczIDEuMjA5IDMyOC45MjkgMi4xNzUgMzIzLjczMSAzLjYyOCAzMTguNzggNS4zMTggMzEzLjgzMiA3LjAxMiAzMDkuMTI3IDkuMTkgMzA0LjY3MiAxMS42MDYgMzAwLjQ2NiAxNC4yNjggMjk2LjI1OSAxNy4xNjkgMjkyLjI5NyAyMC4zMTIgMjg4LjU4NiAyMy42OTcgMjg1LjEyIDI3LjMyNiAyODEuOTAyIDMxLjE5NCAyNzguNjg0IDM1LjMwNyAyNzUuOTYyIDM5LjQxNiAyNzMuMjQgNDQuMDExIDI3MC43NjUgNDguNjA3IDI2OC41MzkgNTMuNjg1IDI2Ni41NTkgNDguMzYzIDI2NC4wODMgNDMuMjg1IDI2MS42MDggMzguNDUxIDI1OC42MzkgMzMuODU0IDI1NS40MjIgMjkuNzQ0IDI1MS45NTUgMjUuODc1IDI0OC4yNDQgMjIuMjQ3IDI0NC41MzEgMTguODYzIDI0MC4zMjIgMTUuNzE5IDIzNi4xMTYgMTMuMDU5IDIzMS40MTQgMTAuMzk3IDIyNi45NTkgOC4yMjIgMjIyLjAwOCA2LjI4OCAyMTcuMDU3IDQuNTk0IDIxMi4xMDkgMy4xNDMgMjA2LjkxMSAxLjkzNCAyMDEuNDY0IC45NjggMTkwLjU3NiAwIDE3OS45MzIgMCAxNzQuNzM1IC40ODQgMTY5Ljc4NiAuOTY4IDE2NC44MzUgMS42OTMgMTYwLjEzNCAyLjkwMyAxNTUuNjc4IDQuMTEyIDE1MS4yMjMgNS41NjIgMTQ2Ljc2NyA3LjI1MyAxNDIuODA3IDkuMTkgMTM4LjYwMiAxMS4xMjUgMTM0Ljg4OCAxMy41NDEgMTMxLjE3NSAxNS45NiAxMjcuNzExIDE4LjYxOSAxMjQuMjQ0IDIxLjUyMiAxMjEuMDI5IDI0LjY2NiAxMTguMDU4IDI3LjgwOSAxMTUuMDg3IDMxLjE5NCAxMTIuMzY1IDM0LjgyMiAxMDkuODg5IDM4LjY5MSAxMDcuNjYzIDQyLjU2IDEwNy42NjMgNS4wNzggMCA1LjA3OCAwIDU4Ljc2NCAzMy45MDcgNTguNzY0IDMzLjkwNyAyMDAuNDcgMCAyMDAuNDcgMCAyNTUuMTIyIDE1Ni42NjcgMjU1LjEyMiAxNTYuNjY3IDIwMC40NyAxMDcuNjYzIDIwMC40NyAxMDcuNjYzIDEwOC4zMzcgMTA3LjkwOSAxMDMuMjU5IDEwOC42NTIgOTguNDIxIDEwOS4zOTYgOTMuODI3IDExMC4zODUgODkuNDc0IDExMS42MjMgODUuMTIxIDExMy4xMDcgODEuMjUyIDExNC44NCA3Ny4zODMgMTE2LjgyIDczLjk5OCAxMTkuMDQ3IDcwLjYxMSAxMjEuNzcyIDY3LjcxMSAxMjQuNDk0IDY1LjA1MSAxMjcuNDYxIDYyLjYzMyAxMzAuOTI5IDYwLjQ1NSAxMzQuNjM5IDU4LjUyIDEzOC4zNTIgNTcuMDcgMTQyLjU2MSA1NS44NiAxNDcuMjYzIDU0Ljg5NSAxNTEuOTY1IDU0LjQxIDE1Ny4xNjIgNTQuMTY3IDE2MS4zNzEgNTQuMTY3IDE2NS4zMzEgNTQuNjUxIDE2OS4yOTEgNTUuMzc2IDE3Mi43NTUgNTYuMzQ1IDE3Ni4yMjEgNTcuNTU0IDE3OS40MzkgNTkuMDA1IDE4Mi40MDggNjAuNjk4IDE4NS4xMyA2Mi44NzMgMTg3LjYwNSA2NS4yOTIgMTkwLjA4MSA2Ny45NTIgMTkyLjA2MSA3MC44NTQgMTk0LjA0MSA3NC4yMzkgMTk1LjUyNCA3OC4xMDggMTk3LjAxMSA4MS45NzcgMTk4LjI0OSA4Ni41NzQgMTk5LjIzOCA5MS4xNjcgMTk5Ljk4IDk2LjI0NiAyMDAuNzIyIDEwMS44MDkgMjAwLjk3MSAxMDcuODUyIDIwMC45NzEgMjU1LjEyMiAzMDcuMzk3IDI1NS4xMjIgMzA3LjM5NyAyMDAuNDcgMjczLjQ4NyAyMDAuNDcgMjczLjQ4NyAxMTMuNDE1IDI3My43MzYgMTA4LjMzNyAyNzMuOTgzIDEwMy4yNTkgMjc0LjQ3OCA5OC40MjEgMjc1LjQ2NiA5My44MjcgMjc2LjQ1OCA4OS40NzQgMjc3LjY5NiA4NS4xMjEgMjc5LjE4IDgxLjI1MiAyODAuOTEzIDc3LjM4MyAyODIuODk0IDczLjk5OCAyODUuMTIgNzAuNjExIDI4Ny41OTUgNjcuNzExIDI5MC41NjcgNjUuMDUxIDI5My41MzQgNjIuNjMzIDI5Ny4wMDIgNjAuNDU1IDMwMC40NjYgNTguNTIgMzA0LjQyNSA1Ny4wNyAzMDguNjM1IDU1Ljg2IDMxMy4zMzYgNTQuODk1IDMxOC4wMzggNTQuNDEgMzIzLjIzNSA1NC4xNjcgMzI3LjQ0NCA1NC4xNjcgMzMxLjQwNCA1NC42NTEgMzM1LjM2NCA1NS4zNzYgMzM4LjgyOCA1Ni4zNDUgMzQyLjI5MiA1Ny41NTQgMzQ1LjUwOSA1OS4wMDUgMzQ4LjQ4MSA2MC42OTggMzUxLjIwMyA2Mi44NzMgMzUzLjY3OCA2NS4yOTIgMzU1LjkwNCA2Ny45NTIgMzU4LjEzMyA3MC44NTQgMzYwLjExNCA3NC4yMzkgMzYzLjA4MiA4MS45NzcgMzY0LjMyIDg2LjU3NCAzNjUuMzExIDkxLjE2NyAzNjYuMDUzIDk2LjI0NiAzNjYuNzk1IDEwMS44MDkgMzY3LjA0NCAxMDcuODUyIDM2Ny4wNDQgMjU1LjEyMiA0NzMuNDcgMjU1LjEyMiA0NzMuNDcgMjAwLjQ3IDQzOS41NiAyMDAuNDcgNDM5LjU2IDg2LjMzIDQzOS4zMTMgNzcuNjI0IDQzOC4zMjIgNjkuNjQ1IDQzNi44MzggNjEuOTA3IDQzNC44NTggNTQuNjUxIDQzMi4zODMgNDcuODgyIDQyOS40MTQgNDEuNTk1IDQyNS45NDggMzUuNzg4IDQyMS45ODggMzAuMjI5IDQxNy41MzIgMjUuMzkxIDQxMy4wNzcgMjAuNzk3IDQwNy44OCAxNi45MjggNDAyLjY4MiAxMy4zIDM5Ni45OTEgMTAuMTU3IDM5MS4wNTIgNy4yNTMgMzg0Ljg2MyA1LjA3OCAzNzguNjc0IDMuMTQzIDM3MS45OTMgMS42OTMgMzY1LjU1OCAuNzI1IDM1OC42MjkgMCAzNDUuNzU5IDAgMzQwLjA2NSAuNDg0Ii8+Cjwvc3ZnPg==" height="22">

</div>

## Introduction

`sailing-proto` is the consensus core: a pure state machine with **no I/O, no clock, no
threads, and no runtime** — the embedding application supplies storage, a monotonic `now`, and
message delivery, and drives the endpoint through a small `handle_*` / `poll_*` surface. The same
state machine therefore runs identically under tokio, compio, embassy, bare metal, or a
deterministic simulator.

## Status

**Pre-release.** The consensus core, the TCP/TLS/QUIC transports, and the reference proactor
driver (`sailing-compio`: io_uring/IOCP via [compio], one consensus group per thread, typed
cross-thread handles) are implemented and heavily tested (see *Correctness* below). The wire
format and public API may still change until 0.1.

[compio]: https://github.com/compio-rs/compio

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
| `quic` | the QUIC coordinator: quinn-proto datagrams, mandatory cluster mTLS, one consensus stream per peer | std (implies `tcp`) |

The byte-level wire format is pinned in
[`sailing-proto/WIRE.md`](sailing-proto/WIRE.md) (normative, with golden vectors in the tests).

MSRV: **1.91**.

## License

`sailing` is under the terms of both the MIT license and the Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/sailing/
[CI-url]: https://github.com/al8n/sailing/actions/workflows/ci.yml
[codecov-url]: https://app.codecov.io/gh/al8n/sailing/
[doc-url]: https://docs.rs/sailing
[crates-url]: https://crates.io/crates/sailing
