# Multi-Raft architecture

How sailing hosts many Raft groups in one process. This document tracks the design
and the phased roadmap; it is the companion to [`WIRE.md`](./WIRE.md) for the
multi-group layer.

Status: **Phase 0 (the `multi` module scaffold) in progress.** Everything past Phase 0
is roadmap, not yet built.

---

## The core decision: a container of single-group cores

The multi-raft layer is a **container of N independent single-group `Endpoint`s**, not a
multi-group core. `sailing-proto`'s `Endpoint` stays completely group-unaware; a new
`multi` layer owns the `GroupId -> Endpoint` map and multiplexes inputs and outputs.

This is not a stylistic preference — it is the design the whole Raft-library lineage
converged on after trying the alternative:

- etcd/raft once had a `MultiNode` that baked multi-group into the core (every op carried
  a `group` id; one goroutine multiplexed a `map<group, Ready>`). It was **CockroachDB's
  contribution**, and it was **deleted in the very same commit that introduced the
  single-group `RawNode`** (`4b3a7ff`, "raft: add RawNode ... and remove MultiNode").
- etcd, CockroachDB, and TiKV (which runs ~10^5 groups per node in production) all settled
  on **`RawNode`-per-group + everything-else-shared**.

`sailing-proto`'s `Endpoint` already sits exactly where `RawNode` sits. So: **adopt the
outcome, avoid the `MultiNode` shape.** Do not thread a `group` parameter through the core
or introduce a `map<GroupId, _>` inside `sailing-proto`'s consensus code — put the map in
the `multi` container.

## Why the `Endpoint` multiplexes cleanly (audit findings)

A five-slice deep audit of the current code confirmed the `Endpoint` is a near-ideal
multiplexing unit. What is per-group vs. shareable across a host:

| Concern | Current shape | Multi-raft consequence |
| --- | --- | --- |
| **Storage** | `Endpoint` owns none; `log: &mut L`, `stable: &mut S` are injected on every call (`endpoint/mod.rs`, ~93 call sites) | The container/driver owns storage and hands each group its own handle over a shared engine |
| **Clock** | Not owned; `now: impl Into<Now>` passed per call. The core never reads a clock (`time.rs`) | One shared host monotonic clock (and one synchronized wall) feeds every group |
| **Global state** | None — no statics, no global RNG, no `thread_local` (verified crate-wide) | N `Endpoint`s coexist with isolation *by construction* |
| **Per-instance cost** | Allocation-light; construction allocates only an `O(voters)` tracker, every queue/map starts empty (`endpoint/mod.rs` `new_with_rng`) | An idle group costs ≈ a few hundred bytes–1 KB; memory tracks *active* groups, not group count |
| **Time model** | Deadline-based, **no `tick()`**; `poll_timeout() -> Option<Instant>` yields the next serviceable deadline (`endpoint/mod.rs`) | Drive with a `poll_timeout`-keyed timing wheel: `O(log N)` per timer *event*, `O(1)` groups woken |
| **PRNG** | Per-instance SplitMix64 seeded at `new`/`restart` (`prng.rs`) | Seed each group distinctly (mix the group id into the seed) or identical election jitter correlates elections |
| **OpId** | Per-`Endpoint` `{epoch, seq}`; epoch namespaces boot incarnations, **not** groups (`storage.rs`) | A shared physical store must key completions `(group_id, OpId)` and feed each group a per-group boot-epoch |
| **Wire** | Nothing carries a group id; `Message<I>` embeds the sender, `Outgoing<I>` carries `to: I` only (`message/mod.rs`) | Tag the **transport frame envelope** with the group id (see below) |
| **LeaseGuard** | Lease safety rides per-entry `lease_window`/`wall_timestamp` on *AppendEntries* + the per-group commit-wait — **not** the heartbeat (`Heartbeat` carries no lease data) | Heartbeat coalescing is *structurally* safe; it cannot break LeaseGuard |

Two findings are worth calling out because they are gifts:

- **Batched fsync across groups needs zero core change.** Storage completions are already
  asynchronous and order-free, and the log and stable stores carry **no cross-store
  barrier** (safety rides persist-before-*respond* gates). A shared engine may freely
  interleave and batch every group's writes into one fsync, then fan per-group completions
  back. The per-group invariants (prefix-ordered durability per log; ordered stable
  completions per group) are satisfied trivially by batching.
- **Heartbeat coalescing is LeaseGuard-neutral.** Every `Heartbeat` field is per-group
  (`commit` is even per-follower via the `min(commit, match)` clamp), so a coalesced beat
  is a batch of per-group payloads under one node-pair envelope — the saving is per-frame
  overhead, not shared consensus state. Batching *delay* even falls on the conservative
  side of the lease inequality.

## North-star architecture (full TiKV-style)

```
                    +---------------------------------------------+
   sailing-reactor  |  shared reactor: ready-set scheduler +      |  Phase 3
   (threaded host)  |  timing wheel + storage-worker pool         |
                    +----------------+----------------------------+
                                     | drives (Sans-I/O)
   sailing-proto    +----------------v----------------------------+
   mod multi        |  MultiRaft<G,I,F> - pure super state machine|  Phase 0
   (Sans-I/O)       |  BTreeMap<GroupId, Endpoint> (+ store FSM)   |  <- scaffold
                    |  route inputs . drain outputs . agg deadlines|
                    +--+--------------+----------------+-----------+
   shared transport -+  shared storage +   store/node FSM
   (target,group)       engine:            cross-group concerns
   router, frame        group-prefixed     (create/destroy,
   group-tag,           keys, one fsync     snapshot mgr)
   coalesced HB +       per batch, per-      Phase 5
   quiesce              group fan-out
     Phase 1             Phase 2
```

The crate boundary is load-bearing: **`mod multi` in `sailing-proto` is the *pure
Sans-I/O* super-state-machine.** You drive it exactly like an `Endpoint` — inject
messages/time/storage-completions, drain outputs — but multiplexed across groups with an
aggregate scheduler surface. The threaded reactor, the shared storage engine, and the
coalescing transport are *downstream consumers*; they stay out of the pure core, exactly
as they do for the single-group path today.

Adopted patterns, by source:

- **etcd `RawNode`-per-group**: each core is a `has_ready -> drain -> ack` transducer; a
  work-driven scheduler runs only *ready* groups, never a per-tick sweep of all of them.
- **TiKV store-FSM / peer-FSM split**: a singleton store/node FSM owns cross-group concerns
  (transport health, snapshot manager, group create/destroy) beside the per-group cores, so
  store-level duties never leak into the core.
- **TiKV / CockroachDB router**: transport carries `(group_id, message)`; the container
  demuxes to `groups[group_id]`. Outbound is a `(target_node, group_id)` lookup over one
  shared connection pool.
- **Async storage as routed messages -> shared batched fsync**: a shared write worker
  coalesces many groups' appends into one write batch = one fsync over group-prefixed keys,
  then fans typed completions back (error-permanent, three-phase IO state).
- **Coalesced heartbeats + quiescence**: coalesce beats per node pair; quiesce idle groups
  so the common case sends nothing at all (at 10^4 idle groups, *not sending* beats
  coalescing them).

## The wire change (Phase 1 preview)

Tag the **transport frame envelope**, not the protobuf `Message`:

```
[u32 BE total_len][group_id: fixed u64 BE][protobuf Message body]
```

The router must pick the target `Endpoint` *before* decoding the Raft payload — groups may
have different state-machine command/snapshot types, and a `InstallSnapshot` frame can
approach the 64 MiB frame bound, so decoding it just to learn its group is untenable. A
fixed-offset header field is an `O(1)` read. Because `LABEL_VERSION` fences mixed-version
peers at the connection hello, this is a clean version break (bump 5 -> 6), not a
wire-coexistence exercise. The envelope layout also reserves room for a future coalesced
frame `[len][(group_id, msg_len, msg)*]` — which a protobuf field could not.

## Phased roadmap

| Phase | Deliverable | Where |
| --- | --- | --- |
| **0** | `mod multi` scaffold: container + `GroupId` + routing + aggregate output/deadline surface + group-distinct seeding; static group set; downstream seams reserved | `sailing-proto` |
| **1** | Wire group-demux: frame-envelope `group_id`, `LABEL_VERSION` 5 -> 6, `(target, group)` router | `sailing-proto` wire + transport |
| **2** | Shared storage engine: group-prefixed keys, batch-first append, one fsync/batch, per-group completion fan-out, `(group_id, OpId)` keying, per-group boot-epoch | driver / storage impl |
| **3** | Shared reactor: ready-set scheduler + `poll_timeout` timing wheel + storage-worker pool | `sailing-reactor` |
| **4** | Heartbeat coalescing + quiescence (idle-group scale win) | transport + reactor |
| **5** | Dynamic create/destroy via the store FSM | `multi` + reactor |
| **6** (deferred) | Snapshot-bootstrapped group creation; split / merge | separate project |

Split/merge is deferred hard: it couples the state machine's key range to Raft-group
identity and needs a transactional handoff. It must not shape the Phase-0 container.

## Phase 0: the `multi` module scaffold

New module `sailing-proto/src/multi/` — Sans-I/O, `no_std` + `alloc`,
`#![deny(missing_docs)]`, with the group-agnostic consensus core untouched.

```rust
// multi/group_id.rs  — mirrors id.rs's NodeId
pub trait GroupId: Ord + CheapClone + core::fmt::Debug {}   // blanket impl; u64 works

// multi/mod.rs
pub struct MultiRaft<G, I, F, R = Prng>
where G: GroupId, I: NodeId, F: StateMachine {
    groups: BTreeMap<G, Endpoint<I, F, R>>,
    dirty:  VecDeque<G>,          // groups with output to drain (filled on dispatch)
    // aggregate next-deadline tracking for poll_timeout
}

impl MultiRaft {
    pub fn new() -> Self;

    // lifecycle — rung 1 (static / append-only); remove_group reserved for rung 2/5
    pub fn create_group(&mut self, gid: G, cfg: Config<I>, now, seed: u64, fsm: F) -> Result<(), _>;
    pub fn restore_group(&mut self, gid, cfg, now, seed, fsm, boot_epoch, log, stable) -> Result<(), _>;
    pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F, R>>;
    pub fn group(&self, &G) / group_mut(&mut self, &G) / len / is_empty / group_ids();

    // input routing — delegate to the group, then track dirty + deadline
    pub fn handle_message(&mut self, gid: &G, now, log, stable, from: I, msg: Message<I>);
    pub fn handle_timeout(&mut self, gid: &G, now, log, stable);
    pub fn handle_storage(&mut self, gid: &G, now, log, stable) -> StorageProgress;
    pub fn propose / propose_conf_change_v2 / read_index / transfer_leader / flush_appends (per gid);

    // aggregate output — stamped with the originating group
    pub fn poll_message(&mut self) -> Option<(G, Outgoing<I>)>;   // walks the dirty-set, zero-copy
    pub fn poll_event(&mut self)   -> Option<(G, Event<I, F::Response>)>;

    // aggregate scheduling surface for the reactor's wheel
    pub fn poll_timeout(&self)     -> Option<Instant>;             // min over groups
    pub fn deadlines(&self) -> impl Iterator<Item = (G, Instant)>; // reactor builds its wheel
}
```

**Group-distinct seeding.** `create_group` mixes `gid` into the PRNG seed so co-located
groups do not draw identical election-timeout jitter (which would correlate elections).

**Reserved seams** (documented, not built in Phase 0):

- Storage is injected per call today; a `StoreRouter` trait slots in at Phase 2 without
  changing this surface.
- `poll_message` returns `(G, Outgoing)` so the wire group-tag (Phase 1) is a pure
  transport concern.
- `remove_group` + a store/node-FSM hook anticipate Phase 5 (dynamic lifecycle).

**Testing.** A `MultiHarness` over the existing `testkit`: two groups over one mock
store map, asserting group isolation (group A's traffic never perturbs B), cross-group
interleaved drive, group-distinct election seeding, and aggregate `poll_timeout`
correctness.

## References

- etcd/raft `RawNode` (the single-group core) and the `MultiNode` removal (`4b3a7ff`).
- TiKV `raftstore`: `RaftBatchSystem`, `StoreFsm`/`PeerFsm`, `BasicMailbox`/`Router`,
  coalesced heartbeats, Hibernate Regions, region-prefixed keys in a shared engine.
- CockroachDB: `raftScheduler`, coalesced heartbeats, quiescence, range split/merge.
- openraft: `GroupRouter` keyed `(target, group)`; `IOFlushed` completion callback.
