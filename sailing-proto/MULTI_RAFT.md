# Multi-Raft architecture

How sailing hosts many Raft groups in one process. This document tracks the design
and the phased roadmap; it is the companion to [`WIRE.md`](./WIRE.md) for the
multi-group layer.

Status: **Phases 0–5 and 5b are implemented** — the `MultiRaft` container, the group-demux
wire, both multi-group coordinators (`MultiStreamCoordinator`, `MultiQuicCoordinator`), the
shared in-memory storage engine (`GroupEngine`), the multi reactor drivers, coalesced
heartbeats + quiescence, the dynamic-lifecycle mechanics (tombstones, unknown-group
surfacing, removed-self), and the group factory (hands-free materialization of solicited
groups at the driver). Phase 3b (sharded compio) is roadmap; Phase 6 is deferred.

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
| **LeaseGuard** | The per-entry lease window (`lease_window`/`wall_timestamp`) rides *AppendEntries* + the per-group commit-wait; the heartbeat pair's `lease_round`/`lease_support` (the CheckQuorum/LeaseBased renewal) is per-group state | Heartbeat coalescing is *structurally* safe: a coalesced batch carries every per-group field intact, and batching delay is on the conservative side for both lease families |

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

## The wire change (landed in Phase 1)

Tag the **transport frame envelope**, not the protobuf `Message` (normative layout: WIRE.md §3):

```
[u32 BE total_len][u16 BE group_len][group id bytes][protobuf Message body]
```

The router picks the target `Endpoint` *before* decoding the Raft payload — groups may
have different state-machine command/snapshot types, and an `InstallSnapshot` frame can
approach the 64 MiB frame bound, so decoding it just to learn its group is untenable. The
group id is the `GroupId`'s `Data` encoding, bounded 1..=1024 bytes and enforced at
`create_group` (the empty tag is the single-group form), so ids stay generic rather than a
fixed u64. Because `LABEL_VERSION` fences mixed-version peers at the connection hello, this
was a clean break: the version byte was RESET to 1 as the group-tagged baseline (nothing is
published; the pre-group formats burned 1..=5, and a byte must never be reused once anything
ships). The header's front-of-payload position composed directly into the coalesced control
frame that landed in Phase 4 — `[len][0xFFFF][(flags, group_len, group, msg_len, msg)+]`, WIRE.md
§3.1, behind the version-2 hello bump — which a protobuf-embedded tag could not have.

## Phased roadmap

| Phase | Deliverable | Where |
| --- | --- | --- |
| **0** (done) | `mod multi` scaffold: container + `GroupId` + routing + aggregate output/deadline surface + group-distinct seeding; append-only group set; downstream seams reserved | `sailing-proto` |
| **1** (done) | Wire group-demux: the frame-envelope group tag, `LABEL_VERSION` reset to 1, the `(group, peer)` demux through the router/bridge, and both multi-group coordinators | `sailing-proto` wire + transport |
| **2** (done) | Shared storage engine: the in-memory reference `GroupEngine` — every group's stores behind per-group staged-until-flush handles, ONE `flush()` barrier covering all groups' writes (the fsync amortization), per-group completion FIFOs and boot epochs. A disk engine in driver-land mirrors this contract | `sailing-proto` `multi::engine` |
| **3** (done) | Shared reactor host (the I/O layer: real sockets/timers, `flush()` becomes the fsync point): the multi stream/QUIC drivers over one shared `GroupEngine` barrier per crank, a quiesce-aware aggregate deadline fold, and the group-keyed client `MultiHandle`/`GroupHandle` | `sailing-reactor` |
| **3b** | Sharded compio host: shard-per-core `MultiRaft` + per-core engine shards (a per-core WAL — no cross-core fsync contention); cross-core handoff at the TRANSPORT EDGE only, since a peer connection stays core-owned (per-core connections would fight the router's one-conn-per-peer model) | `sailing-compio` |
| **4** (done) | Heartbeat coalescing + quiescence (idle-group scale win): one coalesced control frame per node pair per crank, idle groups stop exchanging beats entirely (any traffic or a connection loss wakes them); with the heartbeat-response append pump gated and eligibility excluding lagging peers (any tracked peer — learners included — probing, receiving a snapshot, or behind the leader's last index still draws catch-up traffic, so a leader must not quiesce over it), the wake classification's absorb set shrank to exactly `HeartbeatResponse` (the final flagged round is precisely the beat + its response) | transport + reactor |
| **5** (done) | Dynamic group-lifecycle mechanics: coordinator-level TOMBSTONES (a removed id's straggler frames drop silently, and the id REFUSES re-creation until an explicit `clear_tombstone` — the references' tombstone-refuses-creation rule, so a stale lifecycle advisory can never implicitly resurrect a removed id; in-memory by design — the embedder's catalog owns persistence, unlike TiKV/CockroachDB's persisted incarnation-keyed tombstones), UNKNOWN-GROUP surfacing (initial-shaped traffic for an unhosted, untombstoned group → `poll_unknown_group` → the drivers' `LifecycleEvent::UnknownGroup` tail), and the REMOVED-SELF flow (a committed conf change that drops the host from every membership role → `LifecycleEvent::RemovedSelf`; the replica keeps running harmlessly until the app removes it). The PLACEMENT BRAIN is explicitly the embedder's — no auto-create, no auto-teardown | coordinators + driver + reactor |
| **5b** (done) | Cockroach-shaped auto-materialization, shipped as a DRIVER-side hook: the embedder registers a `GroupFactory` (`with_group_factory` on both multi reactor drivers) whose `Some(GroupBlueprint)` — config + seed + INITIAL state machine — materializes a solicited group inside the very crank that polled the gated unknown-group signal, running the exact create-command path (engine + coordinator + routing, same rollback); a consumed signal never reaches the lifecycle tail, while a decline — or a create refusal (the admission gate applies to blueprints too) — falls through to the tail exactly as on a factory-less driver, and membership decisions still ride ordinary conf changes wherever the embedder's policy lives (the `getOrCreateReplica` / `maybe_create_peer` shape both references converge on). The factory is the placement brain's ADMISSION EDGE — it must validate ids against the embedder's catalog (a `Some` is a real resource commitment), it is CREATE-only (recovery stays boot-time embedder work through `RestoreGroup`), and it never overrides a tombstone (a tombstoned id's signals are never enqueued, a removal purges queued ones, and the residual interleaving fails closed at admission). Per-group lifecycle GENERATIONS were pre-committed here on the assumption that the factory would consume advisories from the ASYNC lifecycle tail, where the host's lifecycle can move between capture and consumption; the shipped factory instead runs SYNCHRONOUSLY inside the driver crank — poll, materialize, admit in one pass, with no lifecycle mutation able to interleave — so no staleness window exists to state-bind, and generations move to the explicit future condition: if an async/deferred factory is ever introduced, its advisories must carry the incarnation they observed | driver + reactor |
| **6** (deferred) | Snapshot-bootstrapped group creation; split / merge. Epoch doctrine, settled in advance: generations are INTRA-group — allocated by the group's own conf changes and fenced by persisted tombstones carrying a next-incarnation floor (CockroachDB's `NextReplicaID` model) — never by a central allocator | separate project |

**Placement doctrine.** The blessed path is the symmetric, embedded, Cockroach-style policy
loop: every host runs its own placement decisions against the observability this layer already
exposes (per-group role/term/commit, the lifecycle tail, quiescence state), the way CockroachDB's
replicate/split/merge queues run on each store's leaseholders with no separate control plane. A
PD-style external placer remains *buildable* on the same triggers, but nothing in this design may
ever REQUIRE one — in particular, no central ID, epoch, or placement allocation. With the
Phase-5b factory a fully hands-free CRDB-style node is now expressible: the factory is the
admission edge, and the embedded policy loops make the decisions.

**Membership-level rejoin (the supported recipe).** A node whose group replica lost its log
(removed then re-created fresh) must NOT be walked back by the append protocol: the leader's
progress still carries the old `match`, and the staleness guard rightly drops rejects at or below
it — under one identity, a durable log must never regress (the same invariant every Raft library
holds). Rejoin instead goes through membership: conf-change the node OUT and back IN, which
recreates its leader-side progress at zero and catches it up by snapshot — the membership-level
analogue of the references' new-replica-ID rule.

Split/merge is deferred hard: it couples the state machine's key range to Raft-group
identity and needs a transactional handoff. It must not shape the Phase-0 container.

## The `multi` container (as built)

New module `sailing-proto/src/multi/` — Sans-I/O, `no_std` + `alloc`,
`#![deny(missing_docs)]`, with the group-agnostic consensus core untouched.

```rust
// multi/group_id.rs — mirrors id.rs's NodeId (blanket impl; u64 works out of the box; the
// Data encoding is the wire tag, bounded 1..=1024 bytes and enforced at create_group)
pub trait GroupId: Data + CheapClone + Ord + Hash + Debug + Display + 'static {}

// multi/mod.rs — as built
pub struct MultiRaft<G, I, F, R = Prng> { /* BTreeMap<G, Endpoint<I, F, R>> + dirty queues */ }

impl MultiRaft {
    // admission — validated (id uniqueness, the encoding bound, one shared node id per host);
    // the full Endpoint constructor family, group-seeded or caller-RNG'd
    create_group(_with_rng) / restore_group(_with_rng) / restore_group_migrating(_with_rng)
    remove_group / group / contains_group / len / is_empty / group_ids

    // input routing — every wrapper #[must_use]: None = no such group, nothing happened
    handle_message / handle_timeout / handle_storage
    propose / flush_appends / propose_conf_change(_v2) / propose_read_mode_change
    read_index / transfer_leader

    // aggregate output — stamped with the originating group
    poll_message() -> Option<(G, Outgoing<I>)>     // walks the dirty-set, zero-copy
    poll_event()   -> Option<(G, Event<I, F::Response>)>

    // aggregate scheduling surface for the reactor's wheel
    poll_timeout() -> Option<Instant>              // O(N) min over groups
    deadlines()    -> impl Iterator<Item = (G, Instant)>
}
```

There is deliberately no mutable endpoint access (`group()` is shared-only): a driver mutating
an `Endpoint` directly would enqueue output the aggregate drains never learn about.

**Group-distinct seeding.** `create_group` mixes `gid` into the PRNG seed so co-located
groups do not draw identical election-timeout jitter (which would correlate elections).

**Reserved seams:**

- `MultiRaft` takes storage per call; the coordinators resolve per-group stores through the
  `GroupStores` trait (the seam that shipped — the Phase-2 engine implements it over
  group-scoped handles without changing the surface).
- `poll_message` returns `(G, Outgoing)` so the wire group-tag stays a pure transport
  concern (the coordinators stamp it).
- `remove_group` is the Phase-5 teardown seam, now consumed: the coordinators wrap it with the
  lifecycle mechanics (tombstones, unknown-group surfacing) while the container stays pure.

**Testing.** Container tests assert group isolation, the admission checks, seed
decorrelation, and the unknown-group verdicts; the transport layers round-trip group-tagged
frames at the unit level and drive multi-group coordinators end-to-end over live
connections (demux to the right group, unhosted-group drop, malformed-tag close).

## References

- etcd/raft `RawNode` (the single-group core) and the `MultiNode` removal (`4b3a7ff`).
- TiKV `raftstore`: `RaftBatchSystem`, `StoreFsm`/`PeerFsm`, `BasicMailbox`/`Router`,
  coalesced heartbeats, Hibernate Regions, region-prefixed keys in a shared engine.
- CockroachDB: `raftScheduler`, coalesced heartbeats, quiescence, range split/merge.
- openraft: `GroupRouter` keyed `(target, group)`; `IOFlushed` completion callback.
