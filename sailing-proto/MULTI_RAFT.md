# Multi-Raft architecture

How sailing hosts many Raft groups in one process. This document tracks the design
and the phased roadmap; it is the companion to [`WIRE.md`](./WIRE.md) for the
multi-group layer.

Status: **Phases 0–5, 5b, and 3b are implemented** — the `MultiRaft` container, the group-demux
wire, both multi-group coordinators (`MultiStreamCoordinator`, `MultiQuicCoordinator`), the
shared in-memory storage engine (`GroupEngine`), the multi reactor drivers, coalesced
heartbeats + quiescence, the dynamic-lifecycle mechanics (tombstones, unknown-group
surfacing, removed-self), the group factory (hands-free materialization of solicited
groups at the driver), and the sharded compio host (K parallel planes, one full multi
driver per core, stream-only). QUIC sharding is an open row; Phase 6 is deferred.

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
| **2** (done) | Shared storage engine: the in-memory reference `GroupEngine` — every group's stores behind per-group staged-until-flush handles, ONE `flush()` barrier covering all groups' writes (the fsync amortization), per-group completion FIFOs and boot epochs + per-group lineage records (incarnation gen + admission floor) that OUTLIVE group removal and ride the same barrier. A disk engine in driver-land mirrors this contract | `sailing-proto` `multi::engine` |
| **3** (done) | Shared reactor host (the I/O layer: real sockets/timers, `flush()` becomes the fsync point): the multi stream/QUIC drivers over one shared `GroupEngine` barrier per crank, a quiesce-aware aggregate deadline fold, and the group-keyed client `MultiHandle`/`GroupHandle` | `sailing-reactor` |
| **3b** (done) | Sharded compio host, shipped as **K PARALLEL PLANES**: every core runs a COMPLETE compio multi driver — its own fused `MultiStreamCoordinator`, its own `GroupEngine` (a per-core WAL barrier: zero cross-core fsync contention), its own TCP listener on a per-shard port — hosting the disjoint group subset a UNIFORM cluster-wide shard map assigns it (`ShardMap`: FNV-1a over the group id's canonical `Data` encoding, or an embedder override; same K + same mapping on every node is the contract). Group `g`'s replicas talk `shard(g)` ↔ `shard(g)`: K independent meshes, one conn per peer PER PLANE (the router's one-conn-per-peer dedup holds within each plane), and NO cross-core hop anywhere on the hot path — conn → consensus → storage all core-local. Every Phase-4/5/5b feature (coalescing, quiescence, tombstones, lifecycle, factory) works per-plane UNCHANGED because a plane IS a full multi coordinator; a conn loss wakes exactly its one plane. One `ShardedMultiHandle` routes group-keyed operations by the map; the client tails fan in by construction (one events channel, one lifecycle channel, one in-flight budget, cloned into every plane). WHY not this row's original conn-core/shard-core handoff (one conn per peer + cross-core handoff at the transport edge): it requires exposing `sailing-proto`'s PRIVATE `PeerRouter` + frame codec and RE-SPLITTING the heartbeat-coalescing/quiesce-stamping logic — which straddles `MultiStreamCoordinator`'s flush/ship-heartbeats path — across the core boundary, would have forced v1 to drop Phase-4 quiescence, and puts 2 cross-core queue hops on every message: strictly worse for the sharding throughput goal, so the split shape is REJECTED, not deferred. Stream transport only in v1 (see the QUIC row below) | `sailing-compio` |
| **3b-QUIC** (open) | QUIC sharding. The plane model needs a per-core CONNECTION referent to partition (plane `i` owns its own sockets), but the QUIC driver runs quinn's single shared `UdpSocket` with quinn-internal per-peer multiplexing — there is no clean per-core connection unit to shard without either K UDP sockets/ports (a new addressing contract for QUIC peers) or surgery inside the quinn endpoint. Left explicitly open rather than half-shipped | `sailing-compio` |
| **4** (done) | Heartbeat coalescing + quiescence (idle-group scale win): one coalesced control frame per node pair per crank, idle groups stop exchanging beats entirely (any traffic or a connection loss wakes them); with the heartbeat-response append pump gated and eligibility excluding lagging peers (any tracked peer — learners included — probing, receiving a snapshot, or behind the leader's last index still draws catch-up traffic, so a leader must not quiesce over it), the wake classification's absorb set shrank to exactly `HeartbeatResponse` (the final flagged round is precisely the beat + its response) | transport + reactor |
| **5** (done) | Dynamic group-lifecycle mechanics: coordinator-level TOMBSTONES (a removed id's straggler frames drop silently, and the id REFUSES re-creation until an explicit `clear_tombstone` — the references' tombstone-refuses-creation rule, so a stale lifecycle advisory can never implicitly resurrect a removed id; in-memory by design — the embedder's catalog owns persistence, unlike TiKV/CockroachDB's persisted incarnation-keyed tombstones), UNKNOWN-GROUP surfacing (initial-shaped traffic for an unhosted, untombstoned group → `poll_unknown_group` → the drivers' `LifecycleEvent::UnknownGroup` tail), and the REMOVED-SELF flow (a committed conf change that drops the host from every membership role → `LifecycleEvent::RemovedSelf`; the replica keeps running harmlessly until the app removes it). The PLACEMENT BRAIN is explicitly the embedder's — no auto-create, no auto-teardown | coordinators + driver + reactor |
| **5b** (done) | Cockroach-shaped auto-materialization, shipped as a DRIVER-side hook: the embedder registers a `GroupFactory` (`with_group_factory` on both multi reactor drivers) whose `Some(GroupBlueprint)` — config + seed; the INITIAL state machine is built LAZILY by the factory's separate `build` phase, only after the driver's sender gate admits the blueprint, so refused and declined solicitations never construct one — materializes a solicited group inside the very crank that polled the gated unknown-group signal, running the exact create-command path (engine + coordinator + routing, same rollback); a consumed signal never reaches the lifecycle tail, while a decline, a build abort, or a create refusal (the admission gate applies to blueprints too) falls through to the tail exactly as on a factory-less driver, and membership decisions still ride ordinary conf changes wherever the embedder's policy lives (the `getOrCreateReplica` / `maybe_create_peer` shape both references converge on). The factory is the placement brain's ADMISSION EDGE — it must validate ids against the embedder's catalog (a `Some` is a real resource commitment), the driver refuses blueprints that do not name the soliciting peer in their seed voters (sender-membership fail-closed, enforced before the build phase), it is CREATE-only (recovery stays boot-time embedder work through `RestoreGroup`), it never overrides a tombstone (a tombstoned id's signals are never enqueued, a removal purges queued ones, and the residual interleaving fails closed at admission), and blueprints for FORK-BORN ids MUST use the OBSERVER shape — self absent from the seed voters (`Config::try_new_observer`) — because a full-voter empty is promotable with a virgin election timer and an empty quorum's first commit lands on the manufactured fork baseline's exact coordinate, which log-matching fuses silently; an observer empty still grants votes, so the fork holder's manufactured log wins the only possible election and the forced snapshot's boundary config is what promotes the joiner (fresh BOOTSTRAPPED ids keep full-voter blueprints — the distinction is the catalog's, and the catalog is the split registry). Per-group lifecycle GENERATIONS were pre-committed here on the assumption that the factory would consume advisories from the ASYNC lifecycle tail, where the host's lifecycle can move between capture and consumption; the shipped factory instead runs SYNCHRONOUSLY inside the driver crank — poll, materialize, admit in one pass, with no lifecycle mutation able to interleave — so no staleness window exists to state-bind, and generations move to the explicit future condition: if an async/deferred factory is ever introduced, its advisories must carry the incarnation they observed | driver + reactor |
| **6** (in progress) | Snapshot-bootstrapped group creation — SHIPPED as `create_group_from_fork` (container, both multi coordinators, every multi driver/handle incl. the sharded plane routing): a fork is a MANUFACTURED SNAPSHOT INSTALL — baseline meta (index 1, term 1), the caller's AUTHORITATIVE blob persisted, log compacted-through-1 — booted through the `Endpoint::restart` path so its validation/poison discipline is inherited wholesale, which forces every zero-progress joiner onto the snapshot path (an uncompacted fork would LOG-WALK the joiner: only post-fork entries replayed onto its EMPTY state machine — silent divergence). A fork is a LOCAL act by an already-authorized replica: never solicited over the wire, never factory-reachable, and it never clears a tombstone; admission rides the same floor-first gate as create/restore. **SPLIT is SHIPPED** on this substrate as a three-layer choreography, one committed `Split` admin entry (child id as raw bytes + two lineage counters + an opaque instruction — G-free so the group-unaware core can decode it, and the forked state NEVER rides the entry: wire cost independent of FSM size): the ENDPOINT applies at the deterministic point (`fsm.split` beside SetReadMode/ConfChange, the recovery blob derived AT APPLY from the just-forked half so blob and FSM correspond by construction, the fork staged, the parent's snapshots FENCED at the oldest outstanding split index — the fork durability barrier, without which a correlated crash after a parent compaction could lose the child's only recovery source); the CONTAINER relays (`poll_pending_fork` → typed child decode, a relay-time lineage guard seeded from DURABLE state so restart-replayed forks re-relay while same-gen retry duplicates fold to resolved no-ops, a non-member-host short-circuit, hosted-child conflicts PARKED — blob held, the parent fence standing, a one-shot conflict signal for the embedder, consumed by the drivers only once the bounded lifecycle tail accepts it (backpressure defers the cue; a park that resolves first purges it) — until the squatter leaves (materialize) or the same-lineage twin catches up (redundant), the child config rebuilt from the parent's local tuning under the fork's voter set); the DRIVERS materialize (the fork drain runs BEFORE the storage crank's flush, so ONE engine barrier covers registration + authoritative blob + both lineage records before the child can transmit — a child that can solicit peers is always locally blob-durable first, and the drain front-runs the factory drain so a local fork wins any same-id race — then the parent's fence lifts and the typed `LifecycleEvent::SplitApplied{parent, child}` fires). The materialization IS `create_group_from_fork` (the manufactured install above, now also stamping the child's incarnation and its INHERITED read mode into the baseline meta), so a fresh joiner of a split-born child is structurally forced onto the snapshot path. A factory that vouches a split-born child's id on a non-forked host must blueprint the OBSERVER shape (self outside the seed voters — the Phase-5b fork-born rule): the materialized empty then cannot campaign, the fork holder's manufactured baseline wins every election, and the forced snapshot promotes the joiner — a full-voter empty is promotable with a virgin timer, and an empty quorum's first commit collides with the manufactured `(1,1)` baseline coordinate, silently fusing divergent committed state. Gates by layer: leader/joint-config/hosted-child/child-encoding at the container (`propose_split`), the child-id floor at the COORDINATOR delegators through the per-call `FloorStore` seam (fail-fast leg; the drain's admission recheck stays authoritative), same-plane (`shard(child) == shard(parent)`) at the sharded handle — typed `SplitError` across all three producers. Two pin refinements changed public shape: `split`/`absorb` are DEFAULTED methods on `StateMachine` (`Option<Self>`/`bool` — a subtrait would infect every `apply_committed` monomorphization; a committed split against a default FSM poisons, `SplitUnsupported`), and `Event::SplitApplied` carries the child id as BYTES (events stay G-free; the typed surface is the drivers' lifecycle tail). **MERGE is SHIPPED** on the same substrate (see the merge section below the phase notes). Epoch doctrine, settled in advance: generations are INTRA-group — allocated by the group's own conf changes and fenced by persisted tombstones carrying a next-incarnation floor (CockroachDB's `NextReplicaID` model) — never by a central allocator | proto + coordinators + drivers |

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

**Deployment note (membership churn).** Hosts whose groups perform membership changes should
enable BOTH `pre_vote` and `check_quorum` in each group's `Config` (they are per-group knobs on a
multi-group host). A removed or partitioned member whose election timer fires campaigns at a
higher term and, with an up-to-date log, deposes a live leader — the Raft-thesis §4.2.3
disruptive-server problem, multiplied across co-hosted groups. `check_quorum` makes members
ignore vote requests while they observe a live leader, and `pre_vote` stops the term inflation.
The removal path's window is narrow (the leader's farewell append delivers the excising commit to
the pruned peer), but partitioned members remain, and the defaults stay OFF for etcd-raft library
parity. The removed-follower lifecycle e2es model the pair.

Split shipped without shaping the Phase-0 container — the endpoint stages, the container
relays, the drivers materialize; the container stayed the pure routing layer.

## Merge (as shipped)

Two colocated groups (identical voter sets, neither carrying learners, both non-joint, same
active read mode) become one through two entries plus an explicit abort, with **no clock
anywhere**. The learner precondition is the same replica-set-alignment doctrine as CRDB: the
relay places children only on VOTER hosts and parks a live absorb only on the target's voter
hosts, so a target-learner host — even one that became leader — would park forever; promote or
remove the learners on both sides first. Boot-config observers never enter a committed
configuration, so they are exempt. Both `prepare_merge` and `commit_merge` refuse with
`MergeError::LearnersPresent`.

**Direction rule (claims point strictly down the id order).** A claim must point strictly DOWN a
fixed total order over ids: `prepare_merge` refuses (`MergeError::DirectionInverted`) unless the
source's canonical `Data` encoding sorts STRICTLY ABOVE the target's. The encoding-minimal id of any
pair is therefore always the target/survivor, and because every claim edge strictly decreases one
total order, a claim CYCLE (A→B→…→A) is UNCONSTRUCTIBLE — the property that keeps concurrently-admitted
freezes at different leaders from deadlocking every release valve with mutual `AlreadyFrozen`. This is
a constant property of the id pair (race-immune, never self-clearing); the embedder orients each pair
(source = the encoding-larger side) before proposing. Admission is otherwise optimistically concurrent
— the propose gates are truthful LOCAL refusals, not a serializer — and refusal errors must never be
used as a mutual-exclusion primitive.

- **`PrepareMerge` (the source's log)** freezes the source. The lease SAFETY gate moves even
  earlier than apply — to APPEND observation of the entry (`freeze_pending`, a kind check on
  the hot path): every lease-serve and lease-formation gate fails closed the moment the freeze
  enters the local log, which is what makes the whole choreography clock-free — for any
  post-merge write `W` accepted by the target, `emit(read) < append(freeze, source leader) <
  commit < apply < absorb < accept(W)`, so every lease read OVERLAPS `W` and may legally
  linearize before it. No commit-wait, no wall horizon, no cross-node clock comparison. Full
  `Frozen` semantics stay apply-time (proposals, conf changes, transfers, reads refuse typed;
  heartbeats, appends, elections, and snapshot sends run UNCHANGED so the freeze itself
  propagates and survives leader crashes), and the freeze pins its **claim** — the one target
  named in the payload, held for the whole frozen generation, so exactly one target can ever
  absorb or abort a given freeze.
- **`CommitMerge` (the target's log)** applies only at its minted target lineage (the split's
  optimistic-guard idiom; a stale mint no-ops with `Event::MergeAborted` — parks never form
  for a killed or replayed commit), then PARKS the apply drain at `k − 1`: the absorbed half
  lives in another group's endpoint, which only the container holds. The per-crank
  `service_merge_applies` resolves every park from the target's log plus local facts: the
  **abort window** — the single committed coordinate `k + 1` — must be decided first (the
  target LEADER seals a quiet window with a no-op; a committed matching abort there un-parks
  ABORTED on every replica; anything else closes the window for good), and only then does the
  local source gate run (frozen at the expected generation FOR THIS TARGET, applied past the
  boundary; the host whose local source replica LEADS the source resolves LAST, keeping the
  freeze feedable until every source peer provably matched through the boundary). The absorb
  extracts the local source endpoint, folds its state machine into the target
  (`StateMachine::absorb`), stages a FORCED snapshot capture, and the driver folds the
  resolution in the same crank: `floor(source) = u64::MAX` (terminal — the id never returns)
  plus the source's storage teardown, all behind ONE engine barrier. A parked target is never
  quiesce-eligible; an FSM that refuses the absorb fail-stops the target deterministically and
  surfaces NO resolution (nothing is floored or torn down behind a poison).
- **`RollbackMerge`** is the abort, and it rides the **TARGET's log** so it is totally ordered
  against the commit it races (a source-side abort has no cross-log order against the target's
  commit — observation timing would decide the race per host, the committed divergence the
  randomized band proved). Below the commit it kills it at the commit's own lineage guard; at
  the coordinate right after a parked commit it un-parks every replica aborted; any later it
  no-ops at its own stale mint (the merge already resolved). The SOURCE's thaw is a relayed
  consequence: the applied abort stages a relay (`poll_pending_merge_abort`), and the driver
  proposes the source-side `RollbackMerge` (empty source field) on the source's own log —
  log-borne there so a restart re-derives the thaw; the claim gates the relay, so a foreign
  target's abort can never thaw a source claimed elsewhere. A relay lost to churn is recovered
  by re-proposing the abort. This abort-derived thaw is the FIRST of two legitimate thaw
  derivations.
- **The dead-target self-thaw (the SECOND thaw derivation).** A source can be stranded when its
  claimed target legally DISSOLVES — a chain `S→T→U` where `T` freezes into `U` and is absorbed —
  because both of `S`'s release verbs ride the now-dead `T`'s log. `service_merge_applies` self-heals
  it: a hosted FROZEN source whose claimed target is (i) NOT hosted here AND (ii) reads the terminal
  `MERGED_FLOOR` derives its OWN thaw on its own log (leader-only, bound to the freeze generation,
  `thaw_in_flight`-idempotent — the same mint discipline as the abort relay), refusing while any local
  park still names it (fail-safe). The safety argument is the husk-minority lemma: a committed
  `CommitMerge(S→T)` lives on a target QUORUM whose replicas all PARK and resolve locally, so any
  target replica that skipped the commit via install-supersede is sub-quorum and its (leader-only)
  source could never even append this thaw — so in the merge-SUCCEEDED world the derivation is
  unconstructible, in the ABORTED world the drivable-thaw belt heals `S` first, and in the
  never-committed world (the genuine strand) it is exactly correct. This is why a FALSE terminal floor
  is now a consensus-grade safety violation: it can mint a committed thaw against a live lineage.

**Do not remove a merge participant mid-choreography** — and this is now a TYPED GUARANTEE, not
advice: `remove_group` (and every coordinator/driver door that threads it) REFUSES each unresolved
participant, leaving the group fully intact, and each refusal self-clears once the merge resolves.
The five legs are the CLOSED product of the choreography's participant states — `{holder} ∪
{source: freeze-pending | frozen} ∪ {target: parked | claimed-pre-park} ∪ {named-as-source-by-a-park}`
— so no in-flight role slips the gate:

- a **frozen source** (`RemoveError::Frozen`, an active freeze — applied or append-observed): its
  target parks against this exact freeze. Roll the merge back first (abort → thaw), then removal
  admits.
- a **parked target** (`RemoveError::MergeParked`, holding its apply drain on a committed
  `CommitMerge`): removing the decider strands the frozen source. Let the merge resolve (absorb or
  abort), then removal admits.
- a **claimed target BEFORE it parks** (`RemoveError::Claimed`, the mirror of `SpokenFor`): another
  hosted source names this group as its merge target — applied (`frozen_for`) or an append-pending
  `PrepareMerge` DECODED from that source's own log — while this group has not yet proposed its
  `CommitMerge`. Removing it would strand that source frozen for a target that no longer exists (its
  absorb AND its abort both ride this group's log). Roll the naming merge back first (this group is
  hosted pre-park, so `rollback_merge` on it thaws the source), then removal admits. This is the ONLY
  leg that reads a peer group's log; every other leg is an in-memory read, and the decode is paid per
  (rare) removal so appends stay kind-only.
- a **group a park names as its source** (`RemoveError::SpokenFor`, the cross-endpoint leg): even
  before this group's own replica has observed its freeze, a hosted target's park names it.
- a group still **owing an aborted source its thaw** (`RemoveError::OwesThaw`, an undischarged
  target-role `abandoned` obligation): its log is that obligation's only replay source. The
  container also refuses to dissolve it as a fresh merge's source (`SourceOwesThaw`) and HOLDS any
  absorb of it until the thaw pass discharges the obligation.

The pending-`CommitMerge` windows need no leg of their own: the absorb barrier holds the source
`Frozen` and the target `MergeParked` throughout.

Recovery for a genuinely-DEAD participant is the embedder's catalog, exactly like any dead group:
a frozen source or parked target is restored (or floored), and the ONE deliberate escape is an
**OWED source** (a frozen source a hosted target already owes a thaw) — removing it ADMITS, because
the container's removal purge binds every holder's obligation to the departing incarnation and the
driver floors the id. The freeze gates cover the whole admin propose family (a frozen group refuses
splits and refuses to be a merge target; a mid-absorb source refuses a fresh freeze), and
`pre_vote`/`check_quorum` recommendations are unaffected by the freeze — a frozen group elects
normally.

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
