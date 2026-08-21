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
[u32 BE total_len][u16 BE group_len][group id bytes][varint generation][protobuf Message body]
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
§3.1, behind the version-2 hello bump — which a protobuf-embedded tag could not have. The header
later grew the sender's INCARNATION STAMP (`varint generation`, one zero byte on the unreshaped
common path) in place under the same version, the input to the demux generation fence described
under membership churn below; WIRE.md §6 is normative for its enforcement, and every coalesced
entry carries its own stamp because one frame batches several groups' independently-moving
lineages.

## Phased roadmap

| Phase | Deliverable | Where |
| --- | --- | --- |
| **0** (done) | `mod multi` scaffold: container + `GroupId` + routing + aggregate output/deadline surface + group-distinct seeding; append-only group set; downstream seams reserved | `sailing-proto` |
| **1** (done) | Wire group-demux: the frame-envelope group tag, `LABEL_VERSION` reset to 1, the `(group, peer)` demux through the router/bridge, and both multi-group coordinators | `sailing-proto` wire + transport |
| **2** (done) | Shared storage engine: the in-memory reference `GroupEngine` — every group's stores behind per-group staged-until-flush handles, ONE `flush()` barrier covering all groups' writes (the fsync amortization), per-group completion FIFOs and boot epochs + per-group lineage records (incarnation gen + admission floor) that OUTLIVE group removal and ride the same barrier. A disk engine in driver-land mirrors this contract | `sailing-proto` `multi::engine` |
| **3** (done) | Shared reactor host (the I/O layer: real sockets/timers, `flush()` becomes the fsync point): the multi stream/QUIC drivers over one shared `GroupEngine` barrier per crank, a quiesce-aware aggregate deadline fold, and the group-keyed client `MultiHandle`/`GroupHandle` | `sailing-reactor` |
| **3b** (done) | Sharded compio host, shipped as **K PARALLEL PLANES**: every core runs a COMPLETE compio multi driver — its own fused `MultiStreamCoordinator`, its own `GroupEngine` (a per-core WAL barrier: zero cross-core fsync contention), its own TCP listener on a per-shard port — hosting the disjoint group subset a UNIFORM cluster-wide shard map assigns it (`ShardMap`: FNV-1a over the group id's canonical `Data` encoding, or an embedder override; same K + same mapping on every node is the contract). Group `g`'s replicas talk `shard(g)` ↔ `shard(g)`: K independent meshes, one conn per peer PER PLANE (the router's one-conn-per-peer dedup holds within each plane), and NO cross-core hop anywhere on the hot path — conn → consensus → storage all core-local. Every Phase-4/5/5b feature (coalescing, quiescence, tombstones, lifecycle, factory) works per-plane UNCHANGED because a plane IS a full multi coordinator; a conn loss wakes exactly its one plane. One `ShardedMultiHandle` routes group-keyed operations by the map; the client tails fan in by construction (one events channel, one lifecycle channel, one in-flight budget, cloned into every plane). WHY not this row's original conn-core/shard-core handoff (one conn per peer + cross-core handoff at the transport edge): it requires exposing `sailing-proto`'s PRIVATE `PeerRouter` + frame codec and RE-SPLITTING the heartbeat-coalescing/quiesce-stamping logic — which straddles `MultiStreamCoordinator`'s flush/ship-heartbeats path — across the core boundary, would have forced v1 to drop Phase-4 quiescence, and puts 2 cross-core queue hops on every message: strictly worse for the sharding throughput goal, so the split shape is REJECTED, not deferred. Stream transport only in v1 (see the QUIC row below) | `sailing-compio` |
| **3b-QUIC** (open) | QUIC sharding. The plane model needs a per-core CONNECTION referent to partition (plane `i` owns its own sockets), but the QUIC driver runs quinn's single shared `UdpSocket` with quinn-internal per-peer multiplexing — there is no clean per-core connection unit to shard without either K UDP sockets/ports (a new addressing contract for QUIC peers) or surgery inside the quinn endpoint. Left explicitly open rather than half-shipped; re-confirmed at the hardening pass — the per-core connection referent still does not exist under quinn's shared socket, and nothing landed since changes the calculus | `sailing-compio` |
| **4** (done) | Heartbeat coalescing + quiescence (idle-group scale win): one coalesced control frame per node pair per crank, idle groups stop exchanging beats entirely (any traffic or a connection loss wakes them); with the heartbeat-response append pump gated and eligibility excluding lagging peers (any tracked peer — learners included — probing, receiving a snapshot, or behind the leader's last index still draws catch-up traffic, so a leader must not quiesce over it), the wake classification's absorb set shrank to an idle `HeartbeatResponse` (the final flagged round is precisely the beat + its response; a response advertising a wedged merge park still wakes) | transport + reactor |
| **5** (done) | Dynamic group-lifecycle mechanics: coordinator-level TOMBSTONES (a removed id's straggler frames drop silently, and the id REFUSES re-creation until an explicit `clear_tombstone` — the references' tombstone-refuses-creation rule, so a stale lifecycle advisory can never implicitly resurrect a removed id; in-memory by design — the embedder's catalog owns persistence, unlike TiKV/CockroachDB's persisted incarnation-keyed tombstones), UNKNOWN-GROUP surfacing (initial-shaped traffic for an unhosted, untombstoned group → `poll_unknown_group` → the drivers' `LifecycleEvent::UnknownGroup` tail), and the REMOVED-SELF flow (a committed conf change that drops the host from every membership role → `LifecycleEvent::RemovedSelf`; the replica keeps running harmlessly until the app removes it). The PLACEMENT BRAIN is explicitly the embedder's — no auto-create, no auto-teardown | coordinators + driver + reactor |
| **5b** (done) | Cockroach-shaped auto-materialization, shipped as a DRIVER-side hook: the embedder registers a `GroupFactory` (`with_group_factory` on both multi reactor drivers) whose `Some(GroupBlueprint)` — config + seed; the INITIAL state machine is built LAZILY by the factory's separate `build` phase, only after the driver's sender gate admits the blueprint, so refused and declined solicitations never construct one — materializes a solicited group inside the very crank that polled the gated unknown-group signal, running the exact create-command path (engine + coordinator + routing, same rollback); a consumed signal never reaches the lifecycle tail, while a decline, a build abort, or a create refusal (the admission gate applies to blueprints too) falls through to the tail exactly as on a factory-less driver, and membership decisions still ride ordinary conf changes wherever the embedder's policy lives (the `getOrCreateReplica` / `maybe_create_peer` shape both references converge on). The factory is the placement brain's ADMISSION EDGE — it must validate ids against the embedder's catalog (a `Some` is a real resource commitment), the driver refuses blueprints that do not name the soliciting peer in their seed voters (sender-membership fail-closed, enforced before the build phase), it is CREATE-only (recovery stays boot-time embedder work through `RestoreGroup`), it never overrides a tombstone (a tombstoned id's signals are never enqueued, a removal purges queued ones, and the residual interleaving fails closed at admission), and blueprints for FORK-BORN ids MUST use the OBSERVER shape — self absent from the seed voters (`Config::try_new_observer`) — because a full-voter empty is promotable with a virgin election timer and an empty quorum's first commit lands on the manufactured fork baseline's exact coordinate, which log-matching fuses silently; an observer empty still grants votes, so the fork holder's manufactured log wins the only possible election and the forced snapshot's boundary config is what promotes the joiner (fresh BOOTSTRAPPED ids keep full-voter blueprints — the distinction is the catalog's, and the catalog is the split registry). Per-group lifecycle GENERATIONS were pre-committed here on the assumption that the factory would consume advisories from the ASYNC lifecycle tail, where the host's lifecycle can move between capture and consumption; the shipped factory instead runs SYNCHRONOUSLY inside the driver crank — poll, materialize, admit in one pass, with no lifecycle mutation able to interleave — so no staleness window exists to state-bind, and generations move to the explicit future condition: if an async/deferred factory is ever introduced, its advisories must carry the incarnation they observed | driver + reactor |
| **6** (done) | Snapshot-bootstrapped group creation — SHIPPED as `create_group_from_fork` (container, both multi coordinators, every multi driver/handle incl. the sharded plane routing): a fork is a MANUFACTURED SNAPSHOT INSTALL — baseline meta (index 1, term 1), the caller's AUTHORITATIVE blob persisted, log compacted-through-1 — booted through the `Endpoint::restart` path so its validation/poison discipline is inherited wholesale, which forces every zero-progress joiner onto the snapshot path (an uncompacted fork would LOG-WALK the joiner: only post-fork entries replayed onto its EMPTY state machine — silent divergence). A fork is a LOCAL act by an already-authorized replica: never solicited over the wire, never factory-reachable, and it never clears a tombstone; admission rides the same floor-first gate as create/restore. **SPLIT is SHIPPED** on this substrate as a three-layer choreography, one committed `Split` admin entry (child id as raw bytes + two lineage counters + an opaque instruction — G-free so the group-unaware core can decode it, and the forked state NEVER rides the entry: wire cost independent of FSM size): the ENDPOINT applies at the deterministic point (`fsm.split` beside SetReadMode/ConfChange, the recovery blob derived AT APPLY from the just-forked half so blob and FSM correspond by construction, the fork staged, the parent's snapshots FENCED at the oldest outstanding split index — the fork durability barrier, without which a correlated crash after a parent compaction could lose the child's only recovery source); the CONTAINER relays (`poll_pending_fork` → typed child decode, a relay-time lineage guard seeded from DURABLE state so restart-replayed forks re-relay while same-gen retry duplicates fold to resolved no-ops, a non-member-host short-circuit, hosted-child conflicts PARKED — blob held, the parent fence standing, a one-shot conflict signal for the embedder, consumed by the drivers only once the bounded lifecycle tail accepts it (backpressure defers the cue; a park that resolves first purges it) — until the squatter leaves (materialize) or the same-lineage twin catches up (redundant), the child config rebuilt from the parent's local tuning under the fork's voter set); the DRIVERS materialize (the fork drain runs BEFORE the storage crank's flush, so ONE engine barrier covers registration + authoritative blob + both lineage records before the child can transmit — a child that can solicit peers is always locally blob-durable first, and the drain front-runs the factory drain so a local fork wins any same-id race — then the parent's fence lifts and the typed `LifecycleEvent::SplitApplied{parent, child}` fires). The materialization IS `create_group_from_fork` (the manufactured install above, now also stamping the child's incarnation and its INHERITED read mode into the baseline meta), so a fresh joiner of a split-born child is structurally forced onto the snapshot path. A factory that vouches a split-born child's id on a non-forked host must blueprint the OBSERVER shape (self outside the seed voters — the Phase-5b fork-born rule): the materialized empty then cannot campaign, the fork holder's manufactured baseline wins every election, and the forced snapshot promotes the joiner — a full-voter empty is promotable with a virgin timer, and an empty quorum's first commit collides with the manufactured `(1,1)` baseline coordinate, silently fusing divergent committed state. Gates by layer: leader/joint-config/hosted-child/child-encoding at the container (`propose_split`), the child-id floor at the COORDINATOR delegators through the per-call `FloorStore` seam (fail-fast leg; the drain's admission recheck stays authoritative), same-plane (`shard(child) == shard(parent)`) at the sharded handle — typed `SplitError` across all three producers. Two pin refinements changed public shape: `split`/`absorb` are DEFAULTED methods on `StateMachine` (`Option<Self>`/`bool` — a subtrait would infect every `apply_committed` monomorphization; a committed split against a default FSM poisons, `SplitUnsupported`), and `Event::SplitApplied` carries the child id as BYTES (events stay G-free; the typed surface is the drivers' lifecycle tail). **MERGE is SHIPPED** on the same substrate (see the merge section below the phase notes). Epoch doctrine, settled in advance: generations are INTRA-group — allocated by the group's own conf changes and fenced by persisted tombstones carrying a next-incarnation floor (CockroachDB's `NextReplicaID` model) — never by a central allocator | proto + coordinators + drivers |

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

**Deployment note (membership churn).** A removed or partitioned member whose election timer fires
campaigns at a higher term and, with an up-to-date log, deposes a live leader — the Raft-thesis
§4.2.3 disruptive-server problem, multiplied across co-hosted groups. `check_quorum` makes members
ignore vote requests while they observe a live leader, and `pre_vote` stops the term inflation.
Both are per-group knobs on a multi-group host; the library defaults stay OFF for etcd-raft parity,
but **reshape-born groups default them ON** — the multi hosts force `pre_vote` + `check_quorum` on
every split child and on the RESHAPE/rejoin subset of factory materializations, because a reshaping
id makes membership churn steady-state, exactly where the disruptive-server window recurs. A factory
serves both reshape births and fresh day-0 materializations, so the force is gated on the blueprint's
provenance: an observer-shaped (fork-born) or reshaped-generation (`> 0`) blueprint is forced, while a
day-0 full-voter blueprint keeps the caller's config byte-for-byte. Single-group and
embedder-`with_group` deployments keep the etcd-parity defaults; an embedder that reshapes
pre-created groups should construct them with the same two flags.

The IGNORANCE half is cured on delivery: the leader's farewell carries the excising commit to the
pruned peer (whose progress is already gone) — an append for a straggler, a commit-carrying heartbeat
for a caught-up peer — and a LOST farewell in EITHER arm is re-driven on a bounded blind budget, so
the removed peer applies its own removal and self-removes, never a bare "you are removed" assertion.
Two residuals the retry cannot reach are now closed too:

- **The compacted / never-had-the-entry tail**, where the farewell suffix is gone below
  `first_index` and every retry shot burns on the clamped-heartbeat fallback. The COURTESY SNAPSHOT
  closes it: a leader that hears from a sender its committed configuration does not name — and that
  the farewell budget no longer owns — offers one whole-blob `InstallSnapshot` carrying the
  post-removal ConfState, rate-bounded to one per peer per three election timeouts. The peer installs
  it — or, when its own durable log already holds that boundary, commits and applies through it
  without a transfer, the boundary being proof it was committed — applies the excluding membership,
  surfaces the SAME `ConfChanged` a log-applied removal emits (so `RemovedSelf` fires whichever way
  the removal arrived), and disarms. Every no-transfer answer is withheld until its evidence is
  crash-surviving: the term durable, the boundary covered by a persisted commit or a durable blob,
  and apply past it. That gate — not the receiver's volatile commit index — is what decides an ack is
  truthful, so a retried offer arriving mid-window cannot slip out an answer the way a branch on
  volatile state would. Both paths therefore leave evidence a crash preserves, which is what lets the
  ack discharge a debt; the shortcut also preserves the durably-acked tail above the boundary,
  keeping the install path in agreement with what a restart would have derived. An offer is only ever made at a term the
  peer will accept, since anything staler dies at the peer's own term pre-pass. The cost is bounded
  and stated: at the etcd-parity defaults an ignorant removed peer's campaign deposes the leader,
  and every such deposition is self-healing — the peer cannot win a quorum of a configuration it is
  not in, so the live members re-elect and the new leadership's first-tick proactive offer, made at
  the lifted term, follows within a heartbeat. The cure lands on the FIRST DELIVERED offer, after
  which the peer can never campaign again. A debt is discharged only by evidence — the peer's own
  completed acknowledgement at or past the earliest boundary offered for that removal, honored
  whatever role this replica now holds, a committed re-add, self-removal, or the map's capacity
  eviction — never by a count of attempts, so under persistent targeted loss of every courtesy frame
  the group degrades to one self-healing deposition per election-timeout window with the cure still
  standing, never to an uncured peer nobody owes anything. Under pre-vote, which every reshape-born
  group defaults to, the peer never inflates a term and the cure lands with zero depositions. Nothing suppresses the removed peer's own traffic
  to shorten that: a leader with stale configuration history cannot tell a departed peer from a
  re-added one, so muting inbound reconciliation can wedge the group, while suppressing our own
  sends can only delay a cure. A snapshot re-baselines
  regardless of log continuity, which is exactly why it reaches where the append cannot; and the
  compacted class implies the snapshot exists, since a peer is only below `first_index` because a
  capture past the compaction point was taken.
- **Contact from a RETIRED incarnation** — a merged-away source or a forked-away stale id. The
  DEMUX GENERATION FENCE closes it: every frame's group header carries the sender's committed
  generation for that gid (WIRE.md §6), and a receiver drops any frame whose stamp is below its
  DURABLE admission floor, of every message class, before the endpoint sees it. A retired
  incarnation is therefore structurally inert at every up-to-date peer, durably and across
  restarts, rather than merely eventually reaped. The comparator is the RETIREMENT floor, not the
  live shape generation, so a replica trailing a reshape still admits at an applied sibling; equal
  always admits.

Both cures act only on FULLY-APPLIED configuration truth: a freshly-elected leader's applied view
is stale by whatever its election-inherited tail holds — possibly a committed re-add of the very
peer a cure names — so every removal cure (the farewell re-drive and the courtesy offer alike)
waits until that leader's own first entry applies, at which point the tail's apply fold has already
pruned whatever it voided.

Both refuse rather than destroy, and neither ever authorizes removal by assertion: the fence only
DROPS frames, and the courtesy install only delivers a committed snapshot the peer removes itself by
APPLYING. The install path additionally REFUSES an excluding ConfState carried at a generation below
the receiver's committed one — a cross-incarnation removal directive has no admissible form, so a
stale view can never reap a live replica.

**The residuals that remain**, both belonging to the embedder's durable catalog reap rather than to
any wire mechanism: a courtesy blob too large for one frame is SKIPPED (chunked courtesy would need
ack routing for a peer that has no Progress to route through), a leader that never captures a
snapshot past the removal never has an eligible one to offer, and a removal applied by a follower
that later leads fires no farewell at all, because only the leader that applied the removal holds
the peer's proven `match` the append arm anchors on. That last residual is the FAREWELL's alone:
the courtesy debt needs only the removal INDEX, which is apply-time knowledge every replica has, so
it is minted on all of them — at BOTH edges where a replica learns a removal, the log-applied conf
change and a snapshot install whose membership transition drops the peer (the boundary is the index
then, and that same snapshot is by construction able to pay the debt). Whichever member leads next
therefore already owes the departed peer its cure, and the disruption cycle cannot recur at a later
leader the way the farewell gap can. The install edge reconciles both directions: it prunes debts
for peers the new configuration RE-ADMITS and mints them for peers it drops.

The install's removal event keys on that same transition, never on mere absence from the installed
ConfState: a fresh joiner or a fork-born observer routinely installs a snapshot captured before it
was ever admitted, and reading that as a removal would tell a replica mid-join to tear itself down.
Absence is history; only a member-to-nonmember transition against the receiver's own applied
configuration is removal — measured across the full membership dimension, so a voter demoted to
learner is a role change and not a departure. Neither can strand data — an ignorant-but-alive replica holds every committed entry it ever
acked and cannot win an election it should not (§5.4.1 plus the `become_leader` voter re-check).

The removed-follower lifecycle e2es, the farewell-retry tests, the courtesy-cure e2es, and the
fence-inertness e2es model the cure on both transports.

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
`Frozen` and the target `MergeParked` throughout. A source that still owes a STAGED FORK is held the
same two ways as an owed thaw — refused `SplitInFlight` at the freeze door, and its absorb (or husk
dissolve) held every crank — because consuming it destroys the split-away half's only local
derivation: the `Split` entry, or the queued fork's in-memory blob once a rebaseline has retired that
entry (which clears the CAPTURE barrier while deliberately keeping the queue, so the hold keys on
both). The obligation is host-local, so a sibling that already flushed the child's baseline can
commit the consumption anyway, which is why the resolver's holds — not the door — are the guarantee.
The one composition with no local release is a fork whose child id IS the merge target: the fork
waits on the occupant, the occupant is `MergeParked` on this absorb, and the absorb waits on the
fork. It is signalled `MergeBlockedCause::ForkFence` rather than resolved — the hold turns what was
a silent drop of the split-away half into a wedge an embedder can see and act on.

Recovery for a genuinely-DEAD participant is the embedder's catalog, exactly like any dead group:
a frozen source or parked target is restored (or floored), and the ONE deliberate escape is an
**OWED source** (a frozen source a hosted target already owes a thaw) — removing it ADMITS, because
the container's removal purge binds every holder's obligation to the departing incarnation and the
driver floors the id. The freeze gates cover the whole admin propose family (a frozen group refuses
splits and refuses to be a merge target; a mid-absorb source refuses a fresh freeze), and
`pre_vote`/`check_quorum` recommendations are unaffected by the freeze — a frozen group elects
normally.

## Merge liveness

Every park above resolves from committed state, but three shapes hold a park — or its durability
— on a timescale no consensus event closes. Each has a cure, and none of the cures weakens a
barrier.

### The under-hosted park

A replica that never hosted the source (lifecycle churn tore it down, or the replica joined after
the source dissolved) cannot fold: the union is not materializable here, and aborting instead
would skip it on this replica alone — silent, permanent divergence from every replica that
absorbed. Its only exit is the resolved quorum's post-merge snapshot. That exit is unreachable
for the population that needs it most: a parked replica is not log-lagging (the park sits ABOVE a
fully replicated log) and its apply stall is purely local, so every leader-side signal reads it as
healthy and no snapshot is ever sent.

The cure is an advertisement, an out-of-band install, and — for the one replica nobody installs to
— a handoff.

- **The advertisement.** `service_merge_applies` re-derives the unresolvable classification every
  crank and the follower stamps the boundary on its `HeartbeatResponse` (`stuck_boundary`, zero =
  absent, WIRE.md §1). A leader that has quiesced solicits no acks at all, so a slow-tick belt
  drives ONE unsolicited response per election timeout to the known leader, with the lease fields
  pinned ZERO — echoing a remembered round would extend a `LeaseBased` lease on support that was
  never promised at that time. A LEADER never advertises: it is the consumer. The classification
  is GATED on no fork durability barrier standing and no abort obligation naming a
  hosted-and-frozen source, because the adopt below clears both, and clearing them on host-local
  proof would destroy a staged fork's only replay derivation, or the only drive for another
  group's thaw.
- **The cure send.** An advertised boundary from a TRACKED member mints a leader-side cure debt on
  the courtesy-snapshot pattern: one whole blob per peer per cooldown, eligible only once this
  leader's own DURABLE snapshot COVERS the boundary (a lower capture cannot carry the union;
  deferral costs nothing, since the leader's own forced capture covers it promptly), discharged
  only by completed evidence at-or-past the boundary. The peer's `Progress` is never touched — it
  stays in `Replicate`, keeps taking appends, and keeps feeding the commit quorum throughout the
  transfer. A debt whose peer stops advertising expires after a few election timeouts: the park
  resolved some other way, and a ghost debt would ship whole blobs at nobody.
- **The adopt.** The receipt-time arm installs a covering blob IN PLACE OF the fold: state moves
  to the boundary, the park clears, and the LOG IS KEPT — the tail above the boundary replays, so
  the adopt discards nothing and no acked entry is ever destroyed. The freeze quartet clears
  unconditionally and `freeze_pending` is re-derived from the kept tail, so a chained
  frozen-and-parked host exits unfrozen at exactly the state a restart's compaction at the
  boundary would leave it. A replica genuinely log-behind the boundary takes the ordinary restore
  path, as before.
- **The wedged LEADER.** No one installs to a leader, so a leader holding a locally-unresolvable
  park has no exit of its own and its exit is another leader: it hands leadership to the
  highest-matched voter that is not itself advertising, ONE forced handoff per term (the transfer
  machine's own latch is the single shot, which bounds the proposal freeze and lease revocation
  each attempt costs). With no such candidate NOTHING is armed: churning leadership between hosts
  that are all uncurable buys no progress and pays that cost every term, so the group stays
  degraded-alive under the signal below. `TimeoutNow` bypasses pre-vote, so a mis-timed pick can seat another
  parked replica — a locally-resolvable park cures itself, an unresolvable one advertises and runs
  the same leg next term, and the token walks until it lands on a curable host.

**The whole-blob bound and its loud residual.** A cure rides ONE frame. A blob that exceeds that
bound cannot be sent, and the leader signals `Event::MergeCureUndeliverable` rather than deferring
in silence — the deliberate asymmetry with the courtesy path, which SKIPS an oversized blob: that
residual leaves a removed peer ignorant-but-alive, the safe direction, while this one leaves a
VOTER wedged. Chunked cure transfer is the eventual exit; until it lands the signal is the whole
contract, and recovery falls to the embedder's catalog like any dead group.

### The fence-deferred capture

A parked absorb whose forced capture a STRUCTURAL replay fence refuses — a staged fork's
durability barrier, or an undischarged abort obligation — would otherwise hold the park for that
fence's whole embedder-timescale life, and the abort fence can be UNDISCHARGEABLE behind the park
(its clearing witness rides an entry the park itself keeps from applying). The arm therefore
absorbs and defers only the capture: fold, unpark, and record the held `Merged` as the target's
capture debt, surfacing `MergeResolution::Absorbed` (`LifecycleEvent::MergeAbsorbed`). Transient
fences (a staged capture or install, draining within cranks) and a LIVE FREEZE still HOLD the
park instead — folding into a frozen target would advance state a claiming target has pinned at
its freeze boundary, and the freeze lifts by protocol anyway.

The driver folds `Absorbed` as the `CaptureFailed` source half MINUS the poison and the restart
demand: fail the source's parked routing with its own verdict (`DriverError::SourceAbsorbed` —
neither shutting-down nor poisoned is truthful; its callers park on the vanished endpoint's
completions and would otherwise hang forever), drain the routing's completion-panic
latch, clear the source's volatile per-group maps — and PRESERVE its stores and floor untouched.
No floor write, no storage teardown, no tombstone: they remain the union's only restart
derivation until the capture stages, and a crash meanwhile restores the source and re-parks the
merge.

Three producers discharge a debt, each surfacing the held `Merged` with its ordinary
floor-and-teardown contract: the **fence-lift forced capture** (per crank, once the fence's own
legs clear at the boundary); **any ordinary capture** staged at-or-past the boundary (it shares
the same fence set, so neither races the other); and **durable coverage that predates the
window** — a completed install or capture already at-or-past the boundary — once the membership
fence has itself released (durability alone is not compaction: a completion-time redundant
install raises the durable index while deliberately keeping the log, so a durable-covered
debtor behind a standing fence still stages its capture). A DESTRUCTIVE install DURING the
window flows through the one ordinary install path and SUPERSEDES the chain, by the same rule
that rebaselines covered abort obligations: everything below the blob's boundary was resolved
globally by the transferring leader's own discharge barrier, so the completion clears the
chain WITHOUT surfacing `Merged` — this host authorizes no teardown it did not run. The prior
sources' terminal floors reach it by propagation, and until then their preserved stores and
engine records stand exactly as a husk's — never re-admittable, torn down off the propagated
floor. This is also what makes a partitioned debtor whose thaw witness was compacted away
curable at all: that install is its only way back, and a crash on either side of it is
consistent — before, the unchanged log re-derives the debts; after, the restored state is past
the window with nothing left to authorize locally.

A debt is HOST-LOCAL — the fences that deferred it are this replica's own — so a foreign-led
freeze can legally commit the DEBTOR's own consumption as the next merge source while the debt
still stands here: the propose-time refusal ran on a debt-less replica. The two resolver
teardowns INHERIT the debt chain rather than drop it. A `Clear`-classified absorb discharges the
whole chain into its own one-crank capture barrier — the snapshot covers the debtor's state
machine, which has carried every prior union since its absorb applied. A `Defer`-classified
absorb chains the consumed source's debts onto the target's own minted debt instead of holding
the park — an abort fence's clearing witness can ride ABOVE the park, so holding would be a
circular wait — and one later covering capture discharges the entire chain. The husk dissolve
surfaces the chain alongside `Retired` on the propagated-floor evidence (a claimant that itself
deferred writes no terminal floor until its own debts discharge, so the floor gate serializes
chains by construction). All of these surface the ordinary `Merged` resolution per prior source
with no endpoint event — the holder that would have carried it is consumed, the `Retired`
asymmetry.

While a debt stands, the DEBT — not the park's naming, which died at the defer — fences every
surface that can revive or destroy either group, and every refusal self-releases at the discharge:

- `remove_group` refuses the debt-named source (`RemoveError::SpokenFor`) and the debt-holding
  target (`RemoveError::OwesCapture`, whose discharge is the source's only exit to the terminal
  floor).
- `create_group`/`restore_group` refuse the debt-named id (`CreateGroupError::AbsorbPending`) —
  either would run a fresh husk beside the union its preserved stores still back.
- Both coordinators' demux fences drop the named source's frames silently, exactly as for a
  tombstone: no close (the shared connection carries the live groups' traffic) and no
  unknown-group advisory, which would prompt precisely that revival.
- The drivers' factory pre-build gates refuse to materialize it: a defer-window source passes
  every other gate (the blueprint names the solicitor, no terminal floor has landed, no split
  reserves the id), so this leg is the only one that refuses.
- The merge verbs refuse the debt holder BOTH roles (`MergeError::AlreadyPending`), preserving the
  one-absorb-at-a-time posture the park used to carry; the conf-change fence engages continuously
  across the handoff through the absorb index the fold recorded.

An inner teardown that bypasses every public refusal leg must never consume a debt-holding target;
that holds today by the call graph and is pinned as a debug assertion so it stays a contract.

### W3 — the install-supersede completion

A snapshot install past a fork coordinate runs its log restore FIRST, so the replay source the
fork barrier protects is already gone by the time the barrier is consulted; left standing it could
only wedge, refusing forever a capture no replay can ever need. The RESTORE path therefore clears
the barrier of every fork still QUEUED at-or-below the installed boundary, while KEEPING the queue
entry: a queued fork reads no log, so the child stays materializable from the in-memory blob for
the process lifetime — strictly better than dropping it — and a later lift no-ops on an absent
key. A fork already POPPED into the driver's flush window keeps its barrier, because the ordinary
resolve lifts it moments later and freeing it early would release the fence under an in-flight
materialization whose baseline is not yet durable. The ADOPT path keeps its barriers and their
meaning — there the fences defer the adopt's persist instead of being cleared.

### Observability

A merge held by a structural cause surfaces as `MergeBlocked { target, source, boundary, cause }`
with `cause` one of `SourceUnhosted`, `SourceBehind`, `ForkFence`, `AbortFence`, `Frozen`. It is
EDGE-triggered: once per transition of a target's cause, retired when the park or debt resolves,
never once per crank. The drivers forward it as `LifecycleEvent::MergeBlocked` on the best-effort
lifecycle tail.

It is an OBSERVATION, never a command. The container re-derives every hold on every crank whether
or not anyone reads this, so a dropped signal costs a notification and nothing else. Two causes
are the placement brain's to act on — `SourceUnhosted` (place the source, or let the leader's cure
arrive) and `ForkFence` (resolve the split conflict standing behind it); the rest lift on their
own protocol timescale. It exists because both held shapes are otherwise invisible from outside: a
parked target is not log-lagging, and a debt-holding target looks entirely healthy while its conf
changes are fenced and the consumed source's id is un-reusable.

### Quiescence

Every cure rides the slow tick, and a quiesced group's deadlines are excluded from the driver's
armed fold and skipped in its due sweep — so quiescing a replica that carries cure work silences
the exact cadence that would end it.

- `group_idle` refuses the LEADER side: a parked merge, an outstanding capture debt, and a
  standing cure debt each make the group ineligible. A wedged peer is invisible to the pump
  predicate (it is not log-lagging), so the debt is what keeps the group awake until the cure
  lands.
- The drivers refuse the FOLLOWER-side entry — the one a leader's flagged beat drives, which no
  eligibility check of theirs gates — while an unresolvable park hint, a capture debt, or a cure
  debt stands, and EVICT a group that acquires one after entry (the classification is re-derived
  every crank and can first hold on a crank after the group went quiet).
- An ADVERTISING `HeartbeatResponse` is wake-class at the coordinator's demux where an ordinary
  one is not, so a follower's advertisement re-arms the quiesced leader that has to answer it.
- A fence lifting while a debt-holding group is quiesced is benign: the discharge runs on the next
  wake, and the embedder action that resolves such a fence is itself wake-class.

## Ownership boundary

Split and merge are an OPAQUE fork/fold: the LIBRARY owns fork/fold correctness and incarnation
safety — lineage counters, admission floors, tombstones, and fork provenance — while the EMBEDDER
owns range ownership, descriptors, adjacency, and routing. The library decides how a group forks
its state, fences the child's durability, and floors a dissolved id; it never decides WHICH group
serves which keyspan, nor WHEN to split or merge. Those are placement decisions, and the placement
brain is the embedder's (the blessed embedded policy loop above). The library never interprets
range boundaries — a `Split` instruction is an opaque blob to the group-unaware core, and the
merge direction rule sorts ids, not ranges. The range map, the adjacency that makes two groups
mergeable, and the routing of a key to its serving group all stay entirely embedder-side.

## Id-reuse constraint

A tombstone-cleared group id may be re-used only for the SAME logical range/group it previously
named — never repurposed for a DIFFERENT logical group. There is no wire-level incarnation epoch
yet: the frame envelope tags the group id but carries no incarnation, so a delayed frame from the
old incarnation could reach a repurposed id and demux into the wrong group's `Endpoint`. Re-using
an id for its own successor incarnation is safe — the intra-group generation floor (the persisted
tombstone's next-incarnation floor, CockroachDB's `NextReplicaID` model) fences a stale same-range
frame — but that floor cannot tell a DIFFERENT logical group wearing a recycled id from the group
that id used to name. The SNAPSHOT plane is already fenced: a snapshot's lineage token rides its
meta, a replica adopts a foreign lineage only from content-emptiness, restart reconciles the durable
hard state's lineage record against the slot before any coordinate arm, and transfer-progress acks
are match-inert — so a recycled id's snapshot traffic cannot fuse two lineages' durable state. The
APPEND/VOTE plane remains coordinate-trusting until the wire-level incarnation stamp lands (the
reserved group-header field, WIRE.md §6), and its exposure is TWO-TIERED: entries colliding at the
same index under DIFFERENT terms are caught by the committed-truncation fail-stop — loud and
content-preserving — but a collision at an IDENTICAL `(index, term)` coordinate is indistinguishable
from the entry already held (log matching never compares payloads) and fuses SILENTLY. No mitigation
closes the second tier: `pre_vote`/`check_quorum` only narrow the window in which a foreign leader
is heard. Re-using an id for a DIFFERENT logical group is therefore unsafe on this plane regardless
of configuration; until the stamp's enforcement lands, keeping a recycled id bound to its original
logical group is the embedder's constraint to honor.

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

## Upgrade notes (P6)

- **Wire**: `LABEL_VERSION` 3 is the reshaping baseline; all nodes upgrade together (the hello
  fences a mixed deployment into refusing connections, never mis-decoding — WIRE.md §4). The next
  version's group-header incarnation stamp is reserved, not landed (WIRE.md §6).
- **`StateMachine`**: `split`/`absorb` are DEFAULTED methods (`Option<Self>` / `bool`), so existing
  FSMs compile unchanged; a committed split/merge against a defaulting FSM fail-stops
  deterministically on every replica (`SplitUnsupported` / `MergeUnsupported`) rather than
  diverging. Opting in is implementing the pair.
- **Lineage counters**: an unreshaped group's generation is `0` everywhere (absent on the wire), so
  pre-P6 state needs no migration; counters move only through committed reshaping entries.
- **Out-of-tree stores** (the two normative round-trip contracts): a `StableStore` must hand back
  `SnapshotMeta` VERBATIM — `shape_gen` and `fork_id` included (the meta-fidelity contract on
  `submit_snapshot`) — and must round-trip `HardState::lineage` (absent decodes to `None`, which is
  exact, not conservative). Dropping either silently breaks adoption, restart reconciliation, or
  chunked transfer resume.
- **Disk engines**: the one breaking engine-contract change is the per-group lineage/floor records
  (incarnation gen + admission floor) that OUTLIVE group removal and ride the same flush barrier as
  the stores — mirror `multi::engine`'s reference semantics.

## References

- etcd/raft `RawNode` (the single-group core) and the `MultiNode` removal (`4b3a7ff`).
- TiKV `raftstore`: `RaftBatchSystem`, `StoreFsm`/`PeerFsm`, `BasicMailbox`/`Router`,
  coalesced heartbeats, Hibernate Regions, region-prefixed keys in a shared engine.
- CockroachDB: `raftScheduler`, coalesced heartbeats, quiescence, range split/merge.
- openraft: `GroupRouter` keyed `(target, group)`; `IOFlushed` completion callback.
