//! The deterministic multi-group world: one [`MultiRaft`] container per node over a group-tagged
//! typed bus. See the [module docs](crate::multi).
//!
//! The run loop is the exact analogue of [`Cluster::tick`](crate::Cluster): advance the single
//! global virtual clock to the earliest pending deadline, fire every due `(group, deadline)` on
//! every host, then settle (drain outgoing → deliver due → drain storage) until quiescent at that
//! timestamp. Per-node clock drift and the failover wall are deliberately absent in v1 — the
//! single-group VOPR retains that coverage; the hooks stay reserved here.

use super::{
  conservation::ConservationLedger,
  oracles::{self, GrantKey},
};
use crate::{
  AppliedLog, Checker, DurableEntry, LogSm, MemLog, MemStable, NetworkFaults, StorageFaults,
  StoreMode, checker, network::NetPrng,
};
use core::time::Duration;
use sailing_proto::{
  Config, Event, ForkId, Instant, LogStore, Message, MultiRaft, Outgoing, ProgressState, ReadState,
  StableStore,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// An in-flight group-tagged typed message: `(deliver_at, gid, from, to, generation, message)`.
struct GInFlight {
  deliver_at: Instant,
  gid: u64,
  from: u64,
  to: u64,
  /// The SENDER's lineage generation for `gid` at send time — the wire's incarnation stamp
  /// (WIRE.md §6), which the receiver's admission floor fences at delivery.
  generation: u64,
  message: Message<u64>,
}

/// Membership observations awaiting their checker, keyed by the OBSERVING replica's incarnation
/// and carrying `(boundary index, term, config)` per observation.
type ObservationQueue = BTreeMap<(u64, u64), Vec<(u64, u64, checker::ConfSnapshot)>>;

/// A deterministic world of [`MultiRaft`] container hosts. Nodes are empty containers until
/// [`create_group`](Self::create_group) wires a group onto its member nodes; each `(node, group)`
/// replica owns its own [`MemLog`]/[`MemStable`] pair, mirroring per-group stores in production.
pub struct MultiWorld {
  /// The world seed: threaded into every group's election jitter (the container folds the group
  /// id in, so co-located groups draw decorrelated timeouts).
  seed: u64,
  /// The single global virtual clock (every node's groups share its clock, as in production).
  now: Instant,
  /// Node ids in creation order (ascending in every current use; kept explicit for determinism).
  node_ids: Vec<u64>,
  /// One container host per node.
  hosts: BTreeMap<u64, MultiRaft<u64, u64, LogSm>>,
  /// Per-`(node, gid)` log store.
  logs: BTreeMap<(u64, u64), MemLog>,
  /// Per-`(node, gid)` stable store.
  stables: BTreeMap<(u64, u64), MemStable<u64>>,
  /// The group-tagged typed bus.
  bus: VecDeque<GInFlight>,
  /// Fully-partitioned node ids: their outgoing messages are discarded and inbound messages
  /// to/from them are dropped (a node-level partition takes ALL of the node's groups out).
  isolated: BTreeSet<u64>,
  /// Completed [`tick`](Self::tick)s (threaded into oracle panics for replay).
  tick_count: u64,
  /// One safety-oracle suite per LIVE INCARNATION `(gid, generation)` — unchanged oracle code,
  /// parameterized by the per-incarnation [`ClusterView`](crate::ClusterView) assembled from the
  /// replicas bound to that incarnation. Keyed by incarnation rather than by id because two
  /// incarnations of one id can be hosted at once (a late fork beside a recreation), and a frozen
  /// archive judges nothing — an island must have a LIVE checker or it goes unjudged.
  checkers: BTreeMap<(u64, u64), Checker>,
  /// The one-identity tripwire: `(granter, gid, gen, term) → grantee` over every REAL-vote grant
  /// any replica ever sends (see [`oracles::note_grant`]).
  grants: BTreeMap<GrantKey, u64>,
  /// Per-`(node, gid)` count of applied entries already cross-talk-swept (the sweep high-water).
  /// The sweep FLOORS its start at the group's fork-inherited baseline (see
  /// [`lifecycle::GroupMeta::fork_baseline`]) at every pass, so inherited parent-tagged cells
  /// are never judged as cross-talk no matter how a replica acquired them.
  swept: BTreeMap<(u64, u64), usize>,
  /// Per-`(node, gid)` count of `Event::ConfChanged` drained (the [`NodeView`](crate::NodeView)
  /// `conf_changed` feed and the per-group conf-change settle signal).
  conf_changed: BTreeMap<(u64, u64), u64>,
  /// `(node, gid)` replicas whose membership is SNAPSHOT-DERIVED (a transferred snapshot
  /// installed) — sticky, mirroring the single-group lineage flag.
  snapshot_lineage: BTreeSet<(u64, u64)>,
  /// Per-group committed conf-change transitions observed since the last check (fed to the
  /// membership oracle exactly as `Cluster::pending_transitions` is, then cleared). Keyed by
  /// INCARNATION, like the checkers that consume it: a gid-keyed queue is drained by whichever
  /// incarnation the checker loop reaches first, starving every other one of its own observations.
  pending_transitions: ObservationQueue,
  /// Per-group new transfer-snapshot installs observed since the last check (then cleared).
  pending_new_installs: ObservationQueue,
  /// The harness-side group registry: one [`lifecycle::GroupMeta`] per logical group id, holding
  /// the CURRENT incarnation's expectations (retirement flips `retired`; recreation bumps
  /// `generation` and archives what it replaces).
  groups: BTreeMap<u64, lifecycle::GroupMeta>,
  /// Expectation meta for incarnations the registry no longer holds, keyed `(gid, generation)`.
  /// A recreation ARCHIVES what it supersedes rather than destroying it, because a replica of the
  /// superseded incarnation can still be hosted somewhere — a fork that materializes after the
  /// ceremony lands as its own older incarnation — and it must be judged against ITS expectations
  /// (its inherited baseline, its tag lineage, its key population), not against the successor's.
  /// Judging it against the successor is the single-meta misattribution: legal inherited cells read
  /// as a cross-group leak.
  meta_archive: BTreeMap<(u64, u64), lifecycle::GroupMeta>,
  /// Frozen checker archive for retired incarnations, keyed `(gid, generation)` — each ran one
  /// final check at removal and keeps its cross-tick history inspectable.
  retired: BTreeMap<(u64, u64), Checker>,
  /// Per-`(directed link, group)` mutes: a delivery whose `(from, to, gid)` is muted drops
  /// silently at the bus (AFTER the send-point oracles, like isolation).
  muted: BTreeSet<(u64, u64, u64)>,
  /// Seeded network fault model applied per message at the bus-push point (all-off default —
  /// byte-identical to the faultless bus).
  net_faults: NetworkFaults,
  /// The network-fault PRNG (a stream distinct from the per-replica store seeds).
  net_prng: NetPrng,
  /// Per-`(from, to)` last-scheduled delivery time — the FIFO clamp when reorder is off (one
  /// physical link per node pair carries every group's traffic, so the clamp is per PAIR).
  net_last_sched: BTreeMap<(u64, u64), Instant>,
  /// Messages dropped by the seeded network fault model (non-vacuity counter).
  net_dropped: u64,
  /// Deliveries the GENERATION FENCE dropped: the sender's stamp for the gid was below the
  /// RECEIVER's admission floor, so it speaks for a retired incarnation (the sim model of the
  /// product's demux fence).
  fenced_dropped: u64,
  /// The DISRUPTION subset of [`fenced_dropped`]: retired-incarnation `(Pre)Vote`s. Every one of
  /// these is a campaign that, unfenced, could have deposed a live leader; fenced, none reaches a
  /// replica at all, so the reshape-removed husk's depose count is zero by construction.
  fenced_votes_dropped: u64,
  /// Message duplications fired by the seeded network fault model (non-vacuity counter).
  net_duplicated: u64,
  /// Per-`(node, gid)` replica config, retained so a node crash can rebuild every hosted replica
  /// from durable state under its original knobs.
  configs: BTreeMap<(u64, u64), Config<u64>>,
  /// Per-`(node, gid)` replica incarnation, bumped on every (re)wire and on node crash — the
  /// per-group checkers reset their commit/term monotonicity baselines on a change.
  restarts: BTreeMap<(u64, u64), u64>,
  /// Per-node crash counter — the durable boot epoch handed to `restore_group` so a restarted
  /// node's forwarded-read tokens are unique across incarnations.
  boot_epochs: BTreeMap<u64, u64>,
  /// Per-`(node, parent)` DURABLE relay lineage — the max `parent_gen_after` of the parent's forks
  /// this node has MATERIALIZED. The container's live relay guard is reset to the (possibly
  /// lagging) durable snapshot meta on restart, so — exactly as a real driver restores it from
  /// the engine's `FloorStore::lineage` (bumped in its fork drain) via `raise_relay_guard` — the restore path feeds
  /// this back so a replayed split folds to a duplicate no-op instead of re-materializing (or, now,
  /// PARKING against) an already-relayed child.
  relayed_lineage: BTreeMap<(u64, u64), u64>,
  /// Per-`(node, gid)` PER-HOST TOMBSTONE — the coordinator's own volatile `retired` set, which is
  /// per-host in production because each host removes its own replica. Set when a host's endpoint
  /// removal commits, lifted by that host's re-admission (a re-wire, or a teardown rolled back) and
  /// by the embedder's explicit consent. The fork relay's gate reads it: a tombstoned id is spoken
  /// for, so a fork naming it HOLDS rather than landing.
  host_tombstones: BTreeSet<(u64, u64)>,
  /// Per-`(node, gid)` REPLICA INCARNATION, bound when the replica is wired and never moved
  /// afterwards — the generation THIS replica object speaks for, which is what production stamps
  /// outbound frames with (a host stamps its own committed generation, never a cluster-wide
  /// registry's). A fork-born replica binds the fork's `child_gen`; every other path binds the
  /// registry generation live at the wire. Two incarnations of one id can therefore coexist on
  /// different nodes with DIFFERENT stamps, which is precisely what a shared-registry stamp cannot
  /// express.
  replica_gen: BTreeMap<(u64, u64), u64>,
  /// Per-`(node, gid)` confirmed `ReadState`s in confirmation order. Monotone and NEVER removed
  /// on replica teardown, so the read ledger's scan offsets stay valid across re-wiring.
  read_states: BTreeMap<(u64, u64), Vec<ReadState>>,
  /// Per-`(node, gid)`: whether the replica's LAST-APPLIED config listed the node itself. The
  /// RemovedSelf teardown keys on the member→non-member TRANSITION — a catching-up observer
  /// applies historical confs that predate its own AddNode (self absent throughout), and tearing
  /// it down there would destroy a committed voter's replica mid-join.
  member_view: BTreeMap<(u64, u64), bool>,
  /// PARKED replicas: delivery-isolated for their group and `removed` in its checker view, with
  /// ALL STATE RETAINED — the multi analogue of the single-group `mark_removed` (which never
  /// destroys a node). The departed sweep parks rather than tears down: a stale-leader reconcile
  /// can misjudge a REAL member as departed, and destroying its replica would punch a hole in
  /// the group view that the quorum-durability oracle rightly flags. Reconcile UNPARKS a parked
  /// replica the committed membership still lists; only an applied self-removal tears down.
  parked: BTreeSet<(u64, u64)>,
  /// Total gid-tagged applied entries the cross-talk sweep has decoded and judged (non-vacuity).
  cross_talk_checked: u64,
  /// A `Config::snapshot_threshold` override applied to every replica the world wires from the
  /// moment it is set (see [`set_snapshot_threshold`](Self::set_snapshot_threshold)). `None` —
  /// the construction default — leaves the library's demand-driven threshold untouched, so a
  /// world without the override is byte-identical to one predating the seam.
  snapshot_threshold: Option<usize>,
  /// Whether every replica the world wires constructs its `Config` with pre-vote on — the
  /// removed-replica disruption cure's prevention layer, set per-group by the reshaping profiles.
  /// `false` — the construction default — applies `with_pre_vote(false)`, which equals the library
  /// default, so a world that never sets it is byte-identical to one predating the seam.
  pre_vote: bool,
  /// Whether every replica the world wires constructs its `Config` with check-quorum on — the
  /// prevention layer's leader-side half, defaulted and applied exactly like [`pre_vote`](Self::pre_vote).
  check_quorum: bool,
  /// The instruction-conservation ledger: per-`(ledger id, key)` write histories recorded from
  /// the replicas' RAW applied records (see `conserve_sweep`), judged per recorded split by
  /// [`finalize_conservation_or_panic`](Self::finalize_conservation_or_panic).
  conservation: ConservationLedger,
  /// Per-`(ledger id, key)` the SET of recorded values — the recorder's dedupe AND its ONLY walk
  /// state. Values are globally unique, so a value already in the set is a re-presentation to
  /// skip and a fresh one is a new cell to record, in first-encounter order, no matter how many
  /// replicas' full re-walks present it. A SET rather than a monotone high-water mark because a
  /// MERGE folds a source's cells under the TARGET's ledger id, and an absorbed cell's value can
  /// sit BELOW the target's own — a mark would drop it as stale though it is a distinct cell of a
  /// different lineage (see `conserve_sweep`). Deliberately NO positional resume watermark rides
  /// beside it: `LogSm::split` and a crash restore both move cells to positions an earlier sweep
  /// already passed.
  cons_recorded: BTreeMap<(u64, u16), BTreeSet<u64>>,
  /// Every committed split the world REGISTERED (child materialized), in registration order —
  /// the conservation verdict's work list.
  splits: BTreeMap<u64, split::SplitRecord>,
  /// Splits proposed through [`propose_split`](Self::propose_split) whose child has not yet
  /// registered: child gid → the parent, the split point, and the population slice assigned at
  /// propose. An entry whose split entry is lost (deposed leader, truncated tail) lingers
  /// harmlessly — the child never materializes and the parent's population stays conservatively
  /// shrunk: the moved keys are PARKED and unroutable, but their CELLS remain in the parent's
  /// record, and a later split whose point covers them hands them to ITS child —
  /// `register_split_child` derives that child's conservation assignment from the fork's own
  /// record, so the parked handover is judged rather than misread as an unassigned key
  /// surfacing.
  pending_splits: BTreeMap<u64, split::PendingSplit>,
  /// Committed splits REGISTERED (one per split, however many replicas materialize) — the
  /// report's non-vacuity witness.
  splits_applied: u64,
  /// `Event::SplitStale` observations drained (a stale mint no-op'd deterministically).
  split_stale: u64,
  /// `(parent, child)` split-conflict signals drained. The world leaves a parked fork's
  /// squatter in place — its embedder model is patient observation (the departed sweep's
  /// pattern), the standing snapshot fence keeps the parked fork replayable indefinitely, and a
  /// park that never resolves surfaces as a quiesce/finalize failure rather than being masked
  /// by a forced teardown. Fresh child ids make the signal unreachable today; the counter keeps
  /// it visible if that ever changes.
  split_conflicts: u64,
  /// Every proposed split's FENCE coordinate: `child -> split entry index` (the parent-log index the
  /// split landed at, identical on every parent replica). Recorded at propose so a later drained
  /// `(parent, child)` conflict can be attributed to the index its standing capture fence sits at.
  /// Persistent (a split's fence index never changes); bounded by the run's split count.
  split_fence_index: BTreeMap<u64, sailing_proto::Index>,
  /// Children whose split the world has seen the PARENT's record actually partition for — the
  /// first drained fork on any host, materialized or refused, since either way the parent's
  /// `LogSm::split` already ran there. Keyed like every other split record, by child id. The fold
  /// anchors for the moved keys retire against this, not against proposal (see
  /// [`MultiWorld::pump_forks`]); recording the event keeps that retirement to the FIRST
  /// confirmation, so a reacquisition anchored between two hosts' drains is not stripped by the
  /// second. Bounded by the run's split count.
  partitioned_splits: BTreeSet<u64>,
  /// Standing fork-conflict fences observed per `(node, parent)`: `split index -> conflicting CHILD`
  /// for each parked-fork squatter the fork pump drained on that node (see
  /// [`MultiWorld::pump_forks`]). A parked fork holds the parent's capture fence at its split index; a
  /// merge park on that same parent whose coordinate sits at-or-above the fence is deadlocked behind
  /// it (issue #110, the fork-fence coupling). The child is retained so the pump can clear a fence on
  /// every barrier-resolution arm — materialize, refuse, or the container's internal REDUNDANT fold
  /// (a provenance-matched twin, invisible to the pump): a fence whose child is now hosted on the node
  /// has resolved (the fresh-id world never hosts a NON-matching squatter at a fork child, so a hosted
  /// child IS the resolved fork). Records are ACTIVE state, never append-only history.
  fork_conflicts: BTreeMap<(u64, u64), BTreeMap<sailing_proto::Index, u64>>,
  /// Late forks the fork pump REFUSED at materialization — the coordinator-admission model
  /// (the child id retired, or recreated past the fork's generation): no materialization, the
  /// parent's fence lifted, mirroring the product's `SplitRefused` resolution.
  split_refused: u64,
  /// Every registered merge (first resolution anywhere), in registration order — the union
  /// verdict's work list plus the absorb-determinism reference record (see
  /// `multi/world/merge.rs`).
  merges: Vec<merge::MergeRecord>,
  /// Per-host TERMINAL merge floors `(node, source)` — recorded when a source's deferred
  /// teardown lands (the world's engine-floor model; the service's absent-arm discriminator).
  merge_floors: BTreeSet<(u64, u64)>,
  /// Per-gid NON-TERMINAL admission floor persisted when the world permanently stops hosting a
  /// RESHAPED id — the embedder catalog's `removal_floor` discipline (one past the id's removal
  /// ceiling). Cluster-wide by construction (the catalog is one fact, not per host): a target
  /// that later re-derives a torn-down source's abort-thaw obligation discharges it off this floor
  /// (`!floor_admits(floor, expected)`) while the source is unhosted, before any recreate. `0`/absent
  /// for an id that never reshaped, so the default (no-merge, no-split) profile is byte-identical.
  removal_floors: BTreeMap<u64, u64>,
  /// Per-gid INCARNATION floor on the lifecycle-registry scale (`generation_of`): one past the
  /// generation each retirement ended, so a straggler stamped by the retired incarnation is
  /// fenced at the delivery seam while the recreation — which comes back at exactly this value —
  /// admits (equal admits). The wire fence's model input; distinct from `removal_floors`, which
  /// answers the CONTAINER-lineage question the merge/abort machinery asks.
  incarnation_floors: BTreeMap<u64, u64>,
  /// Deferred merge-source teardowns `(node, source, target, capture boundary)` awaiting the
  /// target capture's durability on that host (the one-barrier batch model; see
  /// `sweep_merge_teardowns`).
  pending_merge_teardowns: Vec<(u64, u64, u64, sailing_proto::Index)>,
  /// The embedder's own record of every freeze it has proposed: `source -> target`, set at each
  /// accepted `prepare_merge` (last-wins, since one source freezes toward one target at a time). The
  /// world plays the embedder, which knows its merge intentions, so this is the truthful source of a
  /// frozen source's CLAIMED TARGET — the mirror the `merge_choreography_active` predicate reads to
  /// keep a claimed target off the removal draws (the container's `Claimed` gate). Never cleared:
  /// a stale entry is inert, filtered at read by whether the named source's freeze is still active.
  active_freezes: BTreeMap<u64, u64>,
  /// Merged resolutions observed across all hosts (every per-host resolution counts).
  merges_resolved: u64,
  /// Aborted resolutions observed across all hosts.
  merges_aborted: u64,
  /// CaptureFailed resolutions observed across all hosts: a consumed source whose union could not be
  /// made durable (absorb refused, or capture faulted). The absorb-capable/non-faulting sim FSM never
  /// produces one; a non-zero count is the wedge worth reporting, not chasing.
  merges_capture_failed: u64,
  /// Fence-deferred absorbs surfaced as `Absorbed` (the union applied, the capture owed).
  merges_absorbed: u64,
  /// Fence-deferred absorbs whose fold-point determinism check ran at `Absorbed`; the later
  /// discharge-`Merged` skips its re-check (the target legitimately applies past the boundary
  /// inside the debt window).
  absorbed_pending: std::collections::BTreeSet<(u64, u64, u64)>,
  /// Per-`(target, source)` MONOTONE count of `Event::MergeAborted` observations drained across
  /// the run — the abort clock the fuzzer's pending-merge book retires against: a pair booked at
  /// clock `c` is resolved-by-abort once the clock reads past `c` (the absorb side retires via
  /// merge registration instead). A count, not a set, because the same pair can legitimately
  /// freeze, abort, and re-freeze across a run.
  merge_aborts_observed: BTreeMap<(u64, u64), u64>,
  /// Per-gid count of CONSECUTIVE teardown-gate refusals a `remove_group` draw has abandoned while
  /// `merge_choreography_active` read false — the append-pending-residual escalation counter (see
  /// [`remove_group`](Self::remove_group)). Reset on any successful teardown or a live-choreography
  /// read; a transient replication lag clears within a handful of cranks (the streak never grows),
  /// so passing [`TEARDOWN_TIE_BUDGET`](lifecycle::TEARDOWN_TIE_BUDGET) means a genuine
  /// non-superset hole that refuses forever — and still trips.
  teardown_tie_streak: BTreeMap<u64, usize>,
  /// The write mode every replica store the world wires is constructed under. `Sync` — the
  /// construction default, byte-identical to a world predating the seam — is commit-on-submit;
  /// `Async` runs the stores through the staged-write fsync-loss window (submit → `flush` →
  /// durable; a `crash`/rollback `discard_inflight` drops the un-flushed tail), which is what makes
  /// the randomized crash campaign actually EXERCISE lost-fsync durability (persist-vote-before-grant,
  /// append-before-ack, commit persistence, the reshaping lineage across a crash). Set once from the
  /// profile before any group is wired ([`set_store_mode`](Self::set_store_mode)) and PRESERVED
  /// across every store-creating (re)wire path (create/observer/recreate/resurrect/fork-child) via
  /// the [`fresh_stores`](Self::fresh_stores) chokepoint; the crash/rollback restores inherit it for
  /// free by reusing the retained store objects.
  store_mode: StoreMode,
  /// Async flush-phase witness: log stores made durable across the run (`0` under the sync default —
  /// the flush phase never runs). Nonzero proves the multi tick now fsync-flushes (the second cause
  /// the crash suite was vacuous for).
  log_flushes: u64,
  /// Async flush-phase witness: stable stores made durable across the run.
  stable_flushes: u64,
  /// Seeded torn writes (fsync failures) that stranded a REAL in-flight batch across the run — the
  /// lost-fsync coverage's non-vacuity witness. `0` under the sync default.
  torn_writes_fired: u64,
  /// Crashes that rolled back a NON-EMPTY log-store fsync window (`discard_inflight` dropped a
  /// staged tail) — proof the crash campaign lands mid-window rather than only post-flush. `0`
  /// under the sync default (nothing is ever in flight).
  crashes_with_log_inflight: u64,
  /// Crashes that rolled back a NON-EMPTY stable-store fsync window. `0` under the sync default.
  crashes_with_stable_inflight: u64,
  /// A node armed to crash MID-SETTLE on the next tick (see [`arm_mid_fsync_crash`](Self::arm_mid_fsync_crash)):
  /// the tick's settle loop crashes it AFTER a delivery sub-step submitted fresh appends but BEFORE
  /// the store flush, so `discard_inflight` rolls back a genuine replication window — a crash at an
  /// arbitrary instant, not only at the fully-durable tick boundary an ordinary [`crash`](Self::crash)
  /// models. Consumed (taken) by the next tick. `None` for a between-ticks (durable-window) crash.
  pending_mid_crash: Option<u64>,
  /// The per-`(node, gid)` LINEAGE LEDGER (see [`oracles::LineageLedger`]): attributes every applied
  /// command, installed snapshot, and endpoint `fork_id` to a lineage and holds the durable state
  /// single-lineage (chimera), the within-lineage quorum byte-identical (phantom), and every
  /// admitted transfer terminating (wedge). Fed by the per-tick lineage sweep and the install-event
  /// drain, judged at run end by [`finalize_lineage_or_panic`](Self::finalize_lineage_or_panic).
  lineage: oracles::LineageLedger,
}

impl MultiWorld {
  /// An empty world (no nodes, no groups) at the clock origin.
  pub fn new(seed: u64) -> Self {
    Self {
      seed,
      now: Instant::ORIGIN,
      node_ids: Vec::new(),
      hosts: BTreeMap::new(),
      logs: BTreeMap::new(),
      stables: BTreeMap::new(),
      bus: VecDeque::new(),
      isolated: BTreeSet::new(),
      tick_count: 0,
      checkers: BTreeMap::new(),
      grants: BTreeMap::new(),
      swept: BTreeMap::new(),
      conf_changed: BTreeMap::new(),
      snapshot_lineage: BTreeSet::new(),
      pending_transitions: BTreeMap::new(),
      pending_new_installs: BTreeMap::new(),
      groups: BTreeMap::new(),
      meta_archive: BTreeMap::new(),
      retired: BTreeMap::new(),
      muted: BTreeSet::new(),
      net_faults: NetworkFaults::none(),
      // Same stream derivation as the single-group bus ("NET"), distinct from replica seeds.
      net_prng: NetPrng::new(seed.rotate_left(16) ^ 0x004E_4554),
      net_last_sched: BTreeMap::new(),
      net_dropped: 0,
      fenced_dropped: 0,
      fenced_votes_dropped: 0,
      net_duplicated: 0,
      configs: BTreeMap::new(),
      restarts: BTreeMap::new(),
      relayed_lineage: BTreeMap::new(),
      host_tombstones: BTreeSet::new(),
      replica_gen: BTreeMap::new(),
      boot_epochs: BTreeMap::new(),
      read_states: BTreeMap::new(),
      member_view: BTreeMap::new(),
      parked: BTreeSet::new(),
      cross_talk_checked: 0,
      snapshot_threshold: None,
      pre_vote: false,
      check_quorum: false,
      conservation: ConservationLedger::new(),
      cons_recorded: BTreeMap::new(),
      splits: BTreeMap::new(),
      pending_splits: BTreeMap::new(),
      splits_applied: 0,
      split_stale: 0,
      split_conflicts: 0,
      split_fence_index: BTreeMap::new(),
      partitioned_splits: BTreeSet::new(),
      fork_conflicts: BTreeMap::new(),
      split_refused: 0,
      merges: Vec::new(),
      merge_floors: BTreeSet::new(),
      removal_floors: BTreeMap::new(),
      incarnation_floors: BTreeMap::new(),
      pending_merge_teardowns: Vec::new(),
      active_freezes: BTreeMap::new(),
      merges_resolved: 0,
      merges_aborted: 0,
      merges_capture_failed: 0,
      merges_absorbed: 0,
      absorbed_pending: std::collections::BTreeSet::new(),
      merge_aborts_observed: BTreeMap::new(),
      teardown_tie_streak: BTreeMap::new(),
      store_mode: StoreMode::Sync,
      log_flushes: 0,
      stable_flushes: 0,
      torn_writes_fired: 0,
      crashes_with_log_inflight: 0,
      crashes_with_stable_inflight: 0,
      pending_mid_crash: None,
      lineage: oracles::LineageLedger::new(),
    }
  }

  /// Set the per-replica `Config::snapshot_threshold` override (`None` restores the library
  /// default). Applies at replica CONSTRUCTION — call before creating groups; already-wired
  /// replicas keep the config they were built under.
  pub fn set_snapshot_threshold(&mut self, threshold: Option<usize>) {
    self.snapshot_threshold = threshold;
  }

  /// Set whether replicas construct with pre-vote on (`false` restores the library default).
  /// Applies at replica CONSTRUCTION — call before creating groups; already-wired replicas keep
  /// the config they were built under.
  pub fn set_pre_vote(&mut self, on: bool) {
    self.pre_vote = on;
  }

  /// Set whether replicas construct with check-quorum on (`false` restores the library default).
  /// Applies at replica CONSTRUCTION, exactly like [`set_pre_vote`](Self::set_pre_vote).
  pub fn set_check_quorum(&mut self, on: bool) {
    self.check_quorum = on;
  }

  /// Set the [`StoreMode`](crate::StoreMode) every replica the world wires is constructed under
  /// (`Sync` restores the default). Applies at replica CONSTRUCTION — call before creating groups;
  /// already-wired replicas keep the store they were built with, and crash/rollback restores reuse
  /// the retained store, so the mode is preserved for a replica's whole life once set.
  pub fn set_store_mode(&mut self, mode: StoreMode) {
    self.store_mode = mode;
  }

  /// A fresh `(log, stable)` store pair for `(node, gid)` in the world's configured
  /// [`StoreMode`](crate::StoreMode) — the ONE chokepoint every store-creating wire path calls, so
  /// the mode is preserved on create/observer/recreate/resurrect/fork-child alike. Async seeds each
  /// store from the world seed folded with the node and gid so co-located replicas' fault schedules
  /// decorrelate; the seed only governs the pre-`reroll_storage` window, since installing a fault
  /// rate reseeds the store's fault PRNG.
  fn fresh_stores(&self, node: u64, gid: u64) -> (MemLog, MemStable<u64>) {
    Self::fresh_stores_in(self.store_mode, self.seed, node, gid)
  }

  /// The chokepoint's body, over the two world fields it actually reads — so the fork install's
  /// engine seam, which owns the store maps and cannot hold a `&self` beside them, creates the
  /// child's stores through THIS function rather than a second copy of the rule.
  pub(super) fn fresh_stores_in(
    mode: crate::StoreMode,
    seed: u64,
    node: u64,
    gid: u64,
  ) -> (MemLog, MemStable<u64>) {
    if mode.is_async() {
      (
        MemLog::new_async(seed ^ node ^ gid.rotate_left(32)),
        MemStable::new_async(seed.rotate_left(32) ^ node ^ gid.rotate_left(32)),
      )
    } else {
      (MemLog::new(), MemStable::new())
    }
  }

  /// Add node `id` as an empty container host (no groups). Panics if the id already exists.
  pub fn add_node(&mut self, id: u64) {
    assert!(
      self.hosts.insert(id, MultiRaft::new()).is_none(),
      "add_node: node {id} already exists"
    );
    self.node_ids.push(id);
  }

  /// Create group `gid` on every node in `voters` (each node id must already exist). Each member
  /// gets a fresh `(node, gid)` store pair and a fresh replica seeded from the world seed (the
  /// container folds `gid` in, so per-group election jitter is decorrelated for free). Panics on
  /// any admission error — a world-construction bug, not weather.
  pub fn create_group(&mut self, gid: u64, voters: &BTreeSet<u64>) {
    assert!(
      !self.groups.contains_key(&gid),
      "create_group: group id {gid} was already used (ids are single-incarnation; a retired \
       logical group rejoins via recreate_group)"
    );
    assert!(
      self.checkers.insert((gid, 0), Checker::new()).is_none(),
      "create_group: group {gid} already exists"
    );
    self.groups.insert(
      gid,
      lifecycle::GroupMeta {
        voters: voters.clone(),
        keys: (0..super::NUM_KEYS).collect(),
        ..lifecycle::GroupMeta::default()
      },
    );
    let voter_vec: Vec<u64> = voters.iter().copied().collect();
    for &node in voters {
      let config = Config::try_new(
        node,
        voter_vec.clone(),
        Duration::from_millis(1000),
        Duration::from_millis(100),
      )
      .expect("valid multi-world config");
      self.wire_replica(node, gid, config, true);
    }
  }

  /// Wire one `(node, gid)` replica: fresh stores + container admission under `config`.
  /// `is_member` seeds the RemovedSelf transition tracker: `true` for a bootstrap voter (its
  /// founding config lists it), `false` for a catching-up observer (its own AddNode is still
  /// ahead of it in the log).
  /// The prevention knobs `gid`'s existing replicas carry, if it has any — the agreement source
  /// [`wire_replica`](Self::wire_replica) derives a late replica's config from.
  fn live_prevention_knobs(&self, gid: u64) -> Option<(bool, bool)> {
    self
      .configs
      .iter()
      .find(|((_, g), _)| *g == gid)
      .map(|(_, c)| (c.pre_vote(), c.check_quorum()))
  }

  fn wire_replica(&mut self, node: u64, gid: u64, config: Config<u64>, is_member: bool) {
    // The snapshot-threshold override lands HERE — the one chokepoint every replica-construction
    // path funnels through (create/recreate/observer/resurrect), and crash restores inherit it
    // via the retained `configs` entry. `None` leaves the built config untouched.
    let config = match self.snapshot_threshold {
      Some(t) => config.with_snapshot_threshold(t),
      None => config,
    };
    // The prevention-layer knobs land at the SAME chokepoint, so every construction path
    // (create/recreate/observer/resurrect, and crash restores via the retained `configs` entry)
    // carries them. Both `false` — the default profiles — applies the library default, keeping the
    // built config byte-identical to a world predating the seam.
    let config = config
      .with_pre_vote(self.pre_vote)
      .with_check_quorum(self.check_quorum);
    // EVERY REPLICA OF ONE GROUP CARRIES ONE SET OF PREVENTION KNOBS — the container forces them on
    // for a fork-born child (`reshape_born_prevention`, applied identically on every replica), and
    // production wires a later replica of such an id through the same derivation at its factory
    // gate. This path has only the PROFILE's defaults to build from, so it takes the knobs off a
    // replica the group already has instead. Without it a fork child's late replica arrives with
    // pre-vote off, campaigns with real votes at a climbing term, and its check-quorum peers refuse
    // to adopt that term — a live replica that never rejoins its own group.
    let config = match self.live_prevention_knobs(gid) {
      Some((pre_vote, check_quorum)) => config
        .with_pre_vote(pre_vote)
        .with_check_quorum(check_quorum),
      None => config,
    };
    // Fresh stores in the world's configured mode (async for the merge profiles' fsync-loss
    // window). Built before the host borrow so it never straddles the `&self` `fresh_stores` read.
    let (log, stable) = self.fresh_stores(node, gid);
    // This replica's incarnation, bound ONCE here: every non-fork path wires the registry's live
    // generation, which is the generation this replica object will speak for until it is torn
    // down. A re-wire rebinds because it IS a new object. Read before the host borrow, like the
    // stores above.
    let bound = self.generation_of(gid);
    let host = self
      .hosts
      .get_mut(&node)
      .unwrap_or_else(|| panic!("wire_replica: node {node} was never added"));
    self.logs.insert((node, gid), log);
    self.stables.insert((node, gid), stable);
    self.configs.insert((node, gid), config.clone());
    self.member_view.insert((node, gid), is_member);
    self.replica_gen.insert((node, gid), bound);
    // A hosted replica is not tombstoned: admitting one is the re-admission the tombstone gates.
    self.host_tombstones.remove(&(node, gid));
    // Bump the replica incarnation on EVERY (re)wire: a member re-added after a teardown starts
    // a fresh endpoint at commit 0, and the group checker must reset that node's monotonicity
    // baseline rather than flag the legitimate drop.
    *self.restarts.entry((node, gid)).or_insert(0) += 1;
    // Per-NODE seed decorrelation: the container folds the GROUP id into the seed (co-located
    // groups on one host draw distinct jitter), but replicas of the SAME group on DIFFERENT nodes
    // need distinct base seeds too — identical streams under the shared global clock would draw
    // identical election timeouts and split votes forever (the single-group harness seeds each
    // Endpoint by node id for the same reason).
    host
      .create_group(gid, 0, config, self.now, self.seed ^ node, LogSm::new())
      .unwrap_or_else(|e| panic!("wire_replica: admission of group {gid} on node {node}: {e:?}"));
  }

  /// The id of the node currently believing itself leader of `gid`, if any — anchored on the
  /// HIGHEST term. A removed replica the farewell append never reached lingers in Leader role at
  /// its stale term (at etcd-parity defaults higher-term peers silently ignore its beats, so
  /// nothing ever deposes it), and a first-match scan in id order would let that zombie shadow
  /// the live quorum's leader for every consumer that targets "the" leader. Parked replicas are
  /// excluded — a reaped replica is no longer a protocol participant (the single-group
  /// `mark_removed` rule). Every replica of a gid shares the world's one generation for it
  /// (removal tears all replicas down before recreation), so the term alone orders leader
  /// claims; the lowest-id tie-break is determinism only (two same-term leaders cannot exist).
  pub fn leader_of(&self, gid: u64) -> Option<u64> {
    self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter_map(|&n| {
        self.hosts[&n]
          .group(&gid)
          .filter(|ep| ep.role().is_leader())
          .map(|ep| (ep.term(), n))
      })
      .max_by(|(term_a, id_a), (term_b, id_b)| term_a.cmp(term_b).then_with(|| id_b.cmp(id_a)))
      .map(|(_, n)| n)
  }

  /// Propose `cmd` on `gid`'s current leader; returns the assigned index (`None` when the group is
  /// momentarily leaderless or the leader refuses).
  pub fn propose(&mut self, gid: u64, cmd: &[u8]) -> Option<sailing_proto::Index> {
    let leader = self.leader_of(gid)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, gid)).expect("leader log");
    let stable = self.stables.get(&(leader, gid)).expect("leader stable");
    host
      .propose(
        &gid,
        self.now,
        log,
        stable,
        &bytes::Bytes::copy_from_slice(cmd),
      )?
      .ok()
  }

  /// Node `node`'s applied `(index, command-bytes)` sequence for `gid` (empty if the node does not
  /// host the group).
  pub fn applied_of(&self, node: u64, gid: u64) -> AppliedLog {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map(|ep| {
        ep.state_machine()
          .applied()
          .iter()
          .map(|(idx, cmd)| (idx.get(), cmd.to_vec()))
          .collect()
      })
      .unwrap_or_default()
  }

  /// True if every hosting node's ORACLE-ALIGNED applied sequence for `gid` agrees as a prefix
  /// of the longest — the State Machine Safety core, scoped to one group. Alignment (see
  /// `aligned_applied`) is what keeps the prefix NOTION valid across a
  /// split: raw records stop being prefix-related the moment one replica's `fsm.split` removes
  /// the moved cells mid-record while a lagging peer still holds them; a group that never split
  /// is compared byte-for-byte as before.
  pub fn agreement_holds(&self, gid: u64) -> bool {
    // The merged-lineage form: equal-applied replicas must hold identical RAW records (see
    // `ClusterView::positional_agreement` for why no positional filter survives an absorb).
    if self.group_absorbed(gid) {
      let replicas: Vec<(u64, AppliedLog)> = self
        .node_ids
        .iter()
        .filter(|n| self.hosts[n].contains_group(&gid))
        .map(|&n| {
          (
            self.hosts[&n]
              .group(&gid)
              .map_or(0, |ep| ep.applied_index().get()),
            self.applied_of(n, gid),
          )
        })
        .collect();
      let sorted = |log: &AppliedLog| {
        let mut v = log.clone();
        v.sort();
        v
      };
      for (i, a) in replicas.iter().enumerate() {
        for b in replicas.iter().skip(i + 1) {
          if a.0 == b.0 && sorted(&a.1) != sorted(&b.1) {
            return false;
          }
        }
      }
      return true;
    }
    let logs: Vec<AppliedLog> = self
      .node_ids
      .iter()
      .filter(|n| self.hosts[n].contains_group(&gid))
      // Each replica aligns under the incarnation it is BOUND to: its own population is what
      // decides which of its cells the comparison space keeps.
      .map(|&n| self.aligned_applied(n, gid, self.replica_gen_of(n, gid)))
      .collect();
    let longest = logs.iter().map(Vec::len).max().unwrap_or(0);
    for k in 0..longest {
      let mut seen: Option<&(u64, Vec<u8>)> = None;
      for l in &logs {
        if let Some(cell) = l.get(k) {
          match seen {
            None => seen = Some(cell),
            Some(s) => {
              if s != cell {
                return false;
              }
            }
          }
        }
      }
    }
    true
  }

  /// Tick until `pred(self)` holds or `max_ticks` elapse; returns whether it held.
  pub fn run_until(&mut self, max_ticks: u32, pred: impl Fn(&Self) -> bool) -> bool {
    for _ in 0..max_ticks {
      if pred(self) {
        return true;
      }
      self.tick();
    }
    pred(self)
  }

  /// Advance the simulation one step (the exact analogue of `Cluster::tick`): advance the global
  /// clock to the earliest pending deadline, fire every due `(host, group)` timer, then settle —
  /// drain outgoing onto the bus, deliver due messages, drain storage completions — until
  /// quiescent at this timestamp. Returns whether any work happened.
  pub fn tick(&mut self) -> bool {
    let mut progressed = false;

    // Step a+b: advance the clock and fire due timers. `poll_timeout` is each container's minimum
    // over its groups; the single global clock needs no per-node folding (drift is reserved).
    let next_timer = self.hosts.values().filter_map(|h| h.poll_timeout()).min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
        progressed = true;
      }
      for node in self.node_ids.clone() {
        let host = self.hosts.get_mut(&node).expect("host exists");
        // Collect first: firing a timer mutates the host, invalidating the `deadlines()` borrow.
        let due: Vec<u64> = host
          .deadlines()
          .filter(|(_, d)| *d <= self.now)
          .map(|(gid, _)| gid)
          .collect();
        for gid in due {
          progressed = true;
          let host = self.hosts.get_mut(&node).expect("host exists");
          let log = self.logs.get_mut(&(node, gid)).expect("replica log");
          let stable = self.stables.get_mut(&(node, gid)).expect("replica stable");
          host
            .handle_timeout(&gid, self.now, log, stable)
            .expect("due group is hosted");
        }
      }
    }

    // Flush each replica's coalesced replication batch ONCE before the settle loop (re-flushing
    // each pass would re-send to a still-Probe peer and the progress flag would never clear).
    for node in self.node_ids.clone() {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let gids: Vec<u64> = host.group_ids().copied().collect();
      for gid in gids {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let log = self.logs.get(&(node, gid)).expect("replica log");
        let stable = self.stables.get(&(node, gid)).expect("replica stable");
        host
          .flush_appends(&gid, self.now, log, stable)
          .expect("hosted group flushes");
      }
    }

    // Step c: drain outgoing → deliver due → drain storage → materialize committed forks, until
    // quiescent at this timestamp. The fork pump sits INSIDE the settle loop so a split applied
    // by a delivery in this very tick materializes its child (and the child's election timer
    // arms) before the tick's oracle pass runs — the driver's drain-every-crank cadence.
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(
        iters <= 10_000,
        "MultiWorld::tick inner loop exceeded 10_000 iterations — livelock?"
      );

      let any_new = self.drain_outgoing_all();
      let delivered = self.deliver_due();
      // A crash armed to land MID-SETTLE fires HERE — after this sub-step's deliveries submitted
      // fresh appends into the async stores but BEFORE the flush below makes them durable — so
      // `discard_inflight` rolls back a genuine, non-empty replication window (a crash at an
      // arbitrary instant, the "in-flight (pre-flush)" window; an ordinary `crash` between ticks
      // sees only the durable post-flush window). Taken so it fires exactly once, on the first
      // settle sub-step, where the just-delivered appends are still un-flushed.
      if let Some(victim) = self.pending_mid_crash.take() {
        self.crash(victim);
      }
      // Async stores only: make the in-flight (visible-but-unflushed) window durable BEFORE draining
      // completions — the fsync completing between driver sub-steps, the exact analogue of
      // `Cluster::tick`'s in-loop `flush_all` and the model the store docs specify ("flush() each
      // step, before draining completions"). In-loop (fine-grained), NOT once-per-tick: a
      // once-per-tick flush fires every deferred ack in one batch at the tick boundary and only then
      // processes this tick's higher-term truncations, so a follower's already-fired lower-term ack
      // escapes `scrub_acks_above` and a deposed leader sees a phantom durable quorum — a stale-ack
      // artifact of the coarse schedule, not a product fault. Fine-grained flushing keeps each ack's
      // window one sub-step wide, matching the proven single-group model. Gated on the WORLD mode so
      // a default (sync) world skips the phase entirely (byte-identical); a store manually set async
      // in a sync world (a targeted test) is deliberately left un-driven.
      if self.store_mode.is_async() {
        self.flush_async_stores();
      }
      let storage_produced = self.drain_storage_all();
      let forked = self.pump_forks();
      let merged = self.pump_merges();
      progressed |= any_new || delivered || storage_produced || forked || merged;

      if !any_new && !delivered && !storage_produced && !forked && !merged {
        break;
      }
    }

    self.tick_count += 1;
    // The world is quiescent at this timestamp — a consistent observable state. Run the whole
    // per-group oracle suite plus the cross-talk sweep; a violation panics with seed + tick.
    self.check_now();
    progressed
  }

  /// Run the per-group safety-oracle suites and the cross-group cross-talk sweep against the
  /// current state, panicking with the oracle name + seed + tick on a violation. Called at the
  /// end of every [`tick`](Self::tick); exposed so tests can also invoke it at a chosen point.
  pub fn check_now(&mut self) {
    let keys: Vec<(u64, u64)> = self.checkers.keys().copied().collect();
    for (gid, generation) in keys {
      let view = self.group_view(gid, generation);
      self
        .checkers
        .get_mut(&(gid, generation))
        .expect("checker exists")
        .check_or_panic(&view);
      // The checker folded this view's transitions/installs; clear so the next batch is fresh.
      // Clear THIS incarnation's observations only: another incarnation of the same id has its own
      // queue and its own checker still to run.
      self
        .pending_transitions
        .entry((gid, generation))
        .or_default()
        .clear();
      self
        .pending_new_installs
        .entry((gid, generation))
        .or_default()
        .clear();
      self.cross_talk_sweep(gid, generation);
      self.conserve_sweep(gid, generation);
      self.lineage_sweep(gid, generation);
    }
  }

  /// Feed the [`LineageLedger`](oracles::LineageLedger) this group's observable lineage state:
  /// every live replica's aligned committed record under its endpoint `fork_id` (content +
  /// phantom-quorum), and every in-flight snapshot transfer's liveness cursor (wedge). A parked
  /// replica is a reaped non-participant (the `leader_of`/quorum-denominator rule), so it is not a
  /// witness here either. Installs are fed separately at the `SnapshotInstalled` event (the chimera
  /// decision point). Pure observer — reads only public accessors and world bookkeeping.
  ///
  /// The CONTENT leg is scoped to `generation`, because the ledger keys both of its per-replica
  /// maps on the incarnation: committed content by `(gid, generation, lineage, index)` and the
  /// replica's own live lineage by `(node, gid, generation)`. The INSTALL leg already stamps the
  /// replica's BOUND generation (`replica_gen_of`, read at the `SnapshotInstalled` event), so
  /// reading the registry's current one here filed ONE replica's content and its installs under two
  /// different identities — and the chimera leg reads exactly that pairing, so a late fork's island
  /// would present to an install as a pristine adopter holding no committed lineage of its own.
  /// The WEDGE leg stays gid-scoped on purpose — a
  /// leader, its peer progress, and a transfer's reachability are container-level facts with no
  /// incarnation to filter on — and it re-presents the same cursor harmlessly when an id carries two
  /// live checkers, because progress is judged against the observation TICK, which does not move
  /// between the two calls.
  fn lineage_sweep(&mut self, gid: u64, generation: u64) {
    let (seed, tick) = (self.seed, self.tick_count);
    let nodes = self.node_ids.clone();
    // Content + phantom-quorum: only for a group whose FSM applied record is APPEND-ONLY, and
    // collect first (immutable reads) before feeding (mutates the ledger). A MERGE absorb re-bases
    // the target's record non-monotonically — the same index's committed bytes change as the union
    // folds in — so `(index → bytes)` is not a stable committed history there and the per-index
    // leg would read a legitimate re-base as a phantom; a merge-involved group's cross-replica
    // agreement is the positional/equal-applied agreement oracle's business
    // (`ClusterView::positional_agreement`). A SPLIT is phantom-safe: it only REMOVES a moved key's
    // cells from the aligned record and re-tags inherited cells under the CHILD's DISTINCT lineage
    // key. One-gid-one-lineage holds in the world, so the lineage key never partitions here — its
    // teeth are the two-lineage squatter scenario, which feeds the ledger directly.
    if self.record_is_append_only(gid, generation) {
      let mut content: Vec<(u64, Option<ForkId>, AppliedLog)> = Vec::new();
      for &node in &nodes {
        if self.parked.contains(&(node, gid)) {
          continue;
        }
        let Some(ep) = self.hosts[&node].group(&gid) else {
          continue;
        };
        if self.replica_gen_of(node, gid) != generation {
          continue;
        }
        let lineage = ep.fork_id();
        content.push((node, lineage, self.aligned_applied(node, gid, generation)));
      }
      for (node, lineage, applied) in &content {
        self.lineage.observe_content(
          seed,
          tick,
          (*node, gid, generation),
          lineage.as_ref(),
          applied,
        );
      }
    }
    // Wedge: the highest-term leader's in-flight snapshot transfers. Feed EVERY candidate peer each
    // sweep — `Some` for a reachable peer in the leader's `Snapshot` state, `None` otherwise — so a
    // leader change, an election gap, or a partition clears a stale cursor instead of accruing a
    // false wedge (a partition is not a wedge).
    let leader = self.leader_of(gid);
    // One wedge observation per peer: `(peer, in-flight (match, chunk cursor), refusal count)`.
    type TransferObs = (u64, Option<(u64, u64)>, u64);
    let mut xfers: Vec<TransferObs> = Vec::new();
    for &peer in &nodes {
      let in_flight = leader.and_then(|l| {
        if l == peer {
          return None;
        }
        let lep = self.hosts.get(&l)?.group(&gid)?;
        let pr = lep.peer_progress(&peer)?;
        match pr.state {
          ProgressState::Snapshot { acked_through, .. } => {
            // A parked replica is delivery-isolated for its group (the departed sweep's
            // patient-observation model), so a transfer toward it cannot progress and is not a
            // wedge — the same exemption as a partitioned or muted peer.
            let reachable = !self.isolated.contains(&l)
              && !self.isolated.contains(&peer)
              && !self.parked.contains(&(peer, gid))
              && !self.muted.contains(&(l, peer, gid))
              && !self.muted.contains(&(peer, l, gid));
            reachable.then_some((pr.match_index.get(), acked_through))
          }
          _ => None,
        }
      });
      let refused = self
        .hosts
        .get(&peer)
        .and_then(|h| h.group(&gid))
        .map_or(0, |e| e.refused_cross_lineage_install_count());
      xfers.push((peer, in_flight, refused));
    }
    for (peer, in_flight, refused) in xfers {
      self
        .lineage
        .observe_transfer(seed, tick, gid, peer, in_flight, refused);
    }
  }

  /// Whether the record of `gid`'s incarnation `generation` is APPEND-ONLY this instant — the
  /// precondition for the lineage ledger's per-index phantom-quorum leg (see `lineage_sweep`). A
  /// merge (freeze, park, absorb, or a merged-away husk) re-bases or folds records
  /// non-monotonically, so `(index → bytes)` stops being a stable committed history; a split is
  /// phantom-safe and is NOT excluded.
  ///
  /// EVERY LEG IS ASKED OF ONE INCARNATION, because the answer retires oracle coverage and the
  /// records of two coexisting incarnations of an id are unrelated. A union folded into the live
  /// successor re-bases the successor's record and nothing of the island's, so reading the id
  /// id-wide would silently switch off the island's phantom-quorum leg for the rest of the run —
  /// a true positive lost to someone else's merge. Where attribution is impossible the qualified
  /// forms stay conservative and keep excluding (see `merge_choreography_active_at`).
  fn record_is_append_only(&self, gid: u64, generation: u64) -> bool {
    !(self.group_absorbed_at(gid, generation)
      || self.meta_at(gid, generation).is_some_and(|m| m.merged)
      || self.any_replica_frozen_at(gid, generation)
      || self.merge_choreography_active_at(gid, generation)
      || self.group_merge_parked_at(gid, generation))
  }

  /// The run-end LINEAGE LEDGER verdict — chimera, phantom-quorum, and wedge — panicking with the
  /// oracle detail + seed for replay. Run beside the membership and conservation finalizers so
  /// every VOPR seed faces it. See `oracles::LineageLedger`.
  pub fn finalize_lineage_or_panic(&self, seed: u64) {
    self.lineage.finalize_or_panic(seed);
  }

  /// Applied cells the lineage ledger's phantom-quorum leg judged — its non-vacuity witness.
  pub fn lineage_cells_judged(&self) -> u64 {
    self.lineage.cells_judged()
  }

  /// Snapshot installs the lineage ledger's chimera leg examined — its non-vacuity witness.
  pub fn lineage_installs_observed(&self) -> u64 {
    self.lineage.installs_observed()
  }

  /// Render the membership oracle's run-end VERDICT for every checker this world ever built:
  /// the live per-group suites AND the frozen archives of retired incarnations. The per-tick
  /// [`check_now`](Self::check_now) only RECORDS snapshot-install observations — the verdict
  /// must wait until each group's committed-config history is FINAL (a later higher-term
  /// overwrite/ambiguation can supersede the reference a mid-run judgment would use) — and
  /// [`remove_group`](Self::remove_group) archives a checker after one more record-only check,
  /// so without this pass a corrupt install on a removed or recreated group would never face
  /// the verdict at all. Panics with the oracle name + seed for exact replay.
  ///
  /// A clean `Ok` from the finalizer is NOT the whole verdict: the pass can return `Ok` while
  /// RECORDING installs it could not judge. So each leg also enforces the single-group sweep's
  /// accounting policy — `skipped_unwitnessed_installs == 0` per checker (a nonzero count is a
  /// committed-config history completeness gap, and on a retired group the frozen history can
  /// NEVER catch up, so the silence would be permanent) — panicking with gid/generation
  /// attribution. Kind-unobservable declines are tolerated, exactly as the single-group policy
  /// tolerates them (see [`kind_unobservable_installs`](Self::kind_unobservable_installs)).
  pub fn finalize_membership_or_panic(&mut self, seed: u64) {
    let keys: Vec<(u64, u64)> = self.checkers.keys().copied().collect();
    for (gid, generation) in keys {
      let ck = self
        .checkers
        .get_mut(&(gid, generation))
        .expect("checker exists");
      if let Err(v) = checker::finalize_membership(ck) {
        panic!(
          "SAFETY ORACLE VIOLATION (run-end final pass): {v}\n  group={gid} gen={generation} \
           seed={seed}\n  (replay: run_multi_vopr for this seed and inspect the snapshot install \
           at the reported boundary)",
        );
      }
      Self::assert_installs_accounted(gid, generation, false, ck, seed);
    }
    for (&(gid, generation), ck) in self.retired.iter_mut() {
      if let Err(v) = checker::finalize_membership(ck) {
        panic!(
          "SAFETY ORACLE VIOLATION (run-end final pass, retired group): {v}\n  group={gid} \
           gen={generation} seed={seed}\n  (replay: run_multi_vopr for this seed and inspect \
           the snapshot install at the reported boundary)",
        );
      }
      Self::assert_installs_accounted(gid, generation, true, ck, seed);
    }
  }

  /// The finalize pass's ACCOUNTING leg: an `Ok` verdict with a nonzero skipped counter means an
  /// observed install never faced the membership verdict at all. The single-group sweep asserts
  /// that counter is `0` across its whole band; the multi run enforces the same zero-tolerance
  /// per checker, where the gid/generation attribution a band total cannot carry is still known.
  ///
  /// `kind_unobservable_installs` is deliberately NOT enforced, matching the single-group
  /// policy: some installs resolve to a conf-change whose committed-log entry was compacted
  /// before any tick observed it, so the oracle has no EXACT-term ConfChange proof and SOUNDLY
  /// DECLINES (never trust a possibly-stale ConfChange) rather than risk a false verdict — a
  /// bounded coverage limitation of compaction, NOT a soundness hole. The aggregate is surfaced
  /// through [`kind_unobservable_installs`](Self::kind_unobservable_installs) for sweep-level
  /// coverage bounds.
  fn assert_installs_accounted(gid: u64, generation: u64, retired: bool, ck: &Checker, seed: u64) {
    let skipped = ck.skipped_unwitnessed_installs();
    if skipped == 0 {
      return;
    }
    let leg = if retired { ", retired group" } else { "" };
    panic!(
      "MEMBERSHIP ACCOUNTING FAILURE (run-end final pass{leg}): {skipped} observed snapshot \
       install(s) never faced a membership verdict — a committed-config HISTORY completeness \
       gap (a boundary beyond the watermark or an unresolved divergence that did not converge); \
       the history must cover every committed index an install lands on\n  group={gid} \
       gen={generation} seed={seed}\n  (replay: run_multi_vopr for this seed and inspect the \
       group's observed installs)",
    );
  }

  /// Membership-coherence comparisons the run-end final pass performed, summed over every
  /// checker this world ever built (live groups + the retired archive); `0` until
  /// [`finalize_membership_or_panic`](Self::finalize_membership_or_panic) runs. A sweep reads
  /// this to prove the membership oracle genuinely judged installs rather than skipping them.
  pub fn membership_oracle_comparisons(&self) -> u64 {
    self
      .checkers
      .values()
      .chain(self.retired.values())
      .map(Checker::membership_comparisons)
      .sum()
  }

  /// Observed installs the run-end final pass could NOT judge due to an incomplete
  /// committed-config HISTORY, summed over live + retired checkers.
  /// [`finalize_membership_or_panic`](Self::finalize_membership_or_panic) enforces `0` per
  /// checker (the single-group sweep's policy), so a completed run always reports `0` —
  /// surfaced so sweeps can pin exactly that.
  pub fn skipped_unwitnessed_installs(&self) -> u64 {
    self
      .checkers
      .values()
      .chain(self.retired.values())
      .map(Checker::skipped_unwitnessed_installs)
      .sum()
  }

  /// Observed installs the run-end final pass SOUNDLY declined because the resolved conf-change
  /// index is committed-final but its committed-log KIND was compacted before any tick observed
  /// it, summed over live + retired checkers. Tolerated (never enforced), matching the
  /// single-group policy: the net declines rather than risk a stale verdict — a bounded
  /// coverage limitation of compaction, not a soundness hole.
  pub fn kind_unobservable_installs(&self) -> u64 {
    self
      .checkers
      .values()
      .chain(self.retired.values())
      .map(Checker::kind_unobservable_installs)
      .sum()
  }

  /// Assert every NEWLY applied entry on every replica of `gid` decodes (when gid-tagged) to
  /// `gid` itself — the O(1)-per-apply cross-group isolation oracle.
  fn cross_talk_sweep(&mut self, gid: u64, generation: u64) {
    // The floor derives from the GROUP record, never the replica's wiring path: a fork-born
    // group's inherited baseline cells carry an ANCESTOR's tag legitimately (the handover), and
    // every arrival path — fork materialization, a transferred snapshot into a fresh observer,
    // a crash restore from the durable blob — presents them as the record's leading prefix. An
    // onward split a replica applied can only SHRINK that prefix below the recorded count
    // (`LogSm::split` removes moved-key cells record-wide), so flooring at the full count never
    // judges an inherited cell; the few own-tagged cells the floor may skip on a shrunk record
    // would pass the tag assert anyway — under-coverage there, never a false positive.
    // ...and from THIS INCARNATION's record: an island's inherited prefix and tag lineage are its
    // own, and the successor's (empty) ones would read every inherited cell as a leak.
    let baseline = self.meta_at(gid, generation).map_or(0, |m| m.fork_baseline);
    let carried = self
      .meta_at(gid, generation)
      .map(|m| m.carried_tags.clone())
      .unwrap_or_default();
    for node in self.node_ids.clone() {
      if !self.hosts[&node].contains_group(&gid) {
        continue;
      }
      if self.replica_gen_of(node, gid) != generation {
        continue;
      }
      let applied = self.applied_of(node, gid);
      let hw = self.swept.entry((node, gid)).or_insert(0);
      // A crash-restore can legitimately SHRINK the applied prefix (apply outruns the batched
      // commit persist); clamp, and re-sweeping a replayed suffix is harmless (same entries).
      let start = (*hw).max(baseline).min(applied.len());
      let checked = oracles::assert_no_cross_talk(
        self.seed,
        self.tick_count,
        node,
        gid,
        &carried,
        &applied[start..],
      );
      *hw = applied.len();
      self.cross_talk_checked += checked;
    }
  }

  /// The incarnation `node`'s `gid` replica speaks for — its wire stamp and its judging identity.
  /// Falls back to the registry for a replica whose binding predates its stores (nothing in the
  /// world sends for an unwired replica, so the fallback is a total-function convenience).
  pub(crate) fn replica_gen_of(&self, node: u64, gid: u64) -> u64 {
    self
      .replica_gen
      .get(&(node, gid))
      .copied()
      .unwrap_or_else(|| self.generation_of(gid))
  }

  /// The expectation meta for ONE incarnation of `gid`: the live registry entry when `generation`
  /// is the current one, else the archived entry the recreation that superseded it left behind.
  pub(crate) fn meta_at(&self, gid: u64, generation: u64) -> Option<&lifecycle::GroupMeta> {
    match self.groups.get(&gid) {
      Some(meta) if meta.generation == generation => Some(meta),
      _ => self.meta_archive.get(&(gid, generation)),
    }
  }

  /// `node`'s admission floor for `gid` on the INCARNATION scale the stamp uses: one past the
  /// generation each retirement ended, plus the terminal `MERGED_FLOOR` for a source this host
  /// resolved away (a merged id is never re-admitted, so its fence is terminal by construction).
  ///
  /// Deliberately NOT [`NodeStores::floor`]: that store answers on the CONTAINER's lineage scale
  /// (a reshaped id's removal ceiling), while the world re-admits every recreation at container
  /// generation 0 — the two scales do not order against each other, and comparing them would
  /// fence a live incarnation's own traffic. The diligent-embedder HUSK feed is excluded for a
  /// second reason: it is a pre-teardown hint about a source this host still runs, and the
  /// product's demux reads only the engine's persisted record.
  fn admission_floor(&self, node: u64, gid: u64) -> u64 {
    if self.merge_floors.contains(&(node, gid)) {
      return sailing_proto::MERGED_FLOOR;
    }
    self.incarnation_floors.get(&gid).copied().unwrap_or(0)
  }

  /// Assemble the per-group [`ClusterView`](crate::ClusterView) from `gid`'s hosting nodes —
  /// field-for-field the shape `Cluster::view` builds, scoped to one group's replicas and their
  /// `(node, gid)` stores, so the UNCHANGED oracle suite judges each group independently.
  fn group_view(&self, gid: u64, generation: u64) -> checker::ClusterView {
    let mut nodes = Vec::new();
    for &node in &self.node_ids {
      let Some(ep) = self.hosts[&node].group(&gid) else {
        continue;
      };
      // PARTITION BY INCARNATION: a replica bound to a different incarnation of this id is a
      // different group as far as every safety oracle is concerned — its terms restart, its log
      // index space restarts, and its record descends from a different history. Judging the two
      // together manufactures divergence out of two individually-correct replicas.
      if self.replica_gen_of(node, gid) != generation {
        continue;
      }
      let log = &self.logs[&(node, gid)];
      let stable = &self.stables[&(node, gid)];
      let durable_first = log.durable_first_index().get();
      let durable_last = log.durable_last_index().get();
      let visible_last = log.last_index().get();
      let durable_entries: Vec<DurableEntry> = log
        .durable_entries()
        .iter()
        .map(|e| DurableEntry {
          index: e.index().get(),
          term: e.term().get(),
          data: e.data().to_vec(),
          is_conf_change: e.kind().is_conf_change(),
        })
        .collect();
      let (snapshot_last_index, snapshot_last_term) = match stable.durable_snapshot() {
        Some(meta) => (meta.last_index().get(), meta.last_term().get()),
        None => (0, 0),
      };
      // The checker's applied-record legs (positional agreement, the index-keyed rewrite
      // high-water) get the ORACLE-ALIGNED record — see `aligned_applied` for why the raw
      // record stops fitting both notions once the group splits. A group that ABSORBED via a
      // merge instead ships the RAW record under the equal-applied agreement form (see
      // `ClusterView::positional_agreement`): an absorb re-introduces own-tagged cells at
      // replica-local resolution states, so no positional filter stays lag-invariant.
      let applied_log = if self.group_absorbed_at(gid, generation) {
        self.applied_of(node, gid)
      } else {
        self.aligned_applied(node, gid, generation)
      };
      let cs = ep.conf_state();
      nodes.push(checker::NodeView {
        id: node,
        removed: self.parked.contains(&(node, gid)),
        is_voter: cs.is_voter(&node),
        poisoned: ep.is_poisoned(),
        is_leader: ep.role().is_leader(),
        term: ep.term().get(),
        commit: ep.commit_index().get(),
        applied: ep.applied_index().get(),
        applied_log,
        durable_first,
        durable_last,
        visible_last,
        durable_entries,
        snapshot_last_index,
        snapshot_last_term,
        installed_snapshot: self.snapshot_lineage.contains(&(node, gid)),
        conf_voters: cs.voters().clone(),
        conf_voters_outgoing: cs.voters_outgoing().clone(),
        conf_learners: cs.learners().clone(),
        conf_learners_next: cs.learners_next().clone(),
        conf_auto_leave: cs.auto_leave(),
        conf_changed: self.conf_changed.get(&(node, gid)).copied().unwrap_or(0),
        hardstate_commit: stable.hard_state().commit().get(),
        inflight_staged: usize::from(log.has_inflight()) + usize::from(stable.has_inflight()),
        incarnation: self.restarts.get(&(node, gid)).copied().unwrap_or(0),
      });
    }
    checker::ClusterView {
      positional_agreement: !self.group_absorbed_at(gid, generation),
      seed: self.seed,
      tick: self.tick_count,
      committed_voters: {
        let v = self.committed_voters_of(gid, generation);
        if v.is_empty() { None } else { Some(v) }
      },
      committed_transitions: self
        .pending_transitions
        .get(&(gid, generation))
        .cloned()
        .unwrap_or_default(),
      new_installs: self
        .pending_new_installs
        .get(&(gid, generation))
        .cloned()
        .unwrap_or_default(),
      nodes,
    }
  }

  /// The group's REAL committed VOTER set, read exactly as `Cluster::committed_voters` reads it:
  /// the HIGHEST-TERM leader among the group's hosting replicas is authoritative; leaderless,
  /// the most common committed voter set across hosting replicas (ties to the first-sorting
  /// set), so the result is a pure function of world state. Parked replicas are excluded from
  /// BOTH paths — the [`leader_of`](Self::leader_of) rule: a reaped stale leader still wearing
  /// Leader role would otherwise become the authoritative config source the moment the group is
  /// between live leaders (and a parked stale config would keep voting in the leaderless tally),
  /// handing the quorum-durability oracle a denominator anchored on a zombie's view.
  ///
  /// SCOPED TO ONE INCARNATION for the same reason the view is: a replica bound to another
  /// incarnation of this id descends from a different config history, and its voters are not this
  /// group's. Unfiltered, a late fork's island reads the live successor's voter set, intersects it
  /// with its own single hosting replica to nothing, and `commit_is_quorum_durable` then has no
  /// denominator to judge against — the island goes silently unchecked.
  fn committed_voters_of(&self, gid: u64, generation: u64) -> BTreeSet<u64> {
    let authoritative = self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter(|&&n| self.replica_gen_of(n, gid) == generation)
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .filter(|ep| ep.role().is_leader())
      .max_by_key(|ep| ep.term());
    if let Some(ep) = authoritative {
      return ep.conf_state().voters().iter().copied().collect();
    }
    let mut tally: BTreeMap<BTreeSet<u64>, usize> = BTreeMap::new();
    for &n in &self.node_ids {
      if self.parked.contains(&(n, gid)) || self.replica_gen_of(n, gid) != generation {
        continue;
      }
      let Some(ep) = self.hosts[&n].group(&gid) else {
        continue;
      };
      let voters: BTreeSet<u64> = ep.conf_state().voters().iter().copied().collect();
      *tally.entry(voters).or_insert(0) += 1;
    }
    tally
      .into_iter()
      .max_by(|(a_set, a_n), (b_set, b_n)| a_n.cmp(b_n).then_with(|| b_set.cmp(a_set)))
      .map(|(set, _)| set)
      .unwrap_or_default()
  }

  /// Drain every host's outgoing `(gid, message)` queue onto the bus (isolated hosts drain to the
  /// void) and its event queue. Returns whether any message reached the bus.
  fn drain_outgoing_all(&mut self) -> bool {
    let mut any_new = false;
    for node in self.node_ids.clone() {
      if self.isolated.contains(&node) {
        let host = self.hosts.get_mut(&node).expect("host exists");
        while host.poll_message().is_some() {}
      } else {
        // Re-borrow the host each iteration: `schedule_send` needs `&mut self`, so the poll
        // borrow cannot be held across it.
        while let Some((gid, out)) = self
          .hosts
          .get_mut(&node)
          .expect("host exists")
          .poll_message()
        {
          any_new = true;
          let (to, message) = Outgoing::into_parts(out);
          self.schedule_send(node, gid, to, message);
        }
      }
      self.drain_host_events(node);
    }
    any_new
  }

  /// THE single event-drain for node `node`'s container: every drain site routes here so no
  /// tracked event is cherry-picked or dropped on any path (the single-group harness's rule).
  ///   - `SnapshotInstalled` → the sticky per-`(node, gid)` snapshot-membership lineage AND the
  ///     group's new-install feed for the membership oracle.
  ///   - `ConfChanged` → the per-`(node, gid)` counter AND (from a LOG-BUILT replica only) the
  ///     group's committed-config transition at its exact index, tagged with the conf-change
  ///     ENTRY's term (a non-faulting log lookup — not the replica's current term).
  ///   - `ConfChanged` whose resulting config no longer lists the replica ITSELF → the replica
  ///     applied its own removal (the farewell append landed): the embedder-on-RemovedSelf
  ///     response PARKS it after the drain. Parked, not destroyed: the ex-member's durable log
  ///     is still a real witness for entries it acked, and the other members may lag applying
  ///     the removal — destroying the view here would under-count quorum durability exactly as
  ///     a stale-leader misjudgement would.
  fn drain_host_events(&mut self, node: u64) {
    let mut self_removed: Vec<u64> = Vec::new();
    loop {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let Some((gid, ev)) = host.poll_event() else {
        break;
      };
      match ev {
        Event::SnapshotInstalled(meta) => {
          self.snapshot_lineage.insert((node, gid));
          // The install adopts the snapshot's ConfState verbatim — refresh the membership view
          // WITHOUT a teardown (no explicit removal event rides an install; a genuinely
          // departed replica is the reconcile sweep's to reap).
          let cs = meta.conf();
          let is_member = cs.voters().contains(&node)
            || cs.voters_outgoing().contains(&node)
            || cs.learners().contains(&node)
            || cs.learners_next().contains(&node);
          self.member_view.insert((node, gid), is_member);
          // The lineage ledger's install observation — the chimera decision point: a fork-lineage
          // snapshot installing over a replica that already holds committed content of ANOTHER
          // lineage is the cross-lineage fusion the door gate refuses. A pristine adopter or a
          // same-token retransfer is legitimate and does not trip.
          let (seed, tick) = (self.seed, self.tick_count);
          // The INSTALLING replica's own incarnation — the identity its ledger entries key on.
          let generation = self.replica_gen_of(node, gid);
          self.lineage.observe_install(
            seed,
            tick,
            (node, gid, generation),
            meta.fork_id(),
            meta.last_index().get(),
          );
          self
            .pending_new_installs
            .entry((gid, generation))
            .or_default()
            .push((
              node,
              meta.last_index().get(),
              checker::ConfSnapshot::from_conf_state(meta.conf()),
            ));
        }
        Event::ConfChanged(cc) => {
          *self.conf_changed.entry((node, gid)).or_insert(0) += 1;
          {
            let cs = cc.conf();
            let is_member = cs.voters().contains(&node)
              || cs.voters_outgoing().contains(&node)
              || cs.learners().contains(&node)
              || cs.learners_next().contains(&node);
            let was_member = self
              .member_view
              .insert((node, gid), is_member)
              .unwrap_or(false);
            // RemovedSelf = the member → non-member TRANSITION. A catching-up joiner applying
            // historical pre-join confs (self absent throughout) is NOT a removal.
            if was_member && !is_member {
              self_removed.push(gid);
            }
          }
          if !self.snapshot_lineage.contains(&(node, gid)) {
            let idx = cc.index();
            let entry_term = {
              let commit = self.hosts[&node]
                .group(&gid)
                .expect("event source is hosted")
                .commit_index();
              self.logs[&(node, gid)]
                .committed_entries_no_fault(commit)
                .iter()
                .find(|e| e.index() == idx)
                .map(|e| e.term().get())
                .unwrap_or(0)
            };
            // The OBSERVING replica's incarnation, read at event time — the identity whose
            // checker is owed this observation.
            let observed_gen = self.replica_gen_of(node, gid);
            self
              .pending_transitions
              .entry((gid, observed_gen))
              .or_default()
              .push((
                idx.get(),
                entry_term,
                checker::ConfSnapshot::from_conf_state(cc.conf()),
              ));
          }
        }
        Event::ReadState(rs) => {
          self.read_states.entry((node, gid)).or_default().push(rs);
        }
        // Registration and per-node wiring ride the FORK PUMP (the committed fork carries the
        // voters/blob/index this per-replica notification does not), so the apply-point event
        // needs no world-side action here.
        Event::SplitApplied(_) => {}
        Event::SplitStale(_) => {
          self.split_stale += 1;
        }
        Event::MergeAborted(ma) => {
          // The abort clock (see `merge_aborts_observed`): the fuzzer book retires a booked
          // pair on OBSERVED resolution, and the abort side's observation is exactly this
          // apply-point event on a target replica.
          if let Ok(source) = <u64 as sailing_proto::Data>::decode_exact(ma.source()) {
            *self.merge_aborts_observed.entry((gid, source)).or_insert(0) += 1;
          }
        }
        _ => {}
      }
    }
    // Embedder-on-RemovedSelf parking, after the drain so it never truncates the event pass.
    // (A self-removed endpoint has stepped down and disarmed its election timer, so the parked
    // replica is quiet; a later committed re-add unparks it with its retained state.)
    for gid in self_removed {
      if self.hosts[&node].contains_group(&gid) {
        self.parked.insert((node, gid));
      }
    }
  }

  /// Run the structural send-point oracles on a message `from` is sending for `gid`, then push
  /// it onto the bus (fault-free for now: zero latency, FIFO, exactly once).
  ///
  /// The tripwires run on every SENT message, BEFORE any future drop/duplicate roll, so a
  /// dropped message can never bypass an oracle (the single-group ordering rule):
  ///   (a) append-before-ack — a success `AppendResponse` must not outrun the replica's
  ///       readable `(node, gid)` log;
  ///   (b) one-identity — a REAL-vote grant binds `(granter, gid, gen, term)` to one candidate
  ///       across every replica object this node ever hosts for the group.
  fn schedule_send(&mut self, from: u64, gid: u64, to: u64, message: Message<u64>) {
    // The wire's incarnation stamp: the SENDING REPLICA's own bound incarnation, exactly as
    // production stamps a host's own committed generation. Reading the registry here instead would
    // make two coexisting incarnations of one id stamp IDENTICALLY — the shared-meta infidelity
    // that hides an island behind its successor's generation. Registry-scale still (the same scale
    // the one-identity grant key uses and the one `remove_group` retires); the container's own
    // lineage counter is NOT it, since the world re-admits every recreation at container
    // generation 0, so that scale cannot order incarnations across a retirement.
    let generation = self.replica_gen_of(from, gid);
    if let Message::AppendResponse(a) = &message
      && !a.reject()
    {
      let log = &self.logs[&(from, gid)];
      assert!(
        log.last_index() >= a.match_index(),
        "append-before-ack violated: node {from} group {gid} acked {:?} but last_index is {:?} \
         (durable_last={:?} inflight={})\n  seed={} tick={}",
        a.match_index(),
        log.last_index(),
        log.durable_last_index(),
        log.has_inflight(),
        self.seed,
        self.tick_count,
      );
    }
    if let Message::VoteResponse(vr) = &message
      && !vr.reject()
      && !vr.pre_vote()
    {
      // The GRANTER's own incarnation: two incarnations restart terms independently, so a
      // registry-wide key would fuse two legitimate grants at the same term into a double vote.
      let generation = self.replica_gen_of(from, gid);
      oracles::note_grant(
        &mut self.grants,
        self.seed,
        self.tick_count,
        (from, gid, generation, vr.term()),
        to,
      );
    }

    // Fast path: faults off ⇒ zero-latency, FIFO, exactly-once (byte-identical to the original
    // bus; the PRNG is never touched).
    if self.net_faults.is_none() {
      self.bus.push_back(GInFlight {
        deliver_at: self.now,
        gid,
        from,
        to,
        generation,
        message,
      });
      return;
    }
    if self
      .net_prng
      .chance_per_mille(self.net_faults.drop_per_mille)
    {
      self.net_dropped += 1;
      return; // lost in flight
    }
    let copies = if self
      .net_prng
      .chance_per_mille(self.net_faults.duplicate_per_mille)
    {
      self.net_duplicated += 1;
      2
    } else {
      1
    };
    for _ in 0..copies {
      // Each copy draws its own jitter (a dup may overtake its twin).
      let jitter = self.net_prng.jitter_draw(self.net_faults.jitter);
      let mut deliver_at = self.now + self.net_faults.latency + jitter;
      // FIFO clamp per ORDERED NODE PAIR when reorder is off: one physical link carries every
      // group's traffic, so the clamp spans groups exactly as the wire does.
      if !self.net_faults.reorder {
        let last = self
          .net_last_sched
          .entry((from, to))
          .or_insert(Instant::ORIGIN);
        if deliver_at < *last {
          deliver_at = *last;
        }
        *last = deliver_at;
      }
      self.bus.push_back(GInFlight {
        deliver_at,
        gid,
        from,
        to,
        generation,
        message: message.clone(),
      });
    }
  }

  /// Deliver every bus message due at or before `now`. A message to a node that does not host its
  /// group is dropped SILENTLY (the unhosted-drop semantics of the group-tagged wire); a message
  /// with either endpoint isolated is dropped by the partition. Returns whether any delivered.
  fn deliver_due(&mut self) -> bool {
    let mut delivered = false;
    let mut rest: VecDeque<GInFlight> = VecDeque::new();
    while let Some(m) = self.bus.pop_front() {
      if m.deliver_at > self.now {
        rest.push_back(m);
        continue;
      }
      if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
        continue; // partition swallows it
      }
      if self.muted.contains(&(m.from, m.to, m.gid)) {
        continue; // the (link, group) mute swallows it — other groups on the link still flow
      }
      if self.parked.contains(&(m.from, m.gid)) || self.parked.contains(&(m.to, m.gid)) {
        continue; // a parked replica is delivery-isolated for its group (state retained)
      }
      // THE GENERATION FENCE, modelled at the delivery seam exactly as the product models it at
      // demux: a frame whose sender stamp is below the RECEIVER's durable admission floor speaks
      // for a retired incarnation and is dropped, counted, never delivered. Equal admits. Ordered
      // with the product's ordering — after the transport-level drops, before store resolution.
      //
      // WHAT THIS DOES AND DOES NOT ISOLATE. Once the stamp is the sending replica's own bound
      // incarnation, a surviving OLDER incarnation of an id (a stale fork landed on a host the
      // successor never reached) is receive-live but send-fenced: inbound frames from the live
      // incarnation clear the floor and are delivered, while everything it sends back is below the
      // floor and dropped. It therefore cannot commit, cannot be elected, and cannot move — a
      // STATIC island, which is why its content stays judgeable rather than racing.
      //
      // That isolation is REAL here and an INFIDELITY at the same time: it rests on
      // `incarnation_floors` being world-wide, whereas production's floor is per-host and is never
      // written at all for a never-reshaped gen-0 id. Production has no such fence for that shape,
      // so the island must be JUDGED on its own incarnation's terms rather than assumed inert —
      // see the incarnation-keyed checkers.
      let admits = sailing_proto::floor_admits(self.admission_floor(m.to, m.gid), m.generation);
      let Some(host) = self.hosts.get_mut(&m.to) else {
        continue; // unknown node id — drop safely
      };
      if !host.contains_group(&m.gid) {
        continue; // unhosted group — silent drop, the connection-level tombstone/demux semantics
      }
      if !admits {
        self.fenced_dropped += 1;
        if matches!(m.message, Message::RequestVote(_)) {
          self.fenced_votes_dropped += 1;
        }
        continue;
      }
      delivered = true;
      let log = self.logs.get_mut(&(m.to, m.gid)).expect("replica log");
      let stable = self
        .stables
        .get_mut(&(m.to, m.gid))
        .expect("replica stable");
      host
        .handle_message(&m.gid, self.now, log, stable, m.from, m.message)
        .expect("hosted group handles");
    }
    self.bus = rest;
    delivered
  }

  /// Drain storage completions for every `(host, group)` and collect any messages they produce
  /// (deferred acks once a staged write flushes). Returns whether new work surfaced.
  fn drain_storage_all(&mut self) -> bool {
    let mut any_new = false;
    for node in self.node_ids.clone() {
      let host = self.hosts.get_mut(&node).expect("host exists");
      let gids: Vec<u64> = host.group_ids().copied().collect();
      for gid in gids {
        let host = self.hosts.get_mut(&node).expect("host exists");
        let log = self.logs.get_mut(&(node, gid)).expect("replica log");
        let stable = self.stables.get_mut(&(node, gid)).expect("replica stable");
        // A budget-bounded drain may leave completions queued (`MorePending`); count that as
        // progress so the settle loop keeps draining until every replica reports `Drained`.
        any_new |= host
          .handle_storage(&gid, self.now, log, stable)
          .expect("hosted group drains")
          .is_more_pending();
      }
    }
    // Collect outgoing produced by completion handlers — same path as the tick outgoing-drain.
    any_new |= self.drain_outgoing_all();
    any_new
  }

  /// Flush every hosted store's staged in-flight window to durable state, tallying the flush-phase
  /// non-vacuity witnesses (flush counts, and torn writes that stranded a REAL batch, summed into
  /// world-running totals so a store purged mid-run does not lose its tally). Only ever called when
  /// the world's [`StoreMode`](crate::StoreMode) is async, so every hosted store is async.
  ///
  /// A store is flushed only when it holds an in-flight window: an empty flush would clone the whole
  /// log for nothing (a big constant on a long log) and roll the torn PRNG on a batch it cannot tear.
  fn flush_async_stores(&mut self) {
    for node in self.node_ids.clone() {
      let gids: Vec<u64> = self.hosts[&node].group_ids().copied().collect();
      for gid in gids {
        if let Some(log) = self.logs.get_mut(&(node, gid))
          && log.has_inflight()
        {
          let torn_before = log.torn_writes();
          log.flush();
          self.log_flushes += 1;
          self.torn_writes_fired += log.torn_writes() - torn_before;
        }
        if let Some(stable) = self.stables.get_mut(&(node, gid))
          && stable.has_inflight()
        {
          let torn_before = stable.torn_writes();
          stable.flush();
          self.stable_flushes += 1;
          self.torn_writes_fired += stable.torn_writes() - torn_before;
        }
      }
    }
  }

  /// Arm node `node` to crash MID-SETTLE on the next [`tick`](Self::tick) instead of now: the tick's
  /// settle loop crashes it after a delivery sub-step submitted fresh appends but before the store
  /// flush, so the crash rolls back a genuine, non-empty replication fsync window (the "in-flight
  /// (pre-flush)" crash the brief asks for, complementing the durable-window [`crash`](Self::crash)).
  /// Only meaningful under an async store mode — a sync store never holds an in-flight window.
  pub(crate) fn arm_mid_fsync_crash(&mut self, node: u64) {
    self.pending_mid_crash = Some(node);
  }

  /// Whether the world wires async stores (the merge profiles) — the fuzzer gates the mid-fsync
  /// crash draw on this so the sync profiles neither draw the extra PRNG nor change behavior.
  pub(crate) fn is_async_stores(&self) -> bool {
    self.store_mode.is_async()
  }
}

#[cfg(test)]
pub(crate) mod tests;

mod faults;
mod lifecycle;
mod merge;
mod query;
mod split;
