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

/// An in-flight group-tagged typed message: `(deliver_at, gid, from, to, message)`.
struct GInFlight {
  deliver_at: Instant,
  gid: u64,
  from: u64,
  to: u64,
  message: Message<u64>,
}

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
  /// One safety-oracle suite PER GROUP — unchanged oracle code, parameterized by the per-group
  /// [`ClusterView`](crate::ClusterView) assembled from the group's hosting nodes.
  checkers: BTreeMap<u64, Checker>,
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
  /// membership oracle exactly as `Cluster::pending_transitions` is, then cleared).
  pending_transitions: BTreeMap<u64, Vec<(u64, u64, checker::ConfSnapshot)>>,
  /// Per-group new transfer-snapshot installs observed since the last check (then cleared).
  pending_new_installs: BTreeMap<u64, Vec<(u64, u64, checker::ConfSnapshot)>>,
  /// The harness-side group registry: one [`lifecycle::GroupMeta`] per logical group id, across
  /// incarnations (retirement flips `retired`; recreation bumps `generation`).
  groups: BTreeMap<u64, lifecycle::GroupMeta>,
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
  /// `engine.group_gen` (bumped in its fork drain) via `raise_relay_guard` — the restore path feeds
  /// this back so a replayed split folds to a duplicate no-op instead of re-materializing (or, now,
  /// PARKING against) an already-relayed child.
  relayed_lineage: BTreeMap<(u64, u64), u64>,
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
      retired: BTreeMap::new(),
      muted: BTreeSet::new(),
      net_faults: NetworkFaults::none(),
      // Same stream derivation as the single-group bus ("NET"), distinct from replica seeds.
      net_prng: NetPrng::new(seed.rotate_left(16) ^ 0x004E_4554),
      net_last_sched: BTreeMap::new(),
      net_dropped: 0,
      net_duplicated: 0,
      configs: BTreeMap::new(),
      restarts: BTreeMap::new(),
      relayed_lineage: BTreeMap::new(),
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
      fork_conflicts: BTreeMap::new(),
      split_refused: 0,
      merges: Vec::new(),
      merge_floors: BTreeSet::new(),
      removal_floors: BTreeMap::new(),
      pending_merge_teardowns: Vec::new(),
      active_freezes: BTreeMap::new(),
      merges_resolved: 0,
      merges_aborted: 0,
      merges_capture_failed: 0,
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
    if self.store_mode.is_async() {
      (
        MemLog::new_async(self.seed ^ node ^ gid.rotate_left(32)),
        MemStable::new_async(self.seed.rotate_left(32) ^ node ^ gid.rotate_left(32)),
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
      self.checkers.insert(gid, Checker::new()).is_none(),
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
    // Fresh stores in the world's configured mode (async for the merge profiles' fsync-loss
    // window). Built before the host borrow so it never straddles the `&self` `fresh_stores` read.
    let (log, stable) = self.fresh_stores(node, gid);
    let host = self
      .hosts
      .get_mut(&node)
      .unwrap_or_else(|| panic!("wire_replica: node {node} was never added"));
    self.logs.insert((node, gid), log);
    self.stables.insert((node, gid), stable);
    self.configs.insert((node, gid), config.clone());
    self.member_view.insert((node, gid), is_member);
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
      .map(|&n| self.aligned_applied(n, gid))
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
    let gids: Vec<u64> = self.checkers.keys().copied().collect();
    for gid in gids {
      let view = self.group_view(gid);
      self
        .checkers
        .get_mut(&gid)
        .expect("checker exists")
        .check_or_panic(&view);
      // The checker folded this view's transitions/installs; clear so the next batch is fresh.
      self.pending_transitions.entry(gid).or_default().clear();
      self.pending_new_installs.entry(gid).or_default().clear();
      self.cross_talk_sweep(gid);
      self.conserve_sweep(gid);
      self.lineage_sweep(gid);
    }
  }

  /// Feed the [`LineageLedger`](oracles::LineageLedger) this group's observable lineage state:
  /// every live replica's aligned committed record under its endpoint `fork_id` (content +
  /// phantom-quorum), and every in-flight snapshot transfer's liveness cursor (wedge). A parked
  /// replica is a reaped non-participant (the `leader_of`/quorum-denominator rule), so it is not a
  /// witness here either. Installs are fed separately at the `SnapshotInstalled` event (the chimera
  /// decision point). Pure observer — reads only public accessors and world bookkeeping.
  fn lineage_sweep(&mut self, gid: u64) {
    let (seed, tick, generation) = (self.seed, self.tick_count, self.generation_of(gid));
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
    if self.record_is_append_only(gid) {
      let mut content: Vec<(u64, Option<ForkId>, AppliedLog)> = Vec::new();
      for &node in &nodes {
        if self.parked.contains(&(node, gid)) {
          continue;
        }
        let Some(ep) = self.hosts[&node].group(&gid) else {
          continue;
        };
        let lineage = ep.fork_id();
        content.push((node, lineage, self.aligned_applied(node, gid)));
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

  /// Whether `gid`'s FSM applied record is APPEND-ONLY this instant — the precondition for the
  /// lineage ledger's per-index phantom-quorum leg (see `lineage_sweep`). A merge (freeze, park,
  /// absorb, or a merged-away husk) re-bases or folds records non-monotonically, so `(index →
  /// bytes)` stops being a stable committed history; a split is phantom-safe and is NOT excluded.
  fn record_is_append_only(&self, gid: u64) -> bool {
    !(self.group_absorbed(gid)
      || self.is_merged(gid)
      || self.group_frozen(gid)
      || self.merge_choreography_active(gid)
      || self.group_merge_parked(gid))
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
    let gids: Vec<u64> = self.checkers.keys().copied().collect();
    for gid in gids {
      let generation = self.generation_of(gid);
      let ck = self.checkers.get_mut(&gid).expect("checker exists");
      if let Err(v) = checker::finalize_membership(ck) {
        panic!(
          "SAFETY ORACLE VIOLATION (run-end final pass): {v}\n  group={gid} seed={seed}\n  \
           (replay: run_multi_vopr for this seed and inspect the snapshot install at the \
           reported boundary)",
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
  fn cross_talk_sweep(&mut self, gid: u64) {
    // The floor derives from the GROUP record, never the replica's wiring path: a fork-born
    // group's inherited baseline cells carry an ANCESTOR's tag legitimately (the handover), and
    // every arrival path — fork materialization, a transferred snapshot into a fresh observer,
    // a crash restore from the durable blob — presents them as the record's leading prefix. An
    // onward split a replica applied can only SHRINK that prefix below the recorded count
    // (`LogSm::split` removes moved-key cells record-wide), so flooring at the full count never
    // judges an inherited cell; the few own-tagged cells the floor may skip on a shrunk record
    // would pass the tag assert anyway — under-coverage there, never a false positive.
    let baseline = self.groups.get(&gid).map_or(0, |m| m.fork_baseline);
    let carried = self
      .groups
      .get(&gid)
      .map(|m| m.carried_tags.clone())
      .unwrap_or_default();
    for node in self.node_ids.clone() {
      if !self.hosts[&node].contains_group(&gid) {
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

  /// The group's incarnation (gen) for the one-identity grant key, from the lifecycle registry
  /// (recreation is what moves it).
  fn gen_of(&self, gid: u64) -> u64 {
    self.generation_of(gid)
  }

  /// Assemble the per-group [`ClusterView`](crate::ClusterView) from `gid`'s hosting nodes —
  /// field-for-field the shape `Cluster::view` builds, scoped to one group's replicas and their
  /// `(node, gid)` stores, so the UNCHANGED oracle suite judges each group independently.
  fn group_view(&self, gid: u64) -> checker::ClusterView {
    let mut nodes = Vec::new();
    for &node in &self.node_ids {
      let Some(ep) = self.hosts[&node].group(&gid) else {
        continue;
      };
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
      let applied_log = if self.group_absorbed(gid) {
        self.applied_of(node, gid)
      } else {
        self.aligned_applied(node, gid)
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
      positional_agreement: !self.group_absorbed(gid),
      seed: self.seed,
      tick: self.tick_count,
      committed_voters: {
        let v = self.committed_voters_of(gid);
        if v.is_empty() { None } else { Some(v) }
      },
      committed_transitions: self
        .pending_transitions
        .get(&gid)
        .cloned()
        .unwrap_or_default(),
      new_installs: self
        .pending_new_installs
        .get(&gid)
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
  fn committed_voters_of(&self, gid: u64) -> BTreeSet<u64> {
    let authoritative = self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter_map(|&n| self.hosts[&n].group(&gid))
      .filter(|ep| ep.role().is_leader())
      .max_by_key(|ep| ep.term());
    if let Some(ep) = authoritative {
      return ep.conf_state().voters().iter().copied().collect();
    }
    let mut tally: BTreeMap<BTreeSet<u64>, usize> = BTreeMap::new();
    for &n in &self.node_ids {
      if self.parked.contains(&(n, gid)) {
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
          let generation = self.generation_of(gid);
          self.lineage.observe_install(
            seed,
            tick,
            (node, gid, generation),
            meta.fork_id(),
            meta.last_index().get(),
          );
          self.pending_new_installs.entry(gid).or_default().push((
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
            self.pending_transitions.entry(gid).or_default().push((
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
      let generation = self.gen_of(gid);
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
      let Some(host) = self.hosts.get_mut(&m.to) else {
        continue; // unknown node id — drop safely
      };
      if !host.contains_group(&m.gid) {
        continue; // unhosted group — silent drop, the connection-level tombstone/demux semantics
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
mod tests;

mod faults;
mod lifecycle;
mod merge;
mod query;
mod split;
