//! The VOPR — a deterministic, fault-injecting randomized fuzzer for the consensus core.
//!
//! [`run_vopr`] is a **pure function of `(seed, ticks)`**: the same arguments replay a
//! bit-identical run and return an identical [`VoprReport`]. Every choice — cluster size, the
//! per-iteration action, which node to crash/isolate, the fault intensities, the calm-window
//! jitter — is drawn from a single seeded [`FaultPrng`](crate::store::FaultPrng) derived from
//! `seed`. There is **NO** wall-clock, **NO** `rand`, and **NO** `HashMap`-iteration-order
//! dependence anywhere in the driver.
//!
//! # What the VOPR composes
//!
//! - async stores + seeded [`StorageFaults`](crate::StorageFaults) (the real fsync-loss
//!   window under crash).
//! - the seeded [`NetworkFaults`](crate::NetworkFaults) bus (latency/jitter/drop/dup/reorder).
//! - the per-tick safety-oracle suite in [`checker`](crate::checker), which
//!   [`Cluster::tick`](crate::Cluster::tick) runs at the end of EVERY tick and which **panics with
//!   `seed`+`tick`** on a safety violation. The VOPR relies on that for SAFETY; it does not
//!   re-implement it.
//!
//! # What the VOPR adds on top of the safety oracles
//!
//! - **Liveness (calm windows):** periodically the adversary backs off — heal every partition,
//!   restart anything crashed, clear the faults — and the cluster is given a generous bounded number
//!   of ticks to elect a leader and make fresh progress (commit + apply new client load). If it does
//!   not, that is a **livelock** and [`run_vopr`] panics with `seed`+`tick`.
//! - **Liveness (quiesce):** after the loop the cluster is fully healed and drained; it must
//!   converge to a single leader, satisfy [`agreement_holds`](crate::Cluster::agreement_holds), and
//!   apply the ENTIRE committed history that the VOPR successfully proposed. If it cannot, the VOPR
//!   panics with `seed`+`tick`.
//! - **Non-vacuity ([`VoprReport`]):** counters of what the run actually exercised (crashes,
//!   partitions, conf-changes, committed entries, max term, faults fired, …) so the seed sweep can
//!   assert the runs were not vacuous (a VOPR that never crashed a node or never committed anything
//!   is useless).
//!
//! # The fault budget (why liveness is a VALID assertion)
//!
//! The adversary may never take down a quorum: at most `⌊(n-1)/2⌋` VOTERS may be simultaneously
//! isolated, so a healthy majority always survives and is always *able* to make progress once the
//! calm window heals the rest. A crash auto-restarts the node from its durable stores within the
//! same step (it exercises recovery, it does not sustain an outage), so only isolation counts
//! against the sustained budget. Conf-changes keep the cluster viable — the voter set never drops
//! below [`MIN_VOTERS`], and a remove is skipped if it would break the surviving quorum.
//!
//! If `run_vopr` ever surfaces a REAL proto bug (a safety-oracle panic, a calm-window livelock, or a
//! quiesce failure) on some seed, that seed+tick IS the bug report — do not tune the VOPR to dodge
//! it.

use crate::{Cluster, NetworkFaults, StorageFaults, store::FaultPrng};
use core::time::Duration;
use std::{collections::BTreeSet, vec::Vec};

/// The smallest voter-set size the VOPR will ever shrink a cluster to via `RemoveNode`. Keeping at
/// least this many voters guarantees a conf-change can never strand the cluster without a viable
/// quorum (and never trips the proto's `EmptyVoterSet` apply-time poison).
const MIN_VOTERS: usize = 2;

/// The largest cluster the VOPR will grow to (caps `AddNode`/`AddLearner`). Bounds the run and keeps
/// the node-id space small and deterministic.
const MAX_NODES: usize = 9;

/// Non-vacuity counters: a tally of what a single [`run_vopr`] actually EXERCISED.
///
/// The seed sweep aggregates these across many seeds and asserts real coverage (e.g. some
/// `crashes`, some `partitions`, `committed > 0`) — a run that never crashed a node or never
/// committed anything is *vacuous* and would pass every assertion while proving nothing.
///
/// Derived `PartialEq`/`Eq` so the determinism test can assert two runs of the same `(seed, ticks)`
/// produce an identical report.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VoprReport {
  /// The cluster seed this run replays from (echoed for convenience / replay).
  pub seed: u64,
  /// Number of `tick`s actually executed across the whole run (main loop + calm windows + quiesce).
  pub ticks_run: u64,
  /// Number of `crash(id)` injections (each loses the fsync window and recovers from durable state).
  pub crashes: u64,
  /// Number of times a crashed/isolated node was brought back (heal + the implicit crash-restart at
  /// the start of a calm window). A "restart" here is a node rejoining a healthy cluster.
  pub restarts: u64,
  /// Number of `isolate(id)` injections (a node partitioned away — counts against the fault budget).
  pub partitions: u64,
  /// Number of `heal(id)` injections (a partitioned node reconnected) outside the bulk calm-window
  /// heal.
  pub heals: u64,
  /// Number of conf-changes successfully PROPOSED (AddNode/AddLearner/RemoveNode that the leader
  /// accepted — not necessarily yet committed).
  pub conf_changes: u64,
  /// Number of client commands successfully PROPOSED (the leader accepted them; tracked in the
  /// `expected` log for the quiesce apply-everywhere check).
  pub proposals: u64,
  /// Number of proposed client commands observed COMMITTED-and-applied by the end of the run (the
  /// size of the proposed set that the quiesce phase confirmed applied cluster-wide).
  pub committed: u64,
  /// The maximum term observed across all nodes at any tick boundary during the run.
  pub max_term_seen: u64,
  /// Total seeded faults that FIRED: network drops + network duplications + (a lower bound on)
  /// storage faults, summed over the whole run.
  pub faults_fired: u64,
  /// Number of calm windows opened (each asserted liveness/progress).
  pub calm_windows: u64,
  /// Number of `read_index` requests a node ACCEPTED (leader-direct or follower-forwarded).
  pub reads_issued: u64,
  /// Number of accepted reads whose `ReadState` confirmation was observed AND passed the
  /// read-linearizability assertion (`index >= the completed-write floor at invocation`).
  pub reads_confirmed: u64,
  /// Number of leader transfers the leader ACCEPTED (the transfer itself may still abort).
  pub transfers: u64,
  /// The number of voters in the cluster at the end of the run (after all conf-changes).
  pub final_cluster_size: usize,
}

/// The weighted action menu. Client load dominates; faults are frequent but not constant; structural
/// changes (conf-change / crash) are rarer. The weights are summed and a seeded draw in
/// `[0, total)` selects the action, so the mix is deterministic from the seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
  /// Propose 1..=k client commands on the current leader (the most common action).
  ClientLoad,
  /// Isolate a voter (subject to the fault budget) — a sustained partition.
  Partition,
  /// Heal a currently-isolated node.
  Heal,
  /// Crash a node (loses its fsync window; auto-restarts from durable state).
  Crash,
  /// AddNode / AddLearner / RemoveNode (subject to viability + one-in-flight).
  ConfChange,
  /// Re-roll the network + storage fault intensities to a new seed-chosen level.
  FaultReroll,
  /// Issue 1..=k linearizable reads — on the leader, or (one third of draws) on a follower to
  /// exercise the forward path. Each records the completed-write floor for the oracle.
  ReadIndex,
  /// Ask the leader to transfer leadership to a random other voter (the transfer may abort —
  /// the oracles and calm windows catch anything it breaks).
  TransferLeader,
}

/// `(action, weight)` menu. Tuned so client load dominates and faults are frequent.
const MENU: &[(Action, u32)] = &[
  (Action::ClientLoad, 50),
  (Action::Partition, 9),
  (Action::Heal, 7),
  (Action::Crash, 6),
  (Action::ConfChange, 5),
  (Action::FaultReroll, 8),
  (Action::ReadIndex, 12),
  (Action::TransferLeader, 4),
];

/// The number of consecutive reconciliation passes a wired joiner must stay both SETTLED (no
/// conf-change in flight) and ABSENT from the committed membership before the VOPR concludes its
/// AddNode/AddLearner never committed and abandons it as an orphan. A small grace so a transient
/// leader loss (which conservatively clears `conf_in_flight`) right after wiring does not abandon a
/// joiner whose change was about to commit.
const ORPHAN_GRACE_PASSES: u32 = 3;

/// The VOPR's deterministic view of the cluster's logical state, threaded through the run.
///
/// **Membership is tracked from the cluster's REAL COMMITTED state, not optimistically.** `voters` /
/// `learners` are RECONCILED each iteration from the leader's runtime `conf_state()` (via
/// [`Cluster::committed_voters`]/[`Cluster::committed_learners`]) so a conf-change that was ACCEPTED
/// but never COMMITTED (leader crashed / lost quorum before replicating it) never leaves a PHANTOM
/// voter inflating the fault budget or pinning quiesce. The other sets are the VOPR's own
/// deterministic bookkeeping (the `Cluster`'s `isolated`/`removed` are private).
struct VoprState {
  /// The current committed VOTER ids, RECONCILED from `committed_voters()` (minus `gone`). The fault
  /// budget and the quiesce caught-up check operate on THIS real set. Last-known set is kept across a
  /// momentary leaderless tick (don't thrash).
  voters: BTreeSet<u64>,
  /// The current committed LEARNER ids, reconciled from `committed_learners()` (minus `gone`).
  learners: BTreeSet<u64>,
  /// Joiner ids the VOPR has `wire_joining_node`'d whose AddNode/AddLearner has not yet been observed
  /// committed (so they are not yet in `voters`/`learners`). Tracked so the sim-live set (faults /
  /// crash / poison-restart) still includes a freshly-wired node, and so an orphan (a joiner whose
  /// change never commits) can be detected and abandoned. Pruned once the node becomes a committed
  /// member or is abandoned into `gone`.
  wired: BTreeSet<u64>,
  /// Per-wired-joiner count of consecutive reconciliation passes it has been settled-but-absent from
  /// the committed membership — the orphan grace counter (see [`ORPHAN_GRACE_PASSES`]).
  missing_streak: std::collections::BTreeMap<u64, u32>,
  /// Nodes the VOPR has `c.mark_removed()` (abandoned orphans + nodes whose RemoveNode COMMITTED).
  /// The cluster keeps these isolated, so they are excluded from the reconciled `voters`/`learners`
  /// and from the sim-live set — they cannot pin `min_applied_len`/quiesce or absorb phantom isolation.
  gone: BTreeSet<u64>,
  /// Voters with an in-flight RemoveNode that has NOT yet committed. A removed voter is kept FULLY
  /// LIVE (it still votes / replicates) until its removal is observed committed — mirroring real Raft,
  /// where a node remains a voting member of every configuration up to and including the one that
  /// removes it. Isolating it at propose time (the old behavior) made it a PHANTOM voter: still in the
  /// surviving nodes' committed configs (hence counted toward quorum) yet unreachable, which can
  /// DEADLOCK an election when the removal never propagates (no node can reach the inflated quorum).
  /// [`reconcile_membership`] moves an id from here into `gone` (isolating it) once `committed_voters`
  /// no longer lists it.
  removing: BTreeSet<u64>,
  /// Currently VOPR-isolated nodes (the sustained-outage set; the fault budget caps the voter subset).
  down: BTreeSet<u64>,
  /// The next node id to hand out for an AddNode/AddLearner (monotonic; never reused).
  next_id: u64,
  /// Whether a conf-change the VOPR proposed has not yet been observed applied — enforces
  /// one-change-in-flight from the VOPR side so the panicking cluster helpers are never tripped.
  conf_in_flight: bool,
  /// The leader's `total_conf_changed` tally when the in-flight conf-change was proposed; the change
  /// is considered settled once the tally advances past this.
  conf_change_baseline: u64,
  /// Every client command the VOPR successfully proposed, as `(command-bytes)` — the quiesce phase
  /// confirms the committed subset is applied cluster-wide. A command is the 8-byte LE of a
  /// monotonically increasing counter, so all commands are DISTINCT (the apply check can match them
  /// positionally / by membership without ambiguity).
  proposed: Vec<Vec<u8>>,
  /// Monotonic client-command counter (the payload source; guarantees distinct commands).
  cmd_counter: u64,
}

impl VoprState {
  /// The sim-live node set: every node id the VOPR considers a participating part of the cluster —
  /// committed voters, committed learners, and freshly-wired joiners — MINUS the abandoned/removed
  /// `gone` set (those are isolated and inert). The fault/crash/poison-restart helpers iterate this
  /// in sorted (deterministic) order.
  fn live_ids(&self) -> BTreeSet<u64> {
    self
      .voters
      .iter()
      .chain(self.learners.iter())
      .chain(self.wired.iter())
      .filter(|id| !self.gone.contains(id))
      .copied()
      .collect()
  }

  /// The voters that are SETTLED members — committed voters MINUS any with an in-flight RemoveNode.
  /// A node being removed stays network-live so it can still VOTE (keeping election quorum reachable),
  /// but it is on its way out and may legitimately lag, so it is EXCLUDED from the catch-up liveness
  /// metrics (calm-window progress / quiesce full-catch-up): requiring a departing node to fully
  /// re-sync would falsely fire a livelock. Real liveness bugs are still caught — only the node(s)
  /// whose removal is in flight are exempt.
  ///
  /// NEVER empty when there are voters: `removing` can transiently cover EVERY voter (a stuck removal
  /// that never commits while another is proposed past it), but a cluster cannot be removing all its
  /// members at once — so if the difference is empty, fall back to the full voter set, measuring the
  /// survivors rather than an empty set (which made the metric vacuously 0). The
  /// quorum liveness metric already tolerates a departing laggard as a minority.
  fn settled_voters(&self) -> BTreeSet<u64> {
    let s: BTreeSet<u64> = self.voters.difference(&self.removing).copied().collect();
    if s.is_empty() { self.voters.clone() } else { s }
  }

  /// The number of voters the fault budget must treat as UNAVAILABLE to the surviving quorum. Counts a
  /// voter that is VOPR-isolated (`down`), `c.mark_removed()` (`gone`), OR has an in-flight RemoveNode
  /// (`removing`). The `removing` case is the apply-time liveness guard: under apply-time, a RemoveNode
  /// commits through the OLD (still-committed) config that INCLUDES the victim, so until it commits the
  /// victim's quorum slot must be reserved — otherwise the adversary could partition enough OTHER
  /// voters that the in-flight removal can never reach quorum, wedging the proto's one-in-flight gate.
  fn voters_down(&self) -> usize {
    self
      .voters
      .iter()
      .filter(|id| self.down.contains(id) || self.gone.contains(id) || self.removing.contains(id))
      .count()
  }

  /// The fault budget: at most `⌊(n-1)/2⌋` voters may be simultaneously down, where `n` is the
  /// current committed voter count. Returns how many MORE voters may be taken down right now.
  fn budget_remaining(&self) -> usize {
    let n = self.voters.len();
    let max_down = (n.saturating_sub(1)) / 2;
    max_down.saturating_sub(self.voters_down())
  }
}

/// The read-linearizability ledger: every accepted `read_index` records the completed-write
/// FLOOR at invocation (the max commit index anywhere in the cluster — an entry committed
/// anywhere is durably on a quorum and acknowledged, i.e. a completed write); every observed
/// `ReadState` confirmation must satisfy `index >= floor`, or the read could serve a state
/// missing a write that completed before the read began — a linearizability violation.
///
/// This is exactly the property the proto's current-term-commit gate (a new leader confirms
/// reads only after its own no-op commits) and the lease fence (a deposed leaseholder must
/// not confirm past its persisted promise) exist to provide; under `LeaseBased` seeds the
/// sim's single virtual clock makes the bound exact (no drift allowance).
///
/// An accepted read that NEVER confirms is legal under faults (a leader change clears
/// forwarded reads; a crash drops pending confirmations): never-confirmed beats wrongly
/// confirmed. Liveness is asserted separately — the calm window and quiesce each drive one
/// read through confirm-and-serve on a healthy cluster.
struct ReadLedger {
  /// Monotone context mint (8-byte BE on the wire); never reused, even for refused issues.
  next_ctx: u64,
  /// Accepted, unconfirmed reads: context -> the floor recorded at invocation.
  inflight: std::collections::BTreeMap<u64, sailing_proto::Index>,
  /// Per-node scan offset into the cluster's monotone `read_states_of` history.
  scan_off: std::collections::BTreeMap<u64, usize>,
  /// Retired reads: context -> (confirming node, confirmed index). Kept for the duplicate-
  /// confirmation oracle and for the calm/quiesce serve checks; bounded by reads issued.
  confirmed: std::collections::BTreeMap<u64, (u64, sailing_proto::Index)>,
}

impl ReadLedger {
  fn new() -> Self {
    Self {
      next_ctx: 0,
      inflight: std::collections::BTreeMap::new(),
      scan_off: std::collections::BTreeMap::new(),
      confirmed: std::collections::BTreeMap::new(),
    }
  }

  /// Issue one read on `target`, recording the floor iff the node accepts. Returns the minted
  /// context (accepted or not; contexts are never reused).
  fn issue(&mut self, c: &mut Cluster, target: u64, report: &mut VoprReport) -> u64 {
    let ctx = self.next_ctx;
    self.next_ctx += 1;
    let floor = c.max_commit();
    if c.read_index_on(target, &ctx.to_be_bytes()) {
      self.inflight.insert(ctx, floor);
      report.reads_issued += 1;
    }
    ctx
  }

  /// Scan every node's newly-confirmed `ReadState`s and run the linearizability assertion.
  /// Panics (with seed+tick) on a violation, an unknown context, or a duplicate confirmation
  /// — each is a real bug (the VOPR mints every context; the proto dedups in-flight reads).
  fn scan(&mut self, c: &Cluster, report: &mut VoprReport, seed: u64) {
    for id in c.node_ids() {
      let states = c.read_states_of(id);
      let off = self.scan_off.entry(id).or_insert(0);
      while *off < states.len() {
        let rs = &states[*off];
        *off += 1;
        let raw: [u8; 8] = rs
          .context()
          .as_ref()
          .try_into()
          .unwrap_or_else(|_| panic!(
            "[read-linearizability] non-VOPR read context {:?} confirmed on n{id} — the VOPR              mints every context in a VOPR run
  seed={seed} tick={}",
            rs.context(),
            c.view().tick,
          ));
        let ctx = u64::from_be_bytes(raw);
        match self.inflight.remove(&ctx) {
          Some(floor) => {
            assert!(
              rs.index() >= floor,
              "[read-linearizability] read ctx={ctx} confirmed on n{id} at index {} BELOW the                completed-write floor {} recorded at invocation — the read could serve a state                missing a committed write
  seed={seed} tick={} (replay: run_vopr({seed}, ticks))",
              rs.index().get(),
              floor.get(),
              c.view().tick,
            );
            self.confirmed.insert(ctx, (id, rs.index()));
            report.reads_confirmed += 1;
          }
          None => {
            let dup = self.confirmed.contains_key(&ctx);
            panic!(
              "[read-linearizability] {} for read ctx={ctx} on n{id} (index {})
                 seed={seed} tick={} (replay: run_vopr({seed}, ticks))",
              if dup {
                "DUPLICATE confirmation — one read context confirmed twice"
              } else {
                "confirmation for a context the VOPR never accepted"
              },
              rs.index().get(),
              c.view().tick,
            );
          }
        }
      }
    }
  }
}

/// Run one deterministic VOPR episode.
///
/// `seed` seeds every random choice (cluster size, actions, victims, fault intensities); `ticks` is
/// the number of main-loop iterations. Returns a [`VoprReport`] of what the run exercised. The same
/// `(seed, ticks)` always produces an identical run and an identical report.
///
/// **Panics** (each a real bug, carrying `seed`+`tick` for replay) on: a safety-oracle violation
/// (from inside [`Cluster::tick`]), a calm-window livelock (a healthy majority failed to make
/// progress within a generous bound), or a quiesce failure (a fully-healed cluster failed to
/// converge / apply the committed history).
pub fn run_vopr(seed: u64, ticks: usize) -> VoprReport {
  // The single master PRNG. Every draw in the run comes from here (deterministic from `seed`).
  let mut prng = FaultPrng::new(seed ^ 0x564F_5052_5F5F_5631); // "VOPR__V1"

  // ── Setup: seed-chosen cluster size in 2..=7 (INCLUDING even sizes). ───────────────────────────
  let size = 2 + (prng.next_u64() % 6) as usize; // 2..=7
  // Seed-chosen read/lease regime: a quarter of seeds run today's shape (Safe, no CheckQuorum), a
  // quarter add CheckQuorum (its stepdown now interacts with reads under partitions), a quarter run
  // LeaseBased reads (which REQUIRE CheckQuorum), and a quarter run LeaseGuard reads (the
  // commit-anchored lease: the post-election commit-wait + the read gate, drift-bounded, NO
  // CheckQuorum needed) — the lease machinery's only randomized-fault coverage. The mode-agnostic
  // read-linearizability oracle validates every confirmed read regardless of how it was served, so a
  // stale LeaseGuard serve (a commit-wait or read-gate bug) panics with seed+tick. Δ=300ms +
  // ε_drift=50ms < the 1000ms election timeout (the LeaseGuard validity bound). Drawn from the
  // master PRNG like every other choice.
  let read_mode = prng.next_u64() % 4;
  let mut c = Cluster::new_async_with(size, seed, move |cfg| {
    let cfg = cfg.with_pre_vote(true);
    match read_mode {
      0 => cfg,
      1 => cfg.with_check_quorum(true),
      2 => cfg
        .with_check_quorum(true)
        .with_read_only(sailing_proto::ReadOnlyOption::LeaseBased),
      _ => cfg
        .with_read_only(sailing_proto::ReadOnlyOption::LeaseGuard)
        .with_lease_duration(Duration::from_millis(300))
        .with_clock_drift_bound(Duration::from_millis(50)),
    }
  });

  // Install a seed-chosen baseline network + per-node storage fault config (modest — the run must
  // still be able to make progress; calm windows back it off entirely).
  let baseline_net = roll_network_faults(&mut prng, /* calm */ false);
  c.set_network_faults(baseline_net, seed.rotate_left(16) ^ 0x004E_4554); // "NET"
  for id in 0..size as u64 {
    let sf = roll_storage_faults(&mut prng);
    c.set_node_faults(id, sf, seed.wrapping_add(id).rotate_left(11));
  }

  let mut st = VoprState {
    voters: (0..size as u64).collect(),
    learners: BTreeSet::new(),
    wired: BTreeSet::new(),
    missing_streak: std::collections::BTreeMap::new(),
    gone: BTreeSet::new(),
    removing: BTreeSet::new(),
    down: BTreeSet::new(),
    next_id: size as u64,
    conf_in_flight: false,
    conf_change_baseline: 0,
    proposed: Vec::new(),
    cmd_counter: 0,
  };
  let mut reads = ReadLedger::new();

  let mut report = VoprReport {
    seed,
    final_cluster_size: size,
    ..VoprReport::default()
  };

  // Elect an initial leader (bounded). A fresh async cluster under modest faults must elect; if it
  // cannot even from a clean start, that is itself a liveness bug.
  elect_leader(&mut c, 3_000, seed, /* phase */ "initial-election");
  observe(&mut c, &mut st, &mut report);
  reconcile_membership(&mut c, &mut st);

  // The next tick at which to open a calm window (seed-jittered cadence).
  let calm_period = 60 + (prng.next_u64() % 60) as usize; // every 60..=119 iterations
  let mut next_calm = calm_period;

  // ── Main loop ─────────────────────────────────────────────────────────────────────────────────
  for iter in 0..ticks {
    // Reconcile the tracked membership from the cluster's REAL committed state BEFORE any
    // budget/conf-change decision this iteration, so a phantom (accepted-but-never-committed) voter
    // never inflates the fault budget and an orphaned joiner is abandoned promptly.
    reconcile_membership(&mut c, &mut st);

    let action = pick_action(&mut prng);
    match action {
      Action::ClientLoad => client_load(&mut c, &mut st, &mut prng, &mut report),
      Action::Partition => partition(&mut c, &mut st, &mut prng, &mut report),
      Action::Heal => heal_one(&mut c, &mut st, &mut prng, &mut report),
      Action::Crash => crash_one(&mut c, &mut st, &mut prng, &mut report),
      Action::ConfChange => conf_change(&mut c, &mut st, &mut prng, &mut report),
      Action::FaultReroll => fault_reroll(&mut c, &st, &mut prng, seed),
      Action::ReadIndex => read_index_load(&mut c, &st, &mut reads, &mut prng, &mut report),
      Action::TransferLeader => transfer_leader(&mut c, &st, &mut prng, &mut report),
    }

    // Let messages flow a seed-chosen small number of ticks (1..=4). The safety-oracle checker runs every tick
    // and panics on a safety violation with seed+tick.
    let steps = 1 + (prng.next_u64() % 4) as usize;
    for _ in 0..steps {
      c.tick();
      report.ticks_run += 1;
    }
    observe(&mut c, &mut st, &mut report);
    reads.scan(&c, &mut report, seed);
    refresh_conf_in_flight(&c, &mut st);

    // ── Calm window ───────────────────────────────────────────────────────────────────────────
    if iter + 1 >= next_calm {
      calm_window(&mut c, &mut st, &mut prng, &mut report, seed);
      read_round(&mut c, &mut reads, &mut report, seed, "calm-window");
      report.calm_windows += 1;
      let jitter = (prng.next_u64() % 60) as usize;
      next_calm = iter + 1 + calm_period + jitter;
    }
  }

  // ── Quiesce ───────────────────────────────────────────────────────────────────────────────────
  quiesce(&mut c, &mut st, &mut report, seed);
  // One final linearizable read on the converged cluster: it must confirm, pass the oracle, and
  // become servable. Also drains any confirmations that surfaced during quiesce itself.
  read_round(&mut c, &mut reads, &mut report, seed, "quiesce");

  report.final_cluster_size = st.voters.len();
  report
}

// ─── Liveness: calm window + quiesce ─────────────────────────────────────────────────────────────

/// Drive ONE linearizable read through confirm-and-serve on a healthy cluster — the read-path
/// liveness assertion (the per-iteration oracle only checks reads that happen to confirm; this
/// proves a read CAN confirm and become servable once the adversary backs off).
///
/// A pending read DIES SILENTLY when its node loses leadership (the step-down clears pending
/// and forwarded reads — by design, and the ledger treats never-confirmed as legal), and a
/// transfer or CheckQuorum round can still move leadership right after quiesce settles. So the
/// liveness claim is per-STABLE-leader: issue on the current leader and wait; if leadership
/// moves, re-issue on the new leader (bounded attempts). A read that fails to confirm while
/// its leader REMAINS leader — or attempts exhausting under endless churn on a calm cluster —
/// is the livelock, and panics with seed+tick. The ledger scan runs while waiting, so the read
/// also passes the linearizability assertion like any other.
fn read_round(
  c: &mut Cluster,
  reads: &mut ReadLedger,
  report: &mut VoprReport,
  seed: u64,
  phase: &str,
) {
  const ATTEMPTS: u32 = 8;
  const CONFIRM_BUDGET: u32 = 1_000; // per attempt; a healthy confirm is a heartbeat round
  const SERVE_BUDGET: u32 = 2_000;

  let mut last: Option<(u64, u64)> = None; // (issued-on, ctx) of the latest attempt, for the dump
  for _ in 0..ATTEMPTS {
    let Some(leader) = c.leader() else {
      // Leaderless moment (e.g. a transfer completing): let the election settle a little.
      for _ in 0..50 {
        c.tick();
        report.ticks_run += 1;
      }
      continue;
    };
    // The churn signal is the (leader id, term) PAIR, not the id alone: a step-down clears the
    // accepted read, and the SAME node can re-win at a higher term within one coarse tick — the
    // id alone would look stable while the context is already dead. A leader never advances its
    // term without stepping down first, so a term move on the same id is exactly re-election.
    let issued_term = c.term_of(leader);
    let ctx = reads.issue(c, leader, report);
    last = Some((leader, ctx));
    if !reads.inflight.contains_key(&ctx) {
      // Refused (capacity / a racing step-down): settle briefly and retry.
      for _ in 0..50 {
        c.tick();
        report.ticks_run += 1;
      }
      continue;
    }

    let mut budget = CONFIRM_BUDGET;
    let confirmed = loop {
      reads.scan(c, report, seed);
      if let Some(hit) = reads.confirmed.get(&ctx) {
        break Some(*hit);
      }
      if c.leader() != Some(leader) || c.term_of(leader) != issued_term {
        break None; // leadership (or its term) moved: the pending read died — re-issue
      }
      if budget == 0 {
        let tick = c.view().tick;
        let per_node = read_round_dump(c);
        panic!(
          "VOPR LIVELOCK ({phase} read): a read on a STABLE leader failed to confirm within \
           {CONFIRM_BUDGET} ticks\n  seed={seed} tick={tick} (replay: run_vopr({seed}, ticks))\n  \
           leader=n{leader} ctx={ctx}\n  nodes: {per_node}",
        );
      }
      c.tick();
      report.ticks_run += 1;
      budget -= 1;
    };

    let Some((node, index)) = confirmed else {
      continue; // next attempt on the new leader
    };
    let mut budget = SERVE_BUDGET;
    while c.applied_index_of(node) < index {
      // Keep the oracle running while we wait: OTHER in-flight reads can confirm during these
      // ticks, and after the final (quiesce) round nothing else would ever assert them.
      reads.scan(c, report, seed);
      if budget == 0 {
        let tick = c.view().tick;
        panic!(
          "VOPR LIVELOCK ({phase} read): read ctx={ctx} confirmed at index {} on n{node} but \
           the node failed to APPLY up to it within {SERVE_BUDGET} ticks (applied={})\n  \
           seed={seed} tick={tick} (replay: run_vopr({seed}, ticks))",
          index.get(),
          c.applied_index_of(node).get(),
        );
      }
      c.tick();
      report.ticks_run += 1;
      budget -= 1;
    }
    // The closing sweep: assert every confirmation that surfaced up to this instant (the final
    // quiesce round returns straight into run_vopr's return — this is the last scan).
    reads.scan(c, report, seed);
    return; // confirmed + servable: the read path is live
  }

  let tick = c.view().tick;
  let per_node = read_round_dump(c);
  let (on, ctx) = last.unwrap_or((u64::MAX, u64::MAX));
  panic!(
    "VOPR LIVELOCK ({phase} read): no read confirmed across {ATTEMPTS} attempts — leadership \
     churned endlessly on a calm cluster\n  seed={seed} tick={tick} (replay: run_vopr({seed}, \
     ticks))\n  last attempt: issued-on=n{on} ctx={ctx} leader-now={:?}\n  nodes: {per_node}",
    c.leader(),
  );
}

/// The per-node state dump for read-liveness panics (the house livelock format).
fn read_round_dump(c: &Cluster) -> String {
  let per_node: Vec<_> = c
    .node_ids()
    .into_iter()
    .map(|id| {
      std::format!(
        "n{id}[{:?} term={:?} commit={} applied={} poison={} reads={} {}]",
        c.role_of(id),
        c.term_of(id),
        c.commit_index_of(id).get(),
        c.applied_index_of(id).get(),
        c.is_poisoned(id),
        c.read_states_of(id).len(),
        c.dbg_membership(id),
      )
    })
    .collect();
  per_node.join(" ")
}

/// Open a CALM WINDOW: back the adversary off entirely (heal every partition, clear all faults) and
/// assert the cluster makes fresh PROGRESS — it must elect a leader and commit+apply new client load
/// within a generous bound. Failure to progress is a LIVELOCK and panics with `seed`+`tick`.
fn calm_window(
  c: &mut Cluster,
  st: &mut VoprState,
  prng: &mut FaultPrng,
  report: &mut VoprReport,
  seed: u64,
) {
  // Heal EVERY isolated node that is not permanently `gone` — not only those still tracked in
  // `st.down`. A node can be `c.isolated` yet absent from `st.down` (reconcile prunes `st.down` to the
  // current voters WITHOUT un-isolating it), which would otherwise strand it unreachable forever
  // (the case where a fresh 2-voter peer is isolated then dropped from `st.down`, never healed → it
  // sits at term 0 and the 2-voter quorum can never make progress).
  for node in c.isolated_nodes() {
    if !st.gone.contains(&node) {
      c.heal(node);
      report.restarts += 1;
    }
  }
  st.down.clear();
  // Clear all faults (network + every live node's storage) BEFORE restarting poisoned nodes, so a
  // restarted node does not immediately re-poison on a fault that is still installed.
  c.set_network_faults(NetworkFaults::none(), seed);
  for id in st.voters.iter().chain(st.learners.iter()).copied() {
    c.set_node_faults(id, StorageFaults::none(), seed.wrapping_add(id));
  }
  // Restart any POISONED nodes. A `transient_read` storage fault poisons a node (the proto's poison
  // path) and a poisoned node is inert FOREVER unless restarted — so a poisoned voter counts as
  // "down". The calm window's whole premise is that a healthy quorum can make progress, which
  // requires bringing poisoned voters back. `crash` resets poison and recovers from the durable log
  // (the lost apply tail re-syncs). Without this, accumulated poison could legitimately strand a
  // quorum and the liveness assertion would falsely fire.
  restart_poisoned(c, st, report);

  // Let the cluster settle to a single leader (generous bound — a healthy majority MUST converge).
  if !c.run_until(4_000, |c| c.leader_count() == 1) {
    // Last-resort phantom-gone recovery for a LEADERLESS deadlock: a divergent-config election cannot
    // resolve when a `gone` node a current voter still lists as a member never answers (e.g. a gone
    // node is still in voter n1's config, so neither candidate can assemble a quorum that
    // both branches accept). Reinstate any such node — trusting ONLY `st.voters` members' applied views,
    // never a hopeless removed laggard's — and give the cluster another window to elect. This
    // fires ONLY when genuinely stuck (no leader for 4000 ticks), so it can't over-reinstate a cleanly
    // removed node while the cluster is making progress.
    let needed: BTreeSet<u64> = st
      .voters
      .iter()
      .flat_map(|&v| c.node_voters(v))
      .filter(|id| st.gone.contains(id))
      .collect();
    let reinstated_any = !needed.is_empty();
    for g in needed {
      c.reinstate(g);
      st.gone.remove(&g);
    }
    if !reinstated_any || !c.run_until(4_000, |c| c.leader_count() == 1) {
      let tick = c.view().tick;
      let per_node: Vec<_> = st
        .voters
        .iter()
        .copied()
        .map(|id| {
          let (armed, due) = c.dbg_timer(id);
          std::format!(
            "n{id}[{:?} term={:?} applied={} last={} poison={} timer={} {}]",
            c.role_of(id),
            c.term_of(id),
            c.applied_len_of(id),
            c.last_index_of(id).get(),
            c.is_poisoned(id),
            if !armed {
              "DISARMED"
            } else if due {
              "due"
            } else {
              "future"
            },
            c.dbg_membership(id),
          )
        })
        .collect();
      panic!(
        "VOPR LIVELOCK (calm window): cluster failed to elect a single leader within 4000 ticks \
       after healing all partitions, clearing faults, and restarting poisoned nodes\n  \
       seed={seed} tick={tick} (replay: run_vopr({seed}, ticks))\n  voters={:?} learners={:?} \
       removing={:?} gone={:?} leaders={}\n  nodes: {}",
        st.voters,
        st.learners,
        st.removing,
        st.gone,
        c.leader_count(),
        per_node.join(" "),
      );
    }
  }

  // Reconcile NOW that a leader exists: a voter whose RemoveNode has COMMITTED (the leader applied it
  // and stopped replicating to it) must leave `st.voters` before we measure progress — otherwise the
  // just-removed node, frozen at its last applied index, would pin `voter_min_applied` and the
  // liveness assertion would falsely fire. Reconcile isolates it (`removing` → `gone`).
  reconcile_membership(c, st);

  // Assert PROGRESS (the liveness payoff): the committed VOTERS must COMMIT-and-APPLY `target_extra`
  // NEW client commands. The population is the LEADER's AUTHORITATIVE committed voter set
  // (`committed_voters()` — the nodes that actually form quorum), read DIRECTLY from the cluster, NOT
  // the harness's incremental `settled_voters` bookkeeping: under heavy churn the latter can drift to a
  // stale phantom (a laggard id the leader has already dropped) or be hollowed out by `removing` down
  // to that single phantom, which would pin or empty the metric. We
  // re-propose as needed because a command accepted by a leader that then loses leadership is not
  // guaranteed to commit (a legitimate Raft outcome).
  let voters_snapshot: Vec<u64> = c.committed_voters().into_iter().collect();
  // Liveness metric: a QUORUM (strict majority) of the settled voters must reach the target — NOT every
  // one. A cluster is LIVE when it COMMITS new entries, which needs only a quorum to ack and apply; a
  // MINORITY that legitimately lags — a freshly-added voter still catching up via snapshot, an
  // in-flight-removal victim, or a just-healed partition straggler — must not pin the metric. The
  // majority-th highest applied index IS the configuration's committed-and-applied frontier: it
  // advances iff the cluster is committing, while a real liveness bug (fewer than a quorum advancing)
  // still trips. Fixes the whole minority-laggard class.
  let voter_quorum_applied = |c: &Cluster| -> usize {
    let mut a: std::vec::Vec<usize> = voters_snapshot
      .iter()
      .map(|&id| c.applied_len_of(id))
      .collect();
    a.sort_unstable_by(|x, y| y.cmp(x)); // descending
    a.get(a.len() / 2).copied().unwrap_or(0) // majority-th highest (empty → 0)
  };
  let before = voter_quorum_applied(c);
  let target_extra = 1 + (prng.next_u64() % 3) as usize; // require 1..=3 new committed entries
  let target = before + target_extra;
  let mut budget = 8_000u32; // generous: a healthy cluster commits a handful of entries fast
  while voter_quorum_applied(c) < target {
    if budget == 0 {
      let tick = c.view().tick;
      let per_node: Vec<_> = st
        .voters
        .iter()
        .copied()
        .map(|id| {
          std::format!(
            "n{id}[{:?} term={:?} applied={} poison={} {}]",
            c.role_of(id),
            c.term_of(id),
            c.applied_len_of(id),
            c.is_poisoned(id),
            c.dbg_membership(id),
          )
        })
        .collect();
      panic!(
        "VOPR LIVELOCK (calm window): a healthy, fully-healed cluster failed to commit+apply \
         {target_extra} new client commands within the window (voter quorum_applied {} did not reach \
         {target})\n  seed={seed} tick={tick} (replay: run_vopr({seed}, ticks))\n  \
         voters={:?} learners={:?} leader={:?} removing={:?} gone={:?}\n  nodes: {}",
        voter_quorum_applied(c),
        st.voters,
        st.learners,
        c.leader(),
        st.removing,
        st.gone,
        per_node.join(" "),
      );
    }
    // Top up client load if there is a leader to accept it (re-propose past any non-committing ones).
    if c.leader_count() == 1 {
      let payload = st.cmd_counter.to_le_bytes().to_vec();
      if c.propose(&payload).is_some() {
        st.proposed.push(payload);
        st.cmd_counter += 1;
        report.proposals += 1;
      }
    }
    c.tick();
    report.ticks_run += 1;
    budget -= 1;
  }
  // Belt-and-suspenders: agreement must hold at the progress point (the per-tick suite already
  // guarantees this, but assert it here so a calm window is a clean liveness+safety checkpoint).
  assert!(
    c.agreement_holds(),
    "VOPR: agreement must hold at the calm-window progress point (seed={seed})"
  );
  refresh_conf_in_flight(c, st);
}

/// QUIESCE: fully heal the cluster, clear every fault, and drain to convergence. Then assert (a) a
/// single leader, (b) [`agreement_holds`](Cluster::agreement_holds), and (c) every client command
/// the VOPR proposed-AND-that-committed is applied consistently across the live nodes. A failure to
/// converge panics with `seed`+`tick`.
fn quiesce(c: &mut Cluster, st: &mut VoprState, report: &mut VoprReport, seed: u64) {
  // Heal everything and clear all faults, then restart any poisoned nodes (a poisoned node is inert
  // until restarted; quiesce must bring the whole live cluster back to apply the committed history).
  // Heal EVERY isolated-but-not-`gone` node, not just `st.down` (reconcile can prune `st.down` without
  // un-isolating — see the calm-window heal).
  for node in c.isolated_nodes() {
    if !st.gone.contains(&node) {
      c.heal(node);
      report.restarts += 1;
    }
  }
  st.down.clear();
  c.set_network_faults(NetworkFaults::none(), seed);
  for id in st.voters.iter().chain(st.learners.iter()).copied() {
    c.set_node_faults(id, StorageFaults::none(), seed.wrapping_add(id));
  }
  restart_poisoned(c, st, report);

  // Settle any in-flight RemoveNode FIRST: with a leader up and the cluster healed, the pending
  // removal commits quickly; reconcile then isolates the victim and drops it from `st.voters`. We
  // must do this before the convergence check below, because a removed voter — once the leader
  // applies its removal and stops replicating to it — freezes at its last applied index, and a stale
  // `st.voters` that still listed it would make `voters_fully_caught_up` unsatisfiable forever.
  let mut settle = 24u32;
  while !st.removing.is_empty() && settle > 0 {
    settle -= 1;
    if !c.run_until(2_000, |c| c.leader_count() == 1) {
      break; // no leader emerged — the convergence check below will report it
    }
    // Advance with the leader up so the in-flight RemoveNode commits + applies, then reconcile so
    // the now-removed victim is isolated and leaves `st.voters`.
    for _ in 0..300 {
      c.tick();
    }
    reconcile_membership(c, st);
  }

  // Drain until a single stable leader, agreement holds, AND every live voter is FULLY caught up to
  // the leader's applied length (not merely a prefix). Full catch-up is the precondition for the
  // apply-everywhere check below — `agreement_holds` only guarantees a consistent prefix, so a voter
  // a few entries behind would pass it yet legitimately have a shorter log; quiesce must wait for it
  // to finish applying. A generous bound; failure to converge is a genuine liveness bug.
  let converged = c.run_until(10_000, |c| {
    c.leader_count() == 1 && c.agreement_holds() && voters_fully_caught_up(c, st)
  });
  if !converged {
    let leader_len = c.leader().map(|l| c.applied_len_of(l)).unwrap_or(0);
    let per_node: Vec<_> = st
      .voters
      .iter()
      .copied()
      .map(|id| {
        std::format!(
          "n{id}[{:?} term={:?} applied={} poison={} inflight={}]",
          c.role_of(id),
          c.term_of(id),
          c.applied_len_of(id),
          c.is_poisoned(id),
          c.node_has_inflight(id),
        )
      })
      .collect();
    panic!(
      "VOPR QUIESCE FAILURE: a fully-healed, fault-free cluster failed to converge (single leader + \
       agreement + every voter caught up) within 10000 ticks\n  seed={seed} \
       (replay: run_vopr({seed}, ticks))\n  leader_count={} agreement={} leader_applied_len={} \
       min_applied_len={} voters={:?} removing={:?}\n  nodes: {}",
      c.leader_count(),
      c.agreement_holds(),
      leader_len,
      c.min_applied_len(),
      st.voters,
      st.removing,
      per_node.join(" "),
    );
  }

  // (c) Every proposed command that COMMITTED must be applied consistently across the live nodes.
  // We do not know which proposals committed (some were dropped / lost to a crash before commit), so
  // we assert the WEAKER, sound property: the set of commands applied by the leader is exactly the
  // committed history, and EVERY live node's applied log is a prefix-consistent view of it (already
  // guaranteed by agreement_holds), AND every command that appears applied anywhere appears in our
  // `proposed` set (no command was conjured) — plus we count how many of our proposals committed.
  let leader = c.leader().expect("quiesce converged to a leader");
  let leader_applied = c.applied_entries_of(leader);
  let proposed_set: BTreeSet<Vec<u8>> = st.proposed.iter().cloned().collect();

  // Every NORMAL command in the leader's applied log must be one we proposed (no phantom commands).
  // Conf-change/empty entries carry empty payloads and are skipped (they are not client commands).
  let mut committed_count = 0u64;
  for (_idx, cmd) in leader_applied.iter() {
    if cmd.is_empty() {
      continue; // empty / conf-change entry — not a client command
    }
    assert!(
      proposed_set.contains(cmd),
      "VOPR INTEGRITY FAILURE: leader applied a command {cmd:?} that the VOPR never proposed \
       (a conjured/duplicated committed entry)\n  seed={seed} (replay: run_vopr({seed}, ticks))",
    );
    committed_count += 1;
  }

  // And every live (non-removed) node must have applied EXACTLY the same committed client commands
  // as the leader (full agreement on the committed history — stronger than the prefix check when the
  // cluster is fully quiesced and caught up).
  let leader_cmds: Vec<&Vec<u8>> = leader_applied
    .iter()
    .filter(|(_, cmd)| !cmd.is_empty())
    .map(|(_, cmd)| cmd)
    .collect();
  // Settled voters only: a node whose removal is still in flight is departing and may legitimately
  // hold a shorter committed history (the quiesce settle loop above drains the common case, so this
  // normally equals `st.voters`).
  for id in st.settled_voters() {
    let applied = c.applied_entries_of(id);
    let cmds: Vec<&Vec<u8>> = applied
      .iter()
      .filter(|(_, cmd)| !cmd.is_empty())
      .map(|(_, cmd)| cmd)
      .collect();
    assert_eq!(
      cmds, leader_cmds,
      "VOPR APPLY FAILURE: after quiesce, voter {id} applied a different committed client history \
       than the leader {leader} — the committed history was not applied consistently everywhere\n  \
       seed={seed} (replay: run_vopr({seed}, ticks))",
    );
  }

  report.committed = committed_count;
  // One final oracle sweep at the fully-quiesced state (belt-and-suspenders; tick already ran it).
  c.run_oracles();
}

/// Elect a leader within `max` ticks or panic (a clean cluster that cannot elect is a liveness bug).
fn elect_leader(c: &mut Cluster, max: usize, seed: u64, phase: &str) {
  if !c.run_until(max, |c| c.leader_count() >= 1) {
    panic!(
      "VOPR LIVELOCK ({phase}): no leader emerged within {max} ticks from a clean start\n  \
       seed={seed} (replay: run_vopr({seed}, ticks))"
    );
  }
}

#[cfg(test)]
mod tests;

// vopr helpers, split by concern.
mod actions;
mod faults;
mod membership;
use actions::*;
use faults::*;
use membership::*;
