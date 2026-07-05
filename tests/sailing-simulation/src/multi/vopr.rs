//! The multi-group VOPR: a deterministic, fault-injecting randomized fuzzer over a
//! [`MultiWorld`] of `MultiRaft` container hosts.
//!
//! [`run_multi_vopr`] is a pure function of `(seed, ticks, profile)`: every choice — world size,
//! the per-iteration action, victims, fault intensities, lifecycle churn — draws from one seeded
//! [`FaultPrng`](crate::store::FaultPrng) stream; no wall clock, no `rand`, no map-order
//! nondeterminism. The per-group safety-oracle suites plus the cross-talk and one-identity
//! tripwires run on every world tick (a violation panics with seed + tick); this loop adds the
//! liveness assertions (calm windows and the final quiesce) and the non-vacuity report.
//!
//! The action menu is DATA — [`MultiProfile`] is a named weight table plus per-replica config
//! knobs — which is the M6 seam: reshaping/storm profiles are weight/knob overrides over the
//! same loop.

use crate::{
  multi::{MultiWorld, decode_gkv, encode_gkv},
  store::FaultPrng,
};
use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

/// The number of distinct KEYS the per-group keyed-value workload writes (values ride the global
/// monotone counter, so per-`(group, key)` values strictly increase).
const NUM_KEYS: u16 = 8;

/// The smallest voter set a group may be shrunk to via `RemoveNode`.
const MIN_VOTERS: usize = 2;

/// A weighted multi-group action. The lifecycle verbs (`CreateGroup` / `RemoveGroup` /
/// `RecreateGroup`) plus the `(link, group)` mutes are the genuinely multi-group additions; the
/// rest are the single-group bodies parameterized by a seed-picked live group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiAction {
  /// Propose 1..=4 gid-tagged keyed-value commands on a live group's leader.
  ClientLoad,
  /// Issue 1..=3 linearizable reads on a live group (leader-direct or member-forwarded).
  ReadIndexLoad,
  /// Isolate a node (ALL its groups) — legal only if EVERY live group keeps a viable quorum.
  Partition,
  /// Heal a currently isolated node.
  Heal,
  /// Crash a node: every hosted replica loses its fsync window and restores from durable state.
  Crash,
  /// Mute one directed `(link, group)` edge — budgeted like Partition, per group.
  MuteGroup,
  /// Unmute one currently muted `(link, group)` edge.
  UnmuteGroup,
  /// Propose an AddNode / AddLearnerNode / RemoveNode on one group (one in flight PER GROUP).
  ConfChange,
  /// Ask one group's leader to transfer leadership to another of its voters.
  TransferLeader,
  /// Propose a read-mode migration on one group (most targets are legitimately refused under
  /// the plain config; the draw stays deterministic either way).
  MigrateReadMode,
  /// Re-roll the network + per-replica storage fault intensities.
  FaultReroll,
  /// Create a fresh group (monotone gid allocator; 3..=5 seed-chosen member nodes).
  CreateGroup,
  /// Retire a live group everywhere (never the last live one).
  RemoveGroup,
  /// Recreate a retired gid as the SAME logical group at gen+1.
  RecreateGroup,
}

/// A named action weight table plus per-replica config knobs — the profile seam M6's
/// storm/reshape profiles override (weights AND knobs, e.g. reusing the snapshot-threshold
/// field for their own compaction pressure).
#[derive(Debug, Clone, Copy)]
pub struct MultiProfile {
  /// The weighted action menu.
  weights: &'static [(MultiAction, u32)],
  /// A `Config::snapshot_threshold` override applied at EVERY replica construction the world
  /// performs (bootstrap voters, recreations, observers, resurrections — and crash restores via
  /// the retained per-replica config). `None` leaves the library's demand-driven default
  /// untouched: construction is byte-identical to a world without the seam, so the default
  /// profile's schedules (and its pinned regression seeds) cannot move.
  snapshot_threshold: Option<usize>,
}

impl MultiProfile {
  /// The default M1 mix: client load dominates, faults are frequent, lifecycle churn is rare
  /// but steady (every band exercises removal and recreation).
  pub const fn default_multi() -> Self {
    Self {
      weights: &[
        (MultiAction::ClientLoad, 50),
        (MultiAction::ReadIndexLoad, 12),
        (MultiAction::Partition, 8),
        (MultiAction::Heal, 6),
        (MultiAction::Crash, 5),
        (MultiAction::MuteGroup, 6),
        (MultiAction::UnmuteGroup, 4),
        (MultiAction::ConfChange, 5),
        (MultiAction::TransferLeader, 4),
        (MultiAction::MigrateReadMode, 3),
        (MultiAction::FaultReroll, 6),
        (MultiAction::CreateGroup, 3),
        (MultiAction::RemoveGroup, 2),
        (MultiAction::RecreateGroup, 2),
      ],
      snapshot_threshold: None,
    }
  }

  /// The snapshot-heavy band profile: the default menu (weights untouched — the single-group
  /// snapshot runners bias no actions either; the default mix's lifecycle churn is already what
  /// puts installs under removal/recreation) with `snapshot_threshold` lowered to a MODERATE
  /// seed-derived value, so groups compact within a bounded run and a lagging replica
  /// snapshot-installs — the coverage the demand-driven default (10_000) never reaches. The draw
  /// copies the single-group compacting entries (`run_vopr_joint_snapshot`): 256..=511, from a
  /// DEDICATED sub-stream so the master action/topology stream draws nothing of it.
  pub fn snapshot_heavy(seed: u64) -> Self {
    let mut p = FaultPrng::new(seed.rotate_left(28) ^ 0x4D53_4E50_5448_5231); // "MSNPTHR1"
    let threshold = 256 + (p.next_u64() % 256) as usize; // 256..=511
    Self {
      snapshot_threshold: Some(threshold),
      ..Self::default_multi()
    }
  }
}

/// Non-vacuity counters: what one [`run_multi_vopr`] actually exercised. Derived
/// `PartialEq`/`Eq` so the determinism test can compare whole reports.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MultiVoprReport {
  /// The world seed (echoed for replay).
  pub seed: u64,
  /// The `snapshot_threshold` a genuinely constructed replica was built under, read back off the
  /// retained per-replica config once the world seam applied the profile — the WITNESS that
  /// [`run_multi_vopr`] pushed `MultiProfile`'s override through world construction rather than
  /// leaving it inert in the profile. `None` only if the run wired no replica; a completed run
  /// always creates a group, so it reports `Some`: the library default under the default profile,
  /// a `256..=511` draw under [`MultiProfile::snapshot_heavy`].
  pub applied_snapshot_threshold: Option<usize>,
  /// World ticks executed across the run (main loop + calm windows + quiesce).
  pub ticks_run: u64,
  /// Groups created (the two initial groups plus every `CreateGroup` action).
  pub groups_created: u64,
  /// Groups retired via `RemoveGroup`.
  pub groups_removed: u64,
  /// Retired groups recreated at gen+1 via `RecreateGroup`.
  pub groups_recreated: u64,
  /// Live groups at run end.
  pub final_groups: usize,
  /// Client commands accepted by some leader (tracked per group for the quiesce check).
  pub proposals: u64,
  /// Proposed commands confirmed applied on their group at quiesce.
  pub committed: u64,
  /// Reads accepted by a replica.
  pub reads_issued: u64,
  /// Accepted reads whose confirmation passed the index-floor assertion.
  pub reads_confirmed: u64,
  /// Confirmed reads whose per-key VALUE was asserted at the serve point.
  pub reads_value_checked: u64,
  /// Conf-changes accepted by a group leader.
  pub conf_changes: u64,
  /// Leader transfers accepted.
  pub transfers: u64,
  /// Read-mode migrations accepted.
  pub read_mode_migrations: u64,
  /// Node isolations injected.
  pub partitions: u64,
  /// Node heals injected (outside the calm-window bulk heal).
  pub heals: u64,
  /// Node crashes injected.
  pub crashes: u64,
  /// `(link, group)` mutes injected.
  pub mutes: u64,
  /// `(link, group)` unmutes injected (outside the calm-window bulk unmute).
  pub unmutes: u64,
  /// Fault-intensity rerolls.
  pub fault_rerolls: u64,
  /// Calm windows opened (each asserted per-group election + fresh progress).
  pub calm_windows: u64,
  /// The maximum term observed across every replica of every group.
  pub max_term_seen: u64,
  /// Seeded network faults that fired (drops + duplications).
  pub faults_fired: u64,
  /// gid-tagged applied entries the cross-talk sweep judged (the isolation oracle's
  /// non-vacuity witness).
  pub cross_talk_checks: u64,
  /// Membership-coherence comparisons the run-end final pass performed, summed over every
  /// checker the run built (live groups + the retired archive) — how many observed snapshot
  /// installs actually faced the committed-config history verdict.
  pub membership_oracle_comparisons: u64,
  /// Observed installs the run-end final pass could NOT judge due to an incomplete
  /// committed-config HISTORY, summed over live + retired checkers. The finalize policy panics
  /// (with gid/generation attribution) on any nonzero per-checker count — the single-group
  /// sweep's `skipped == 0` policy — so a completed run always reports `0`.
  pub skipped_unwitnessed_installs: u64,
  /// Observed installs the run-end final pass SOUNDLY DECLINED because the resolved conf-change
  /// index is committed-final but its committed-log KIND was compacted before any tick observed
  /// it. Tolerated, exactly as the single-group policy tolerates the class: a bounded coverage
  /// limitation of compaction, not a soundness hole — the net never trusts a possibly-stale
  /// ConfChange.
  pub kind_unobservable_installs: u64,
}

/// The fuzzer's deterministic bookkeeping threaded through the run.
struct MState {
  /// The monotone group-id allocator (starts at 100; NEVER reused for a different logical
  /// group — `RecreateGroup` reuses a retired gid as the SAME logical group at gen+1).
  next_gid: u64,
  /// Per-group journal of every accepted client command (the quiesce expected set).
  expected: BTreeMap<u64, Vec<Vec<u8>>>,
  /// The global monotone client-command counter (distinct commands; per-key values increase).
  cmd_counter: u64,
}

/// Run one deterministic multi-group VOPR episode. Panics (with seed + tick, each a real bug) on
/// a safety-oracle violation, a calm-window livelock, or a quiesce failure.
pub fn run_multi_vopr(seed: u64, ticks: usize, profile: MultiProfile) -> MultiVoprReport {
  let mut prng = FaultPrng::new(seed ^ 0x4D56_4F50_525F_5631); // "MVOPR_V1"
  let nodes = 5 + (prng.next_u64() % 3); // 5..=7 hosts
  let mut w = MultiWorld::new(seed);
  w.set_snapshot_threshold(profile.snapshot_threshold);
  for n in 0..nodes {
    w.add_node(n);
  }

  let mut st = MState {
    next_gid: 100,
    expected: BTreeMap::new(),
    cmd_counter: 0,
  };
  let mut reads = MultiReadLedger::new();
  let mut report = MultiVoprReport {
    seed,
    ..MultiVoprReport::default()
  };

  // Two initial groups (the non-vacuity floor) elected under a faultless bus, then the seeded
  // baseline faults install and the weighted loop takes over.
  create_group_action(&mut w, &mut st, &mut prng, &mut report);
  create_group_action(&mut w, &mut st, &mut prng, &mut report);
  // Witness the applied threshold off a genuinely constructed replica now that the world seam has
  // wired the initial groups. Read from a real replica config (not `profile.snapshot_threshold`)
  // so severing the `set_snapshot_threshold` call above is caught — a profile read would tautologize.
  report.applied_snapshot_threshold = w.applied_snapshot_threshold();
  for gid in w.live_groups() {
    assert!(
      w.run_until(3_000, |w| w.leader_of(gid).is_some()),
      "MULTI VOPR LIVELOCK (initial-election): group {gid} elected no leader from a clean \
       start\n  seed={seed}"
    );
  }
  let baseline = roll_network_faults(&mut prng);
  w.set_network_faults(baseline, seed.rotate_left(16) ^ 0x004E_4554);
  reroll_storage(&mut w, &mut prng, seed);

  let calm_period = 60 + (prng.next_u64() % 60) as usize;
  let mut next_calm = calm_period;

  for iter in 0..ticks {
    for gid in w.live_groups() {
      w.reconcile_membership(gid);
    }
    let action = pick_action(&mut prng, profile);
    match action {
      MultiAction::ClientLoad => client_load(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::ReadIndexLoad => {
        read_index_load(&mut w, &mut reads, &mut prng, &mut report);
      }
      MultiAction::Partition => partition(&mut w, &mut prng, &mut report),
      MultiAction::Heal => heal_one(&mut w, &mut prng, &mut report),
      MultiAction::Crash => crash_one(&mut w, &mut prng, &mut report),
      MultiAction::MuteGroup => mute_group(&mut w, &mut prng, &mut report),
      MultiAction::UnmuteGroup => unmute_group(&mut w, &mut prng, &mut report),
      MultiAction::ConfChange => conf_change(&mut w, &mut prng, &mut report),
      MultiAction::TransferLeader => transfer_leader(&mut w, &mut prng, &mut report),
      MultiAction::MigrateReadMode => migrate_read_mode(&mut w, &mut prng, &mut report),
      MultiAction::FaultReroll => {
        let net = roll_network_faults(&mut prng);
        let net_seed = prng.next_u64();
        w.set_network_faults(net, net_seed);
        reroll_storage(&mut w, &mut prng, seed);
        report.fault_rerolls += 1;
      }
      MultiAction::CreateGroup => create_group_action(&mut w, &mut st, &mut prng, &mut report),
      MultiAction::RemoveGroup => remove_group_action(&mut w, &mut prng, &mut report),
      MultiAction::RecreateGroup => recreate_group_action(&mut w, &mut prng, &mut report),
    }

    let steps = 2 + (prng.next_u64() % 5) as usize; // 2..=6 ticks per iteration
    for _ in 0..steps {
      w.tick();
      report.ticks_run += 1;
    }
    reads.scan(&w, &mut report, seed);
    report.max_term_seen = report.max_term_seen.max(w.max_term_all());
    report.faults_fired = w.net_dropped() + w.net_duplicated();

    if iter + 1 >= next_calm {
      calm_window(&mut w, &mut st, &mut prng, &mut report, seed);
      report.calm_windows += 1;
      let jitter = (prng.next_u64() % 60) as usize;
      next_calm = iter + 1 + calm_period + jitter;
    }
  }

  quiesce(&mut w, &mut st, &mut report, seed);
  reads.scan(&w, &mut report, seed);
  // Membership-oracle VERDICT: the per-tick checks only RECORD snapshot-install observations;
  // the run-end final pass judges every one — across live groups AND retired archives — against
  // its group's FINAL, now-stable committed-config history. Panics on a mismatch AND on any
  // observation left without a verdict (skipped == 0, the single-group sweep's policy, enforced
  // per checker with gid/generation attribution); kind-unobservable declines are the tolerated
  // compaction class and only surface in the report.
  w.finalize_membership_or_panic(seed);
  report.membership_oracle_comparisons = w.membership_oracle_comparisons();
  report.skipped_unwitnessed_installs = w.skipped_unwitnessed_installs();
  report.kind_unobservable_installs = w.kind_unobservable_installs();
  report.final_groups = w.live_groups().len();
  report.cross_talk_checks = w.cross_talk_checked();
  report.max_term_seen = report.max_term_seen.max(w.max_term_all());
  report.faults_fired = w.net_dropped() + w.net_duplicated();
  report
}

/// Open a CALM WINDOW: back the adversary off entirely (heal every partition, clear every mute
/// and fault, restore poisoned nodes) and assert EVERY live group elects and commits fresh load
/// within a generous bound — failure is a livelock, and panics with seed + tick.
fn calm_window(
  w: &mut MultiWorld,
  st: &mut MState,
  prng: &mut FaultPrng,
  report: &mut MultiVoprReport,
  seed: u64,
) {
  for node in w.isolated_nodes() {
    w.heal(node);
  }
  w.unmute_all();
  w.set_network_faults(crate::NetworkFaults::none(), seed);
  clear_storage_faults(w, seed);
  for node in w.poisoned_nodes() {
    w.crash(node);
  }

  for gid in w.live_groups() {
    w.reconcile_membership(gid);
    let mut elected = false;
    for _ in 0..4_000 {
      if w.leader_of(gid).is_some() {
        elected = true;
        break;
      }
      w.tick();
      report.ticks_run += 1;
      w.reconcile_membership(gid);
    }
    assert!(
      elected,
      "MULTI VOPR LIVELOCK (calm window): group {gid} failed to elect within 4000 ticks after \
       healing everything\n  seed={seed} tick={}\n  {}",
      w.ticks(),
      w.dbg_group(gid),
    );
    w.reconcile_membership(gid);

    // Fresh PROGRESS: a majority of the group's committed voters must commit-and-apply new load.
    let voters: Vec<u64> = w.group_voters(gid).into_iter().collect();
    let quorum_applied = |w: &MultiWorld| -> usize {
      let mut lens: Vec<usize> = voters.iter().map(|&n| w.applied_of(n, gid).len()).collect();
      lens.sort_unstable_by(|a, b| b.cmp(a));
      lens.get(lens.len() / 2).copied().unwrap_or(0)
    };
    let target = quorum_applied(w) + 1 + (prng.next_u64() % 2) as usize;
    let mut budget = 6_000u32;
    while quorum_applied(w) < target {
      assert!(
        budget > 0,
        "MULTI VOPR LIVELOCK (calm window): group {gid} failed to commit fresh load within the \
         window\n  seed={seed} tick={}\n  {}",
        w.ticks(),
        w.dbg_group(gid),
      );
      w.reconcile_membership(gid);
      if w.leader_of(gid).is_some() {
        let key = (st.cmd_counter % NUM_KEYS as u64) as u16;
        let payload = encode_gkv(gid, key, st.cmd_counter);
        if w.propose(gid, &payload).is_some() {
          st.expected.entry(gid).or_default().push(payload);
          st.cmd_counter += 1;
          report.proposals += 1;
        }
      }
      w.tick();
      report.ticks_run += 1;
      budget -= 1;
    }
    assert!(
      w.agreement_holds(gid),
      "MULTI VOPR: group {gid} agreement must hold at the calm-window progress point (seed={seed})"
    );
  }
}

/// QUIESCE: fully heal the world and drain every LIVE group to convergence — a single leader,
/// agreement, every member fully caught up and unpoisoned — then assert each group's applied
/// client history is exactly a subset of what the fuzzer proposed FOR THAT GROUP, applied
/// identically on every member. Panics with seed on failure.
fn quiesce(w: &mut MultiWorld, st: &mut MState, report: &mut MultiVoprReport, seed: u64) {
  for node in w.isolated_nodes() {
    w.heal(node);
  }
  w.unmute_all();
  w.set_network_faults(crate::NetworkFaults::none(), seed);
  clear_storage_faults(w, seed);
  for node in w.poisoned_nodes() {
    w.crash(node);
  }

  let live = w.live_groups();
  let converged_group = |w: &MultiWorld, gid: u64| -> bool {
    if w.group_leader_count(gid) != 1 || !w.agreement_holds(gid) {
      return false;
    }
    let members: BTreeSet<u64> = w
      .group_voters(gid)
      .into_iter()
      .chain(w.group_learners(gid))
      .collect();
    if members.is_empty() {
      return false;
    }
    let lens: Vec<usize> = members
      .iter()
      .map(|&n| w.applied_of(n, gid).len())
      .collect();
    let caught_up = lens.iter().min() == lens.iter().max();
    caught_up && members.iter().all(|&n| w.hosts_group(n, gid))
  };
  let mut converged = false;
  for pass in 0..20_000u32 {
    for &gid in &live {
      w.reconcile_membership(gid);
    }
    if pass % 64 == 0 {
      for node in w.poisoned_nodes() {
        w.crash(node);
      }
    }
    w.tick();
    report.ticks_run += 1;
    if live.iter().all(|&gid| converged_group(w, gid)) {
      converged = true;
      break;
    }
  }
  if !converged {
    let dumps: Vec<String> = live
      .iter()
      .map(|&gid| std::format!("g{gid}: {}", w.dbg_group(gid)))
      .collect();
    panic!(
      "MULTI VOPR QUIESCE FAILURE: a fully-healed world failed to converge every live group \
       within 20000 ticks\n  seed={seed} (replay: run_multi_vopr({seed}, ticks, profile))\n  {}",
      dumps.join("\n  "),
    );
  }

  for &gid in &live {
    let leader = w.leader_of(gid).expect("quiesce converged to a leader");
    let leader_applied = w.applied_of(leader, gid);
    let expected: BTreeSet<Vec<u8>> = st
      .expected
      .get(&gid)
      .map(|v| v.iter().cloned().collect())
      .unwrap_or_default();
    let mut committed = 0u64;
    for (_, cmd) in &leader_applied {
      if cmd.is_empty() {
        continue; // empty / conf entries carry no client payload
      }
      assert!(
        expected.contains(cmd),
        "MULTI VOPR INTEGRITY FAILURE: group {gid} applied a command {cmd:?} the fuzzer never \
         proposed for it\n  seed={seed}",
      );
      committed += 1;
    }
    report.committed += committed;
    let leader_cmds: Vec<&Vec<u8>> = leader_applied
      .iter()
      .filter(|(_, cmd)| !cmd.is_empty())
      .map(|(_, cmd)| cmd)
      .collect();
    let members: BTreeSet<u64> = w
      .group_voters(gid)
      .into_iter()
      .chain(w.group_learners(gid))
      .collect();
    for member in members {
      let applied = w.applied_of(member, gid);
      let cmds: Vec<&Vec<u8>> = applied
        .iter()
        .filter(|(_, cmd)| !cmd.is_empty())
        .map(|(_, cmd)| cmd)
        .collect();
      assert_eq!(
        cmds, leader_cmds,
        "MULTI VOPR APPLY FAILURE: group {gid} member {member} applied a different committed \
         client history than leader {leader}\n  seed={seed}",
      );
    }
  }
}

#[cfg(test)]
mod tests;

// Action bodies + the seeded pick helpers, split by concern.
mod actions;
mod ledger;
use actions::*;
use ledger::MultiReadLedger;
