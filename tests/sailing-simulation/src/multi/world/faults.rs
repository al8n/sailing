//! Node- and `(link, group)`-scoped fault verbs plus crash/recovery for the multi-group world:
//! full-node partitions (all groups out), per-directed-link-per-group mutes (the §3 "(link,
//! group)" addressability no node-level fault can produce), the seeded network fault model reused
//! from the single-group bus, per-replica storage faults, and node crash-restore through
//! `MultiRaft::restore_group`.

use super::*;

impl MultiWorld {
  /// Isolate node `id`: drop all messages to and from it — every group it hosts partitions.
  pub(crate) fn isolate(&mut self, id: u64) {
    self.isolated.insert(id);
  }

  /// Heal the partition for node `id`.
  pub(crate) fn heal(&mut self, id: u64) {
    self.isolated.remove(&id);
  }

  /// The currently isolated node ids.
  pub(crate) fn isolated_nodes(&self) -> BTreeSet<u64> {
    self.isolated.clone()
  }

  /// Mute the DIRECTED link `from → to` for `gid` only: deliveries silently drop at the bus
  /// (AFTER the send-point oracles, like isolation), while every other group's traffic on the
  /// same link flows — the per-group asymmetric partition shape reshaping bugs hide in.
  pub(crate) fn mute_group(&mut self, from: u64, to: u64, gid: u64) {
    self.muted.insert((from, to, gid));
  }

  /// Unmute one directed `(from, to, gid)` link.
  pub(crate) fn unmute_group(&mut self, from: u64, to: u64, gid: u64) {
    self.muted.remove(&(from, to, gid));
  }

  /// Clear every `(link, group)` mute (the calm-window/quiesce back-off).
  pub(crate) fn unmute_all(&mut self) {
    self.muted.clear();
  }

  /// The current `(from, to, gid)` mute table (for budgets and the seeded unmute pick).
  pub(crate) fn muted_edges(&self) -> &BTreeSet<(u64, u64, u64)> {
    &self.muted
  }

  /// Install a seeded [`NetworkFaults`] config on the group-tagged bus, re-seeding the network
  /// PRNG (deterministic from the call site). All-off keeps the bus byte-identical to the
  /// faultless original.
  pub(crate) fn set_network_faults(&mut self, faults: NetworkFaults, seed: u64) {
    self.net_faults = faults;
    self.net_prng = NetPrng::new(seed);
    self.net_last_sched.clear();
  }

  /// Messages dropped by the seeded network fault model (never partition/mute drops).
  pub(crate) fn net_dropped(&self) -> u64 {
    self.net_dropped
  }

  /// Deliveries the GENERATION FENCE dropped across the run — the sim's mirror of the
  /// coordinators' `fenced_frames_dropped`. Nonzero only once a run has retired an incarnation
  /// whose replicas were still sending.
  pub(crate) fn fenced_dropped(&self) -> u64 {
    self.fenced_dropped
  }

  /// The DISRUPTION subset of [`fenced_dropped`](Self::fenced_dropped): retired-incarnation
  /// `(Pre)Vote`s the fence stopped before any replica saw them.
  pub(crate) fn fenced_votes_dropped(&self) -> u64 {
    self.fenced_votes_dropped
  }

  /// Put one message on the bus with an EXPLICIT sender incarnation stamp, bypassing
  /// `schedule_send` (which reads the stamp off a live host). Test-only: the world's own
  /// retirement purges a removed gid's in-flight traffic, so a retired incarnation's straggler —
  /// the very shape the generation fence exists for — cannot otherwise be observed at the seam.
  #[cfg(test)]
  pub(crate) fn inject_for_test(
    &mut self,
    from: u64,
    gid: u64,
    generation: u64,
    to: u64,
    message: sailing_proto::Message<u64>,
  ) {
    self.bus.push_back(super::GInFlight {
      deliver_at: self.now,
      gid,
      from,
      to,
      generation,
      message,
    });
  }

  /// Message duplications fired by the seeded network fault model.
  pub(crate) fn net_duplicated(&self) -> u64 {
    self.net_duplicated
  }

  /// Install seeded [`StorageFaults`] on the `(node, gid)` replica's store pair (no-op if the
  /// replica is not hosted).
  pub(crate) fn set_replica_faults(
    &mut self,
    node: u64,
    gid: u64,
    faults: StorageFaults,
    seed: u64,
  ) {
    if let Some(log) = self.logs.get_mut(&(node, gid)) {
      log.set_faults(faults, seed);
    }
    if let Some(stable) = self.stables.get_mut(&(node, gid)) {
      stable.set_faults(faults, seed.rotate_left(17));
    }
  }

  /// Crash node `id`: every hosted replica loses its in-flight (unflushed) store window and is
  /// rebuilt from durable state via `MultiRaft::restore_group` under its retained config; all
  /// in-flight bus traffic to/from the node is lost. Per-replica incarnations bump so the
  /// per-group checkers reset their monotonicity baselines across the restart boundary.
  pub(crate) fn crash(&mut self, id: u64) {
    let host = self.hosts.get_mut(&id).expect("crash: node exists");
    let gids: Vec<u64> = host.group_ids().copied().collect();
    let epoch = {
      let e = self.boot_epochs.entry(id).or_insert(0);
      *e += 1;
      *e
    };
    let mut fresh: MultiRaft<u64, u64, LogSm> = MultiRaft::new();
    for gid in &gids {
      let gid = *gid;
      // The durable relay lineage restored below (a real driver's `raise_relay_guard` after
      // restore) — read before the store borrows.
      let relayed = self.relayed_lineage.get(&(id, gid)).copied().unwrap_or(0);
      let log = self.logs.get_mut(&(id, gid)).expect("replica log");
      let stable = self.stables.get_mut(&(id, gid)).expect("replica stable");
      // Witness whether this crash lands MID-WINDOW (a non-empty un-flushed tail is about to be
      // lost) — read before the rollback clears it. Always false in sync mode (nothing is ever in
      // flight), so the counters stay 0 on the default profiles.
      let log_inflight = log.has_inflight();
      let stable_inflight = stable.has_inflight();
      log.discard_inflight();
      stable.discard_inflight();
      let config = self.configs[&(id, gid)].clone();
      fresh
        .restore_group_unchecked(
          gid,
          config,
          self.now,
          self.seed ^ id,
          LogSm::new(),
          epoch,
          log,
          stable,
        )
        .unwrap_or_else(|e| panic!("crash: restore of group {gid} on node {id}: {e:?}"));
      // Fold every already-materialized fork the replay re-stages back to a duplicate no-op.
      fresh.raise_relay_guard(&gid, relayed);
      *self.restarts.entry((id, gid)).or_insert(0) += 1;
      self.crashes_with_log_inflight += u64::from(log_inflight);
      self.crashes_with_stable_inflight += u64::from(stable_inflight);
    }
    self.hosts.insert(id, fresh);
    self.bus.retain(|m| m.from != id && m.to != id);
    self.net_last_sched.retain(|&(f, t), _| f != id && t != id);
  }

  /// Nodes with at least one POISONED replica (a fatal storage/apply error made an endpoint
  /// inert). A poisoned replica never recovers on its own — the calm window/quiesce crash the
  /// node to rebuild from durable state.
  pub(crate) fn poisoned_nodes(&self) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    for &node in &self.node_ids {
      let host = &self.hosts[&node];
      if host.group_ids().any(|gid| {
        host
          .group(gid)
          .is_some_and(sailing_proto::Endpoint::is_poisoned)
      }) {
        out.insert(node);
      }
    }
    out
  }
}
