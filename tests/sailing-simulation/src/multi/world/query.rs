//! Read-only accessors the multi-VOPR layer drives the world through (the world's fields stay
//! private to the `world` module tree; the fuzzer is a pure API consumer).

use super::*;

impl MultiWorld {
  /// Every node id the world has wired, ascending.
  pub(crate) fn node_list(&self) -> Vec<u64> {
    let mut ids = self.node_ids.clone();
    ids.sort_unstable();
    ids
  }

  /// The LIVE (non-retired) group ids, ascending.
  pub(crate) fn live_groups(&self) -> Vec<u64> {
    self
      .groups
      .iter()
      .filter(|(_, m)| !m.retired)
      .map(|(gid, _)| *gid)
      .collect()
  }

  /// The RETIRED group ids, ascending (the recreation candidates).
  pub(crate) fn retired_groups(&self) -> Vec<u64> {
    self
      .groups
      .iter()
      .filter(|(_, m)| m.retired)
      .map(|(gid, _)| *gid)
      .collect()
  }

  /// Whether node `node` currently hosts a replica of `gid`.
  pub(crate) fn hosts_group(&self, node: u64, gid: u64) -> bool {
    self
      .hosts
      .get(&node)
      .is_some_and(|h| h.contains_group(&gid))
  }

  /// The nodes currently hosting `gid`, ascending.
  pub(crate) fn hosting_nodes(&self, gid: u64) -> Vec<u64> {
    self
      .node_ids
      .iter()
      .copied()
      .filter(|n| self.hosts[n].contains_group(&gid))
      .collect()
  }

  /// The AUTHORITATIVE hosting replicas of `gid`: non-parked AND caught up to the applied frontier
  /// (tied for the furthest applied index among the non-parked). This is the admission set for the
  /// per-key value floor. It carries [`leader_of`](Self::leader_of)'s parked exclusion — a reaped
  /// replica is no longer a protocol participant — and adds the caught-up narrowing the value grain
  /// needs: a parked or behind replica whose `applied()` predates a committed `Split`/`Merge` still
  /// holds cells that reshape has since moved or reset (`LogSm::split`/`absorb` rewrite `applied()`
  /// only, and only ON APPLY), so its record would resurrect a value the group no longer serves.
  /// Only a replica at the applied frontier has certainly applied the group's latest committed
  /// prefix, so its split-aware `applied()` matches the view the serve side reads. The applied
  /// INDEX is the caught-up key, not the record length: the index stays monotone and comparable
  /// across a reshape, where the record length (cells vanish or graft record-wide) does not.
  pub(crate) fn authoritative_nodes(&self, gid: u64) -> Vec<u64> {
    let live: Vec<u64> = self
      .node_ids
      .iter()
      .copied()
      .filter(|n| self.hosts[n].contains_group(&gid) && !self.parked.contains(&(*n, gid)))
      .collect();
    let frontier = live
      .iter()
      .map(|&n| self.applied_index_of(n, gid))
      .max()
      .unwrap_or(sailing_proto::Index::ZERO);
    live
      .into_iter()
      .filter(|&n| self.applied_index_of(n, gid) == frontier)
      .collect()
  }

  /// Whether `gid`'s one-conf-change-in-flight gate is currently armed.
  pub(crate) fn conf_in_flight_of(&self, gid: u64) -> bool {
    self.groups.get(&gid).is_some_and(|m| m.conf_in_flight)
  }

  /// How many of `gid`'s UNPARKED hosting replicas currently believe themselves leader. A parked
  /// (reaped) replica is excluded like [`leader_of`](Self::leader_of) excludes it: a zombie
  /// parked in Leader role stays Leader forever (delivery-isolated, nothing deposes it) and
  /// would otherwise pin this count above 1 for the rest of the run.
  pub(crate) fn group_leader_count(&self, gid: u64) -> usize {
    self
      .node_ids
      .iter()
      .filter(|&&n| !self.parked.contains(&(n, gid)))
      .filter(|n| {
        self.hosts[n]
          .group(&gid)
          .is_some_and(|ep| ep.role().is_leader())
      })
      .count()
  }

  /// Replica `(node, gid)`'s applied watermark (`Index::ZERO` if not hosted).
  pub(crate) fn applied_index_of(&self, node: u64, gid: u64) -> sailing_proto::Index {
    self
      .hosts
      .get(&node)
      .and_then(|h| h.group(&gid))
      .map(sailing_proto::Endpoint::applied_index)
      .unwrap_or(sailing_proto::Index::ZERO)
  }

  /// The maximum commit index any replica of `gid` currently believes — the group's
  /// completed-write watermark (an entry committed anywhere is durably on a quorum).
  pub(crate) fn max_commit_of(&self, gid: u64) -> sailing_proto::Index {
    self
      .node_ids
      .iter()
      .filter_map(|n| self.hosts[n].group(&gid))
      .map(sailing_proto::Endpoint::commit_index)
      .max()
      .unwrap_or(sailing_proto::Index::ZERO)
  }

  /// The maximum term across every replica of every hosted group (the report's high-water).
  pub(crate) fn max_term_all(&self) -> u64 {
    self
      .node_ids
      .iter()
      .flat_map(|n| {
        let host = &self.hosts[n];
        host
          .group_ids()
          .filter_map(|gid| host.group(gid))
          .map(|ep| ep.term().get())
          .collect::<Vec<_>>()
      })
      .max()
      .unwrap_or(0)
  }

  /// Initiate a linearizable read on replica `(node, gid)` (the fuzzer's non-panicking entry):
  /// returns whether the replica ACCEPTED the read. A refusal — no known leader, capacity,
  /// poison, unhosted — is a legitimate no-op under faults; a `DuplicateContext` panics (the
  /// caller mints unique contexts, so a duplicate is a harness bug).
  pub(crate) fn read_index_on(&mut self, node: u64, gid: u64, context: &[u8]) -> bool {
    let Some(host) = self.hosts.get_mut(&node) else {
      return false;
    };
    if !host.contains_group(&gid) {
      return false;
    }
    let log = self.logs.get(&(node, gid)).expect("replica log");
    let stable = self.stables.get(&(node, gid)).expect("replica stable");
    match host.read_index(
      &gid,
      self.now,
      log,
      stable,
      bytes::Bytes::copy_from_slice(context),
    ) {
      Some(Ok(())) => true,
      Some(Err(sailing_proto::ReadIndexError::DuplicateContext)) => {
        panic!(
          "read_index_on: duplicate context {context:?} — the caller must mint unique contexts"
        )
      }
      Some(Err(_)) | None => false,
    }
  }

  /// All `ReadState`s confirmed for replica `(node, gid)`, in confirmation order (monotone; the
  /// vec survives replica teardown and re-wiring so ledger scan offsets stay valid).
  pub(crate) fn read_states_of(&self, node: u64, gid: u64) -> &[sailing_proto::ReadState] {
    self
      .read_states
      .get(&(node, gid))
      .map(Vec::as_slice)
      .unwrap_or(&[])
  }

  /// Every `(node, gid)` key that has ever confirmed a read (the ledger's scan population).
  pub(crate) fn read_state_keys(&self) -> Vec<(u64, u64)> {
    self.read_states.keys().copied().collect()
  }

  /// Ask `gid`'s current leader to transfer leadership to `to`; whether the leader accepted.
  pub(crate) fn transfer_group_leader(&mut self, gid: u64, to: u64) -> bool {
    let Some(leader) = self.leader_of(gid) else {
      return false;
    };
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get(&(leader, gid)).expect("leader log");
    let stable = self.stables.get(&(leader, gid)).expect("leader stable");
    matches!(
      host.transfer_leader(&gid, self.now, log, stable, to),
      Some(Ok(()))
    )
  }

  /// Propose a read-mode migration on `gid`'s current leader; the assigned index or `None` (no
  /// leader, migration in flight, or the leader lacks the target mode's knobs — all legitimate
  /// refusals the fuzzer treats as no-ops).
  pub(crate) fn propose_read_mode_change(
    &mut self,
    gid: u64,
    mode: sailing_proto::ReadOnlyOption,
  ) -> Option<sailing_proto::Index> {
    let leader = self.leader_of(gid)?;
    let host = self.hosts.get_mut(&leader).expect("leader host exists");
    let log = self.logs.get_mut(&(leader, gid)).expect("leader log");
    let stable = self.stables.get(&(leader, gid)).expect("leader stable");
    host
      .propose_read_mode_change(&gid, self.now, log, stable, mode)?
      .ok()
  }

  /// Total gid-tagged applied entries the cross-talk sweep has decoded and judged — the
  /// oracle's non-vacuity witness.
  pub(crate) fn cross_talk_checked(&self) -> u64 {
    self.cross_talk_checked
  }

  /// Group `gid`'s LIVE key population, ascending (the keyed workload's pick set; empty for an
  /// unknown gid).
  pub(crate) fn group_keys_of(&self, gid: u64) -> Vec<u16> {
    self
      .groups
      .get(&gid)
      .map(|m| m.keys.iter().copied().collect())
      .unwrap_or_default()
  }

  /// Whether `key` is currently in `gid`'s live population — false once a split moved it to a
  /// child (the read ledger's moved-key drop predicate).
  pub(crate) fn group_holds_key(&self, gid: u64, key: u16) -> bool {
    self.groups.get(&gid).is_some_and(|m| m.keys.contains(&key))
  }

  /// Committed splits the world REGISTERED (child materialized) — the report's split witness.
  pub(crate) fn splits_applied(&self) -> u64 {
    self.splits_applied
  }

  /// `Event::SplitStale` observations drained across the run.
  #[cfg(test)]
  pub(crate) fn split_stale_observed(&self) -> u64 {
    self.split_stale
  }

  /// Late forks the pump refused against the catalog (retired or recreation-outpaced child id).
  #[cfg(test)]
  pub(crate) fn split_refused_observed(&self) -> u64 {
    self.split_refused
  }

  /// Split-conflict signals drained across the run (unreachable under fresh-minted child ids;
  /// counted so a future reachable path is visible, never silently swallowed).
  #[cfg(test)]
  pub(crate) fn split_conflicts_observed(&self) -> u64 {
    self.split_conflicts
  }

  /// Completed world ticks (for the fuzzer's panic messages).
  pub(crate) fn ticks(&self) -> u64 {
    self.tick_count
  }

  /// The `snapshot_threshold` a genuinely constructed replica was built under, read off the
  /// retained per-replica config surface — any one witnesses it, since the world applies a single
  /// uniform override at construction. `None` only when no replica has ever been wired. The
  /// non-`cfg(test)` twin of `replica_snapshot_threshold`, callable from the VOPR runner so the
  /// report's applied-threshold witness comes from a config a replica ACTUALLY received, not the
  /// value the profile requested.
  pub(crate) fn applied_snapshot_threshold(&self) -> Option<usize> {
    self
      .configs
      .values()
      .next()
      .map(|config| config.snapshot_threshold())
  }

  /// The `snapshot_threshold` the replica of `gid` on `node` was constructed under, read off the
  /// retained per-replica config — the exact one handed to the container at admission and rebuilt
  /// from on crash restore. `None` when `node` hosts no replica of `gid`. The seam's witness: it
  /// pins that the profile's snapshot-threshold override actually reaches the replica config on
  /// every construction path, which the group-shape non-vacuity counters cannot observe.
  #[cfg(test)]
  pub(crate) fn replica_snapshot_threshold(&self, node: u64, gid: u64) -> Option<usize> {
    self
      .configs
      .get(&(node, gid))
      .map(|config| config.snapshot_threshold())
  }

  /// A one-line per-replica state dump for `gid` (the fuzzer's livelock panic format).
  pub(crate) fn dbg_group(&self, gid: u64) -> String {
    let per: Vec<String> = self
      .hosting_nodes(gid)
      .into_iter()
      .map(|n| {
        let ep = self.hosts[&n].group(&gid).expect("hosting");
        std::format!(
          "n{n}[{:?}{} term={} commit={} applied={} poison={} voters={:?}]",
          ep.role(),
          if self.parked.contains(&(n, gid)) {
            " PARKED"
          } else {
            ""
          },
          ep.term().get(),
          ep.commit_index().get(),
          ep.applied_index().get(),
          ep.is_poisoned(),
          ep.conf_state().voters(),
        )
      })
      .collect();
    per.join(" ")
  }
}
