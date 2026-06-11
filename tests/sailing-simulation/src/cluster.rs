//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. It wires the run loop that drives real consensus.
use crate::{
  Checker, ClusterView, DurableEntry, LogSm, MemLog, MemStable, NetworkFaults, NodeView,
  StorageFaults, checker, network::NetPrng,
};
use core::time::Duration;
use sailing_proto::{
  ConfChange, ConfChangeV2, Config, Endpoint, Instant, LogStore, Message, Outgoing, ReadState,
  StableStore, Term,
};
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// Per-node snapshot-install tally: incremented each time an `Event::SnapshotInstalled`
/// is drained from that node's event queue during `tick`. Used by snapshot tests to
/// assert that `InstallSnapshot` was genuinely exercised.
type SnapCount = u64;

/// Per-node conf-change tally: incremented each time an `Event::ConfChanged`
/// is drained from that node's event queue during `tick`. Used by membership tests to
/// assert that conf changes were actually applied.
type ConfChangedCount = u64;

type Node = Endpoint<u64, LogSm>;

/// Round-trip a consensus message through the real `Message<I>` wire codec at the delivery seam.
///
/// With the `wire` feature OFF (default) this is the identity — the message moves as a value, as it
/// always has. With `wire` ON, every message the VOPR delivers is `encode`d to bytes and `decode`d
/// back, so the entire fuzzer (crashes, partitions, membership churn) exercises the codec; a codec
/// defect on ANY message panics with the failing seed/tick. The round-trip is a verified identity
/// (`decode(encode(m)) == m`), so it does not change behavior or determinism.
#[cfg(feature = "wire")]
fn wire_roundtrip(message: Message<u64>) -> Message<u64> {
  use sailing_proto::Data;
  let mut buf = Vec::new();
  message.encode(&mut buf);
  // `decode_exact` enforces whole-buffer consumption (the framing invariant).
  let decoded = Message::<u64>::decode_exact(bytes::Bytes::from(buf))
    .expect("a consensus message must round-trip through the wire codec");
  // Assert VALUE identity, not just consumption — a field swap that still consumes the frame
  // would otherwise silently alter the delivered message and change fuzzer behavior.
  assert_eq!(
    decoded, message,
    "the wire codec must round-trip to an identical message"
  );
  decoded
}

#[cfg(not(feature = "wire"))]
#[inline(always)]
fn wire_roundtrip(message: Message<u64>) -> Message<u64> {
  message
}

/// One node's applied log as `(index, command-bytes)` pairs, copied out for cross-run / cross-node
/// comparison (see [`Cluster::applied_entries_of`]). A `Vec<AppliedLog>` is the whole cluster's
/// applied state captured at a point in time.
pub type AppliedLog = Vec<(u64, Vec<u8>)>;

/// An in-flight typed message: `(deliver_at, from, to, message)`.
struct InFlight {
  deliver_at: Instant,
  from: u64,
  to: u64,
  message: Message<u64>,
}

/// A deterministic cluster. Node ids start at 0 and increase monotonically; nodes may
/// be added mid-run. The parallel `Vec`s (nodes/logs/stables/configs/…) are indexed by
/// position; `node_idx` maps id → Vec position for O(log n) lookups.
pub struct Cluster {
  /// Node ids, in Vec order (ids[i] is the id of the node at position i).
  node_ids: Vec<u64>,
  /// Reverse map: id → Vec position.
  node_idx: BTreeMap<u64, usize>,
  nodes: Vec<Node>,
  logs: Vec<MemLog>,
  stables: Vec<MemStable<u64>>,
  /// Config for each node, kept so `crash` can rebuild from durable stores.
  configs: Vec<Config<u64>>,
  bus: VecDeque<InFlight>,
  now: Instant,
  /// Node ids that are fully partitioned: their outgoing messages are dropped and
  /// inbound messages to/from them are dropped. Init empty.
  isolated: BTreeSet<u64>,
  /// Node ids that have been removed from the cluster. The agreement oracle skips
  /// consistency checks for removed nodes' applied-log suffixes beyond the point of removal.
  /// Removed nodes are kept in the Vec structures but are also `isolated` so they don't
  /// receive further messages or participate in elections.
  removed: BTreeSet<u64>,
  /// Double-vote tripwire: maps `(granter, term)` → `grantee`.
  /// A second distinct grantee for the same `(granter, term)` is a fatal bug.
  grants: BTreeMap<(u64, Term), u64>,
  /// Per-node count of `Event::SnapshotInstalled` events drained during `tick`.
  /// Monotonically incremented; reset to zero on `crash`+restart.
  snapshot_installs: Vec<SnapCount>,
  /// Per-node restart counter (incarnation), bumped each time `crash` rebuilds the node from
  /// durable storage. The checker resets a node's commit/term monotonicity baseline when its
  /// incarnation changes: the batched commit/term persist can drop an in-memory advance still in
  /// the fsync window on crash, and the restarted node re-derives it.
  restarts: Vec<u64>,
  /// Per-node count of `Event::ConfChanged` events drained during `tick`.
  /// Monotonically incremented; never reset.
  conf_changed: Vec<ConfChangedCount>,
  /// Per-node list of `ReadState`s confirmed via `Event::ReadState` during `tick`.
  /// Appended monotonically; never cleared. Index into the outer Vec by node position.
  read_states: Vec<Vec<ReadState>>,
  /// When true, the stores run in [`crate::StoreMode::Async`] (staged writes / fsync-loss window):
  /// `tick` flushes every node's staged writes each step (before draining completions), and a
  /// `crash` that discards in-flight writes loses exactly the un-flushed window. Default false
  /// (synchronous stores, byte-identical to the original).
  async_mode: bool,
  /// Seeded network fault model applied per message at the bus-push point (latency/jitter/drop/
  /// duplicate/reorder). Default [`NetworkFaults::none()`] — a faultless, zero-latency, FIFO bus
  /// byte-identical to the original bus. Installed via [`Cluster::set_network_faults`].
  net_faults: NetworkFaults,
  /// Seeded network-fault PRNG, on a stream distinct from the per-node store seeds. Drives every
  /// drop/dup roll and jitter draw, so the same cluster seed yields an identical run. Only consumed
  /// when `net_faults` is non-`none()` (an all-off config touches the PRNG only for the bounded
  /// drop/dup checks, which short-circuit on a `0` rate without a draw — see [`NetPrng`]).
  net_prng: NetPrng,
  /// Per-`(from,to)` last-scheduled `deliver_at`, used to keep deliveries FIFO when
  /// `net_faults.reorder == false`: a message's `deliver_at` is clamped to be ≥ the previous one
  /// for that ordered pair. Empty (and unused) when reorder is on or faults are off.
  net_last_sched: BTreeMap<(u64, u64), Instant>,
  /// Count of messages dropped by the seeded network fault model (non-vacuity counter so tests can
  /// assert the fault model actually fired). Never incremented by partition/isolation drops.
  net_dropped: u64,
  /// Count of messages duplicated by the seeded network fault model (each fired duplication counts
  /// once, i.e. the number of EXTRA copies pushed). Non-vacuity counter.
  net_duplicated: u64,
  /// The per-tick safety-oracle suite. Holds the cross-tick history (commit/term
  /// monotonicity, the committed-history high-water) and runs the WHOLE oracle suite at the end of
  /// every [`tick`](Self::tick); a violation panics with the oracle name + seed + tick for exact
  /// VOPR replay. A pure observer — it never mutates the simulated nodes/stores and never draws a
  /// PRNG, so the run is byte-identical with or without it. See [`crate::checker`].
  checker: Checker,
  /// The cluster construction seed, threaded into the oracle panic for VOPR replay. Captured from
  /// the seed passed to [`new_async`](Self::new_async); `0` for the (seedless) sync constructors.
  seed: u64,
  /// Monotonic count of completed [`tick`](Self::tick)s, threaded into the oracle panic so a
  /// violation pinpoints the exact step to replay.
  tick_count: u64,
  /// The per-node `Config` transform, applied to the bootstrap config of EVERY node — the initial
  /// members and any joiner wired in mid-run — so a dynamically-added node gets the same knobs
  /// (e.g. `pre_vote`/`check_quorum`) as the founders. Without this a freshly-added voter would run
  /// the default config and, sitting far behind, could disrupt elections.
  node_configure: std::boxed::Box<dyn Fn(Config<u64>) -> Config<u64>>,
}

impl Cluster {
  /// Async mode: flush every node's staged (in-flight) writes to durable state, modeling the
  /// fsync for the in-flight window completing between driver iterations. No-op for sync stores
  /// (their `flush` is a no-op) but only ever called when `async_mode` is set.
  fn flush_all(&mut self) {
    for i in 0..self.nodes.len() {
      self.logs[i].flush();
      self.stables[i].flush();
    }
  }

  /// Drain storage completions for every node and collect any messages they produce.
  /// Returns `true` if any new messages were enqueued onto the bus.
  fn drain_storage_all(&mut self) -> bool {
    let mut any_new = false;
    for i in 0..self.nodes.len() {
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(self.now, log, stable);
    }
    // Collect outgoing messages produced by completion handlers (e.g. deferred acks once a staged
    // append flushes). Same path as the `tick` outgoing-drain: the structural oracles + seeded
    // network faults are applied via `schedule_send`.
    for i in 0..self.nodes.len() {
      let id = self.node_ids[i];
      if self.isolated.contains(&id) {
        while self.nodes[i].poll_message().is_some() {}
      } else {
        while let Some(out) = self.nodes[i].poll_message() {
          any_new = true;
          let (to, message) = Outgoing::into_parts(out);
          self.schedule_send(i, to, message);
        }
      }
      while let Some(ev) = self.nodes[i].poll_event() {
        if ev.is_snapshot_installed() {
          self.snapshot_installs[i] += 1;
        }
        if ev.is_conf_changed() {
          self.conf_changed[i] += 1;
        }
        if let sailing_proto::Event::ReadState(rs) = ev {
          self.read_states[i].push(rs);
        }
      }
    }
    any_new
  }

  /// Run the structural oracles on a message node `i` is SENDING, then apply the seeded
  /// [`NetworkFaults`] and push the resulting `InFlight`(s) onto the bus.
  ///
  /// **Oracle ordering (critical):** the append-before-ack and one-grant-per-term tripwires run on
  /// EVERY sent message, BEFORE the drop/duplicate roll — they audit what the node SENDS regardless
  /// of delivery fate, so a dropped message never bypasses an oracle. (A reorder/dup must likewise
  /// never produce a double-vote or a premature ack; the proto's idempotency must absorb them.)
  ///
  /// **Fault application (seeded, deterministic):**
  /// - **drop:** with probability `drop_per_mille/1000`, do not push (the message is lost).
  /// - **duplicate:** with probability `duplicate_per_mille/1000`, push the SAME message TWICE; each
  ///   copy gets an independent jitter draw, so the copies may arrive at different times.
  /// - **latency+jitter:** `deliver_at = now + latency + U[0, jitter]` (seeded uniform). With
  ///   nonzero jitter messages can be delivered out of order; if `reorder == false`, each (from,to)
  ///   pair's `deliver_at` is clamped to be ≥ the previous one for that pair (FIFO).
  ///
  /// When `net_faults.is_none()` this is byte-identical to the original push (no draw, no clamp,
  /// `deliver_at == now`, exactly one `InFlight`). Returns whether at least one copy was pushed.
  fn schedule_send(&mut self, i: usize, to: u64, message: Message<u64>) -> bool {
    let from = self.node_ids[i];

    // ── Structural assertion (a): append-before-ack ──────────────────────────────
    // A success AppendResp must not outrun the node's readable log. (The proto's append-before-ack
    // ordering — deferring a NEW suffix's ack to its durability via `on_log_appended` — is exercised
    // by the fsync-window integration test; this send-time tripwire is a coarse outran-the-log
    // guard. It uses the VISIBLE `last_index()` so it stays byte-identical to the original in sync mode and
    // does not flag the legitimate "duplicate AppendEntries, entries already present" ack path that
    // can fire for a visible-but-in-flight suffix. The per-entry quorum-durability of every COMMITTED
    // index is enforced separately by the `commit_is_quorum_durable` oracle on the durable snapshot.)
    if let Message::AppendResp(a) = &message {
      if !a.reject() {
        assert!(
          self.logs[i].last_index() >= a.match_index(),
          "append-before-ack violated: node {from} acked {:?} but last_index is {:?} \
           (durable_last={:?} inflight={} restarts={})",
          a.match_index(),
          self.logs[i].last_index(),
          self.logs[i].durable_last_index(),
          self.logs[i].has_inflight(),
          self.restarts[i],
        );
      }
    }
    // ── Structural assertion (b): one-grant-per-(node,term) ──────────────────────
    // A success VoteResp from `from` in term `T` to candidate `to` must not appear a second time
    // for a different candidate — that would be a double-vote. Holds under reorder+dup: a duplicate
    // grant to the SAME candidate is fine; a grant to a DIFFERENT one in the same term is a bug.
    if let Message::VoteResp(vr) = &message {
      // Only a REAL-vote grant binds (it persists `voted_for` for the term). A PRE-vote grant is
      // non-binding — "would I vote for you" — so a node may grant pre-votes to several candidates
      // in the same term without it being a double-vote; exclude them from the tripwire.
      if !vr.reject() && !vr.pre_vote() {
        let term = vr.term();
        match self.grants.get(&(from, term)) {
          Some(&prev) => assert_eq!(
            prev, to,
            "double-vote bug: node {from} granted vote in term {term:?} to both {prev} and {to}"
          ),
          None => {
            self.grants.insert((from, term), to);
          }
        }
      }
    }

    // Fast path: faults off ⇒ original behavior (zero-latency, FIFO, single push). Keeps the
    // sync path byte-identical to the original and never touches the network PRNG or FIFO map.
    if self.net_faults.is_none() {
      self.bus.push_back(InFlight {
        deliver_at: self.now,
        from,
        to,
        message,
      });
      return true;
    }

    // ── Seeded drop ───────────────────────────────────────────────────────────────
    if self
      .net_prng
      .chance_per_mille(self.net_faults.drop_per_mille)
    {
      self.net_dropped += 1;
      return false; // lost in flight
    }

    // ── Seeded duplicate ──────────────────────────────────────────────────────────
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
      // Each copy gets an independent jitter draw (a dup may overtake its twin).
      let jitter = self.net_prng.jitter_draw(self.net_faults.jitter);
      let mut deliver_at = self.now + self.net_faults.latency + jitter;
      // FIFO clamp: when reorder is disabled, never schedule a message before the previous one for
      // this ordered pair (so jitter delays but never reorders within (from,to)).
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
      self.bus.push_back(InFlight {
        deliver_at,
        from,
        to,
        message: message.clone(),
      });
    }
    true
  }

  /// Deliver all messages on the bus that are due at or before `self.now`.
  /// Returns `true` if any message was delivered.
  fn deliver_due(&mut self) -> bool {
    let mut delivered = false;
    let mut rest: VecDeque<InFlight> = VecDeque::new();
    while let Some(m) = self.bus.pop_front() {
      if m.deliver_at <= self.now {
        if self.isolated.contains(&m.from) || self.isolated.contains(&m.to) {
          // Drop silently — partition swallows it.
        } else if let Some(&to_idx) = self.node_idx.get(&m.to) {
          delivered = true;
          let (log, stable) = (&mut self.logs[to_idx], &mut self.stables[to_idx]);
          let message = wire_roundtrip(m.message);
          self.nodes[to_idx].handle_message(self.now, log, stable, m.from, message);
        }
        // else: message to an unknown id (shouldn't happen, but drop safely)
      } else {
        rest.push_back(m);
      }
    }
    self.bus = rest;
    delivered
  }

  /// Advance the simulation one step. Returns `true` if any work happened.
  ///
  /// A single step:
  ///   a. Advance virtual time to the earliest pending deadline.
  ///   b. Fire all timers due at that time.
  ///   c. Flush outgoing → deliver due → drain storage for all nodes → repeat until
  ///      quiescent at this timestamp (zero-latency bus drains completely before the
  ///      next timer can fire). Panics if the inner loop exceeds 10_000 iterations
  ///      (indicates a livelock bug).
  pub fn tick(&mut self) -> bool {
    let mut progressed = false;

    // Step a+b: advance clock and fire timers.
    let next_timer = self.nodes.iter().filter_map(Endpoint::poll_timeout).min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
        progressed = true;
      }
      for i in 0..self.nodes.len() {
        if self.nodes[i].poll_timeout().is_some_and(|d| d <= self.now) {
          progressed = true;
          let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
          self.nodes[i].handle_timeout(self.now, log, stable);
        }
      }
    }

    // Step c: flush outgoing → deliver → drain storage → repeat until stable.
    let mut iters = 0u32;
    loop {
      iters += 1;
      assert!(
        iters <= 10_000,
        "Cluster::tick inner loop exceeded 10_000 iterations — livelock?"
      );

      // Drain all node outgoing queues onto the bus.
      // Skip isolated nodes: their outgoing messages are dropped (partition).
      let mut any_new = false;
      for i in 0..self.nodes.len() {
        let id = self.node_ids[i];
        if self.isolated.contains(&id) {
          // Drain and discard so the queue doesn't grow unboundedly.
          while self.nodes[i].poll_message().is_some() {}
        } else {
          while let Some(out) = self.nodes[i].poll_message() {
            any_new = true;
            progressed = true;
            let (to, message) = Outgoing::into_parts(out);
            // Run the structural oracles and apply the seeded network faults, then push onto the
            // bus. The oracles run on every SENT message (inside `schedule_send`), BEFORE the
            // drop/dup roll, so a dropped message never bypasses a tripwire.
            self.schedule_send(i, to, message);
          }
        }
        while let Some(ev) = self.nodes[i].poll_event() {
          progressed = true;
          if ev.is_snapshot_installed() {
            self.snapshot_installs[i] += 1;
          }
          if ev.is_conf_changed() {
            self.conf_changed[i] += 1;
          }
          if let sailing_proto::Event::ReadState(rs) = ev {
            self.read_states[i].push(rs);
          }
        }
      }

      // Deliver all messages due now.
      let delivered = self.deliver_due();
      if delivered {
        progressed = true;
      }

      // Async mode: flush each node's staged (in-flight) writes to durable state BEFORE
      // draining completions — modeling the fsync for the in-flight window completing between
      // driver iterations. A `crash()` that runs `discard_inflight()` WITHOUT a preceding
      // `flush()` therefore loses exactly the staged window. No-op in sync mode.
      if self.async_mode {
        self.flush_all();
      }

      // Drain storage completions for every node (deferred acks produced here
      // will be picked up by the outgoing-drain in the next iteration).
      let storage_produced = self.drain_storage_all();
      if storage_produced {
        progressed = true;
      }

      if !any_new && !delivered && !storage_produced {
        break;
      }
    }

    // The cluster is now quiescent at this timestamp (delivery + storage drained) — a consistent
    // observable state. Advance the tick counter and run the WHOLE per-tick safety-oracle suite.
    // A violation panics with the oracle name + seed + tick for exact VOPR replay. The
    // suite is a pure observer: it reads only public accessors / non-faulting durable seams and
    // never mutates the nodes/stores or draws a PRNG, so the run stays byte-identical and
    // deterministic. (The send-time append-before-ack / one-grant tripwires in `schedule_send`
    // remain as earlier-firing immediate checks; this is the consolidated guarantee.)
    self.tick_count += 1;
    let view = self.view();
    self.checker.check_or_panic(&view);

    progressed
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn three_node_cluster_ticks_and_eventually_elects() {
    let mut c = Cluster::new(3);
    assert_eq!(c.size(), 3);
    // endpoints arm election timers immediately; the cluster should elect a leader.
    let mut found = false;
    for _ in 0..100 {
      c.tick();
      if c.leader().is_some() {
        found = true;
        break;
      }
    }
    assert!(found, "a leader should emerge within 100 ticks");
  }

  /// Drive a cluster to agreement on a batch and return each node's applied (index, command) log.
  fn drive_and_capture(c: &mut Cluster, batch: u32) -> Vec<AppliedLog> {
    assert!(c.run_until(300, |c| c.leader_count() == 1));
    for i in 0..batch {
      c.run_until(100, |c| c.leader_count() == 1);
      c.propose(&i.to_le_bytes());
      c.run_until(60, |_| false);
    }
    assert!(c.run_until(600, |c| c.agreement_holds()
      && c.min_applied_len() >= batch as usize));
    (0..c.size() as u64)
      .map(|n| c.applied_entries_of(n))
      .collect()
  }

  #[test]
  fn faults_off_is_byte_identical_to_baseline() {
    // A cluster with the network fault model installed as `none()` must produce the EXACT same run
    // as a plain `Cluster::new` (no `deliver_at` change, no drops, no extra PRNG influence). This
    // is the byte-identity invariant, made explicit at the cluster level.
    let baseline = {
      let mut c = Cluster::new(3);
      drive_and_capture(&mut c, 8)
    };
    let with_off_faults = {
      let mut c = Cluster::new(3);
      c.set_network_faults(NetworkFaults::none(), 0xDEAD_BEEF);
      drive_and_capture(&mut c, 8)
    };
    assert_eq!(
      baseline, with_off_faults,
      "an all-off NetworkFaults config must be byte-identical to the faultless bus"
    );
    // And no fault counter moved (nothing was dropped or duplicated).
    let mut c = Cluster::new(3);
    c.set_network_faults(NetworkFaults::none(), 7);
    drive_and_capture(&mut c, 8);
    assert_eq!(c.net_dropped(), 0);
    assert_eq!(c.net_duplicated(), 0);
  }

  #[test]
  fn same_seed_same_run_under_faults() {
    // Cluster-level determinism: identical seed ⇒ identical applied logs AND identical fault tallies.
    let run = |seed: u64| -> (Vec<AppliedLog>, u64, u64) {
      let mut c = Cluster::new(3);
      c.set_network_faults(
        NetworkFaults {
          latency: Duration::from_millis(3),
          jitter: Duration::from_millis(20),
          drop_per_mille: 120,
          duplicate_per_mille: 90,
          reorder: true,
        },
        seed,
      );
      let logs = drive_and_capture(&mut c, 8);
      (logs, c.net_dropped(), c.net_duplicated())
    };
    assert_eq!(
      run(0x1234),
      run(0x1234),
      "same seed ⇒ identical run + tallies"
    );
  }
}

// `impl Cluster` is split by concern across these submodules.
mod build;
mod drive;
mod faults;
mod oracles;
mod query;
