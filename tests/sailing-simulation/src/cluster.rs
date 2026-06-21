//! A deterministic, single-threaded cluster of `Endpoint`s over an in-memory typed-message
//! bus and a virtual clock. It wires the run loop that drives real consensus.
use crate::{
  Checker, ClusterView, DurableEntry, LogSm, MemLog, MemStable, NetworkFaults, NodeView,
  StorageFaults, checker, network::NetPrng,
};
use core::time::Duration;
use sailing_proto::{
  ConfChange, ConfChangeV2, Config, Endpoint, Instant, LogStore, Message, Now, Outgoing, ReadState,
  StableStore, Term, Wall,
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
  let mut buf = Vec::new();
  sailing_proto::wire::encode_message(&message, &mut buf);
  let decoded = sailing_proto::wire::decode_message::<u64>(bytes::Bytes::from(buf))
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
  /// Per-node clock RATE as a `(num, den)` rational: node `i`'s local clock reads
  /// `floor(global_now · num/den)` (see [`now_for`](Self::now_for)). `(1, 1)` for every node by
  /// default — a single global clock, byte-identical to the original. A [`set_clock_drift`] policy
  /// makes each node's clock run fast (`num > den`) or slow (`num < den`) within a bound, which is the
  /// ONLY thing that exercises LeaseGuard's cross-leader commit-wait margin (a same-clock read gate is
  /// blind to a constant offset; only differing RATES age a deposed lease and a successor's wait apart
  /// in real time). Indexed by Vec position, parallel to `nodes`; persists across `crash`/restart (a
  /// node's hardware clock rate does not change when it reboots).
  clock_rate: Vec<(u64, u64)>,
  /// The id → `(num, den)` rate policy, applied to every node (founders and mid-run joiners) so a
  /// dynamically-added node gets a deterministic rate. Default `|_| (1, 1)` (no drift). Installed via
  /// [`set_clock_drift`](Self::set_clock_drift).
  drift_policy: std::boxed::Box<dyn Fn(u64) -> (u64, u64)>,
  /// Count of LeaseGuard immediate reads served by a SUPERSEDED leader, classified at SERVE time: a
  /// read recorded by [`note_read_issue`] (its target was a superseded leader at `read_index` time) is
  /// counted when its `ReadState` later drains. Monotone, never reset; a clock-drift non-vacuity witness
  /// (a positive count proves the cross-leader read path was reached). `0` without drift, since a single
  /// global clock leaves no superseded-leader window.
  lease_superseded_serves: u64,
  /// Contexts of reads issued on a node that was a superseded leader at `read_index` time (the
  /// serve-time snapshot — see [`note_read_issue`]). Drained-and-retired when the matching `ReadState`
  /// is confirmed; contexts are unique per read, so a recorded read that never serves simply never
  /// matches. Kept separate from any node-observable state so recording cannot perturb the run.
  superseded_read_contexts: std::collections::BTreeSet<Vec<u8>>,
  /// FAILOVER-tier wall clock: when `true`, every node-facing call supplies a SYNCHRONIZED wall reading
  /// (`Now::synchronized`) alongside the monotonic instant, activating the precise commit-anchor. When
  /// `false` (the default), calls carry the monotonic instant ONLY (`Now::monotonic`, wall absent) —
  /// byte-identical to the original, and the precise anchor never fires. Set by [`enable_failover_clock`].
  failover: bool,
  /// Per-node SYNCHRONIZED-WALL offset in nanos, `offset[i] ∈ [−ε_unc, +ε_unc]`: node `i`'s wall reads
  /// `global_now + offset[i]` (saturating at 0), modelling `|W_i(t) − t| ≤ ε_unc`. Distinct from
  /// [`clock_rate`](Self::clock_rate) (which drifts the MONOTONIC clock): the offset perturbs ONLY the
  /// wall. `0` for every node by default and whenever `failover` is off (the wall is absent then). Varies
  /// across the run via [`resync_offsets`] (a re-draw smaller than the prior value is a backward NTP
  /// step). Indexed by Vec position; a mid-run joiner is appended a `0` and gets its first offset at the
  /// next resync.
  clock_offset: Vec<i64>,
  /// The synchronized-wall uncertainty bound ε_unc in nanos — the half-width of the per-node offset
  /// range drawn by [`resync_offsets`]. `0` until [`enable_failover_clock`] installs it.
  eps_unc_ns: u64,
}

/// Project a SYNCHRONIZED wall reading (nanos) from the global base time and a per-node offset,
/// SATURATING into the valid `u64` range at BOTH ends: a negative sum clamps to `0`, a sum past
/// `u64::MAX` clamps to `u64::MAX`. So an extreme base or offset can neither go negative nor WRAP to a
/// small value — a wrap would silently break the bounded cross-node wall skew the precise commit-anchor
/// relies on (one node's wall near `u64::MAX`, another wrapped near `0`). `i128` holds `base + offset`
/// exactly for every `base` a virtual-time run reaches and every offset the install accepts
/// (`|offset| ≤ i64::MAX`).
fn project_wall(base: u128, offset: i64) -> u64 {
  (base as i128 + offset as i128).clamp(0, u64::MAX as i128) as u64
}

/// Scale a duration by `num/den`, rounding DOWN (the `local_now` direction). u128 intermediate so a
/// long run cannot overflow; the result is clamped into `u64` nanos (a >584-year run would saturate,
/// never wrap). `num == den` is the exact identity.
fn scale_floor(d: Duration, num: u64, den: u64) -> Duration {
  let ns = d.as_nanos() * num as u128 / den as u128;
  Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX))
}

/// Scale a duration by `num/den`, rounding UP (the `global_of` inverse direction). Pairing ceil here
/// with floor in [`scale_floor`] makes the global instant at which a node's local deadline fires
/// EXACT: `local_now(global_of(ld)) >= ld` and no smaller global instant satisfies it.
fn scale_ceil(d: Duration, num: u64, den: u64) -> Duration {
  let ns = (d.as_nanos() * num as u128).div_ceil(den as u128);
  Duration::from_nanos(u64::try_from(ns).unwrap_or(u64::MAX))
}

/// Reject a degenerate clock rate at policy-install time: a zero denominator is a divide-by-zero in
/// the scaling and a zero numerator is a frozen clock (time never advances for that node) — neither is
/// a meaningful drift, and both would corrupt or panic the scheduler. The per-protocol drift ENVELOPE
/// (e.g. LeaseGuard's `±ε/Δ`) is the caller's contract; this enforces only the non-degeneracy the
/// scheduler arithmetic itself requires, so an invalid policy fails loudly at install, not mid-run.
fn validate_rate((num, den): (u64, u64)) -> (u64, u64) {
  assert!(
    num > 0 && den > 0,
    "clock-drift rate must have a positive numerator and denominator, got ({num}, {den})"
  );
  (num, den)
}

impl Cluster {
  /// Node `i`'s LOCAL clock reading at the current global virtual time: `floor(now · num/den)` for the
  /// node's rate. With the default `(1, 1)` rate this is exactly `self.now`, so every node-facing call
  /// is byte-identical to the original single-clock cluster. Under a [`set_clock_drift`] policy a fast
  /// node (`num > den`) reads ahead and a slow node behind — the per-node `now` every `handle_*` /
  /// `read_index` / `propose` / restart sees, so the proto stamps and ages entries on each node's OWN
  /// drifting clock.
  fn now_for(&self, i: usize) -> Instant {
    let (num, den) = self.clock_rate[i];
    if num == den {
      return self.now; // exact identity — no rounding, byte-identical to the single-clock path.
    }
    Instant::from_origin(scale_floor(self.now.since_origin(), num, den))
  }

  /// The full clock reading the driver hands node `i` on every call: the monotonic
  /// [`now_for`](Self::now_for) instant ALWAYS, plus — under the failover tier — a SYNCHRONIZED wall
  /// reading `global_now + offset[i]` (saturating ≥ 0). Off-tier (`failover == false`) this is the bare
  /// monotonic `Now` (wall absent), byte-identical to the original single-clock path. The wall carries
  /// the per-node [`clock_offset`](Self::clock_offset) but NOT the monotonic [`clock_rate`](Self::clock_rate):
  /// the two clock kinds are perturbed independently, matching the design's distinct ε_unc / ε_drift.
  fn now_now(&self, i: usize) -> Now {
    let mono = self.now_for(i);
    if !self.failover {
      return Now::monotonic(mono);
    }
    let wall = project_wall(self.now.since_origin().as_nanos(), self.clock_offset[i]);
    Now::synchronized(mono, Wall::from_nanos(wall))
  }

  /// The earliest GLOBAL virtual time at which node `i`'s LOCAL deadline `local` is reached, i.e. the
  /// inverse of [`now_for`]: `ceil(local · den/num)`. Used to fold each node's `poll_timeout()` (which
  /// the node expressed on its own local clock) back onto the shared global timeline so the
  /// discrete-event scheduler advances to the correct next wake-up. Exact: `now_for(i)` at this instant
  /// is `>= local`, and no earlier global instant qualifies.
  fn global_of(&self, i: usize, local: Instant) -> Instant {
    let (num, den) = self.clock_rate[i];
    if num == den {
      return local;
    }
    Instant::from_origin(scale_ceil(local.since_origin(), den, num))
  }

  /// Node `i`'s next timer deadline expressed in GLOBAL time (its local `poll_timeout()` folded through
  /// [`global_of`]). `None` when the node has no armed timer.
  fn global_timeout(&self, i: usize) -> Option<Instant> {
    self.nodes[i].poll_timeout().map(|d| self.global_of(i, d))
  }

  /// Is the node at Vec position `server` a SUPERSEDED leader right now — still in Leader role, but
  /// outranked by ANOTHER live node in Leader role at a strictly higher term? Removed harness artifacts
  /// are excluded on BOTH sides: a node `mark_removed`'d but frozen in Leader role is neither a valid
  /// superseded server nor a valid superseding higher-term leader (it is no longer a protocol
  /// participant). Counted ONLY where it is genuinely serve-time — immediately after the `read_index`
  /// call that produced a LeaseGuard immediate serve, before any tick can advance the cluster (see
  /// [`drain_events`]). That captures the cross-leader case LeaseGuard governs: a leader serving from
  /// its lease while a fresh higher-term leader has already taken over.
  fn serve_was_superseded(&self, server: usize) -> bool {
    if self.removed.contains(&self.node_ids[server]) || !self.nodes[server].role().is_leader() {
      return false;
    }
    let server_term = self.nodes[server].term();
    self.node_ids.iter().enumerate().any(|(j, id)| {
      j != server
        && !self.removed.contains(id)
        && self.nodes[j].role().is_leader()
        && self.nodes[j].term() > server_term
    })
  }

  /// Drain node `i`'s event queue: bump the snapshot/conf-change tallies and append every `ReadState`
  /// to its confirmed-reads history. Returns whether anything was drained (so the tick loop can mark
  /// progress). The cross-leader non-vacuity counter is driven by the SERVE-TIME context set, not by the
  /// cluster state at this (later, possibly drifted) drain: a `ReadState` whose context was recorded at
  /// `read_index` time as served by a superseded leader (see [`note_read_issue`]) bumps the counter and
  /// retires its context. Draining itself is unchanged from the original single-clock cluster, so it has
  /// no effect on the run — only the bookkeeping is new.
  fn drain_events(&mut self, i: usize) -> bool {
    let mut drained = false;
    while let Some(ev) = self.nodes[i].poll_event() {
      drained = true;
      if ev.is_snapshot_installed() {
        self.snapshot_installs[i] += 1;
      }
      if ev.is_conf_changed() {
        self.conf_changed[i] += 1;
      }
      if let sailing_proto::Event::ReadState(rs) = ev {
        if self.superseded_read_contexts.remove(rs.context().as_ref()) {
          self.lease_superseded_serves += 1;
        }
        self.read_states[i].push(rs);
      }
    }
    drained
  }

  /// Record, at SERVE time, that a read with `context` was just issued on node `i` and that node is a
  /// superseded leader RIGHT NOW. For a LeaseGuard immediate serve the `read_index` call already
  /// produced the `ReadState` synchronously, so this is the true serve-time classification; the matching
  /// `ReadState` is counted when it later drains (see [`drain_events`]). Only `accepted` reads are
  /// recorded. A read that is accepted but does NOT serve immediately (a superseded leader whose lease
  /// expired degrades to a quorum round it can never complete while outranked) emits no `ReadState`, so
  /// its unique context simply never matches — no over-count. Recording only TOUCHES a private set the
  /// VOPR never observes, so the run is unchanged.
  fn note_read_issue(&mut self, i: usize, context: &[u8], accepted: bool) {
    if accepted && self.serve_was_superseded(i) {
      self.superseded_read_contexts.insert(context.to_vec());
    }
  }

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
      let now_i = self.now_now(i);
      let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
      self.nodes[i].handle_storage(now_i, log, stable);
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
      self.drain_events(i);
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

    // Structural assertion (a): append-before-ack
    // A success AppendResponse must not outrun the node's readable log. (The proto's append-before-ack
    // ordering — deferring a NEW suffix's ack to its durability via `on_log_appended` — is exercised
    // by the fsync-window integration test; this send-time tripwire is a coarse outran-the-log
    // guard. It uses the VISIBLE `last_index()` so it stays byte-identical to the original in sync mode and
    // does not flag the legitimate "duplicate AppendEntries, entries already present" ack path that
    // can fire for a visible-but-in-flight suffix. The per-entry quorum-durability of every COMMITTED
    // index is enforced separately by the `commit_is_quorum_durable` oracle on the durable snapshot.)
    if let Message::AppendResponse(a) = &message
      && !a.reject()
    {
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
    // Structural assertion (b): one-grant-per-(node,term)
    // A success VoteResponse from `from` in term `T` to candidate `to` must not appear a second time
    // for a different candidate — that would be a double-vote. Holds under reorder+dup: a duplicate
    // grant to the SAME candidate is fine; a grant to a DIFFERENT one in the same term is a bug.
    if let Message::VoteResponse(vr) = &message {
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

    if self
      .net_prng
      .chance_per_mille(self.net_faults.drop_per_mille)
    {
      self.net_dropped += 1;
      return false; // lost in flight
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
          let now_to = self.now_now(to_idx);
          let (log, stable) = (&mut self.logs[to_idx], &mut self.stables[to_idx]);
          let message = wire_roundtrip(m.message);
          self.nodes[to_idx].handle_message(now_to, log, stable, m.from, message);
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

    // Step a+b: advance clock and fire timers. Each node's `poll_timeout()` is expressed on its OWN
    // (possibly drifting) local clock; `global_timeout` folds it onto the shared global timeline so the
    // discrete-event scheduler advances to the correct real wake-up, and a node fires exactly when
    // global time reaches that folded deadline. With no drift this is byte-identical to the original
    // `poll_timeout().min()` / `poll_timeout() <= now` form.
    let next_timer = (0..self.nodes.len())
      .filter_map(|i| self.global_timeout(i))
      .min();
    let next_msg = self.bus.iter().map(|m| m.deliver_at).min();
    if let Some(target) = [next_timer, next_msg].into_iter().flatten().min() {
      if target > self.now {
        self.now = target;
        progressed = true;
      }
      for i in 0..self.nodes.len() {
        if self.global_timeout(i).is_some_and(|d| d <= self.now) {
          progressed = true;
          let now_i = self.now_now(i);
          let (log, stable) = (&mut self.logs[i], &mut self.stables[i]);
          self.nodes[i].handle_timeout(now_i, log, stable);
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
        if self.drain_events(i) {
          progressed = true;
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
  fn project_wall_saturates_both_ends() {
    // Interior: exact base + offset.
    assert_eq!(project_wall(1_000, 250), 1_250);
    assert_eq!(project_wall(1_000, -250), 750);
    // Lower end: a negative sum clamps to 0, never an underflow.
    assert_eq!(project_wall(10, -25), 0);
    assert_eq!(project_wall(0, i64::MIN), 0);
    // Upper end: a sum past u64::MAX clamps to u64::MAX, never a WRAP to a small value.
    assert_eq!(project_wall(u64::MAX as u128, 1), u64::MAX);
    assert_eq!(project_wall(u64::MAX as u128, i64::MAX), u64::MAX);
    assert_eq!(project_wall(u64::MAX as u128 - 5, 100), u64::MAX);
    // Exactly at the boundary stays exact.
    assert_eq!(project_wall(u64::MAX as u128 - 5, 5), u64::MAX);
  }

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
  fn clock_drift_off_is_byte_identical_to_baseline() {
    // The byte-identity floor of the drift machinery: a no-drift policy (every node `(1, 1)`) makes
    // `now_for` the exact identity, so every node-facing call sees the same global `now` it always
    // did — the run must be bit-for-bit the baseline single-clock cluster.
    let baseline = {
      let mut c = Cluster::new(3);
      drive_and_capture(&mut c, 8)
    };
    let with_off_drift = {
      let mut c = Cluster::new(3);
      c.set_clock_drift(|_| (1, 1));
      drive_and_capture(&mut c, 8)
    };
    assert_eq!(
      baseline, with_off_drift,
      "a no-drift (1/1) clock policy must be byte-identical to the single-clock cluster"
    );
  }

  #[test]
  fn clock_drift_is_deterministic() {
    // Same drift policy ⇒ identical run. Bounded per-node rates (node 0 slowest, node 1 fastest,
    // within the ±ε/Δ = ±1/6 band for the 50ms/300ms LeaseGuard config) must still drive a
    // reproducible, convergent run.
    let run = || -> Vec<AppliedLog> {
      let mut c = Cluster::new(3);
      c.set_clock_drift(|id| match id {
        0 => (5, 6), // slowest valid rate (Δ−ε)/Δ
        1 => (7, 6), // fastest valid rate (Δ+ε)/Δ
        _ => (1, 1),
      });
      drive_and_capture(&mut c, 8)
    };
    assert_eq!(run(), run(), "same drift policy ⇒ identical run");
  }

  #[test]
  fn clock_drift_diverges_node_clocks_but_scheduler_holds() {
    // The core invariant: differing per-node clock RATES, one consistent global timeline. Each node's
    // local clock advances at its own rate (so the readings genuinely diverge), yet the discrete-event
    // scheduler — folding each node's local deadline back to global time via `global_of` — still drives
    // the cluster to a leader and commits a batch.
    let mut c = Cluster::new(3);
    c.set_clock_drift(|id| match id {
      0 => (5, 6),
      1 => (7, 6),
      _ => (1, 1),
    });
    assert!(
      c.run_until(400, |c| c.leader_count() == 1),
      "a drifted cluster must still elect a single leader"
    );
    for i in 0..8u32 {
      c.run_until(100, |c| c.leader_count() == 1);
      c.propose(&i.to_le_bytes());
      c.run_until(80, |_| false);
    }
    assert!(
      c.run_until(800, |c| c.agreement_holds() && c.min_applied_len() >= 8),
      "a drifted cluster must still commit + apply the batch consistently"
    );
    // The clocks have genuinely diverged: at a positive global time the fast node (7/6) reads strictly
    // ahead of the slow node (5/6). (Index == id for the three founders.)
    let slow = c.now_for(0).since_origin();
    let fast = c.now_for(1).since_origin();
    assert!(
      fast > slow,
      "the fast node (7/6) must read ahead of the slow node (5/6): fast={fast:?} slow={slow:?}"
    );
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
