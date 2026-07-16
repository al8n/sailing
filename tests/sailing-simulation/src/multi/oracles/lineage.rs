//! The per-`(node, gid)` LINEAGE LEDGER — the mechanical catch for the "a coordinate `(index,
//! term)` treated as proof of content across fork lineages" family.
//!
//! A fork child boots from a MANUFACTURED snapshot at `(1, 1)` carrying a `ForkId` provenance
//! token; a token-less group carries none. `(last_index, last_term, conf)` is a content-identity
//! only WITHIN one lineage (Log Matching), so every downstream coordinate proof — the redundancy
//! short-circuit, the reclaim proof, the progress-ack watermark, restart's log-vs-snapshot
//! reconciliation — is sound only when a replica's durable state stays SINGLE-LINEAGE. The door
//! gate (`endpoint/snapshot.rs`) holds that at the wire: a token-bearing snapshot lands only on a
//! replica with no committed content, or one already bearing the same token. This ledger holds the
//! SAME invariant as an observer — it never gates a message, it watches applied commands, installed
//! snapshots, and endpoint `fork_id` transitions, and attributes every one to a lineage:
//!
//!   - CHIMERA: a snapshot install of lineage `B` onto a replica that already holds committed
//!     content of a DIFFERENT lineage `A` — the exact cross-lineage fusion the gate refuses. A
//!     replica adopts a lineage only from nothing (pristine) or its own (kin retransfer).
//!   - PHANTOM-QUORUM: two replicas that agree on `(gid, lineage, index)` hold IDENTICAL command
//!     bytes. The lineage key is what keeps a legitimate content-reset (a fork boundary re-using an
//!     index across lineages) from reading as a divergence, while a genuine within-lineage split
//!     brain still trips.
//!   - WEDGE: an ADMITTED transfer (the leader drove a peer into `Snapshot` progress) terminates —
//!     install, chunk/redundancy progress, or refusal — within a bounded number of cranks. A
//!     transfer that neither progresses nor is refused, while the peer is reachable, is a wedge.
//!
//! The ledger is a PURE observer: it never draws from a PRNG and never mutates the simulated
//! nodes/stores, so the run is byte-identical with or without it. Verdicts accumulate and the run
//! ends with [`LineageLedger::finalize_or_panic`]; the world runs it beside the membership and
//! conservation finalizers, so every existing scenario+seed feeds it.

use sailing_proto::ForkId;
use std::{
  collections::{BTreeMap, btree_map::Entry},
  string::String,
  vec::Vec,
};

/// The interned lineage of a replica: `None` — the token-less lineage (a group's own writes); a
/// `Some(i)` interning index into [`LineageLedger::forks`] — a specific fork provenance. `ForkId`
/// is `Eq` but neither `Ord` nor `Hash`, so it is interned to a small integer usable as a map key.
type Lineage = Option<usize>;

/// One replica incarnation, the observers' identity key: `(node, gid, generation)`.
pub(crate) type ReplicaKey = (u64, u64, u64);

/// The phantom-quorum content key: `(gid, generation, lineage, index)` — the coordinate a
/// within-lineage quorum must agree on byte-for-byte.
type ContentKey = (u64, u64, Lineage, u64);

/// A replica's observed lineage state within ONE incarnation (`(node, gid, generation)`): the
/// lineage its durable content currently belongs to, and whether it has EVER held committed
/// content this incarnation (the door gate's "populated" predicate).
#[derive(Clone)]
struct ReplicaLineage {
  lineage: Lineage,
  has_committed: bool,
}

/// One tracked in-flight transfer's liveness cursor, keyed `(gid, peer)`. `first_tick` is when the
/// current NO-PROGRESS window opened; it resets on any progress (match advance, chunk-cursor
/// advance, or a refusal-count climb).
#[derive(Clone)]
struct Transfer {
  first_tick: u64,
  last_match: u64,
  last_acked_through: u64,
  last_refused: u64,
}

/// How many cranks an admitted transfer may make NO progress (no chunk staged, no install, no
/// refusal) while the peer is reachable before it is a wedge. Generous by design: a legitimate
/// transfer in the world resolves in a handful of cranks (the FSM snapshot is a few cells), and a
/// reachable-but-stalled peer is cleared each crank the world reports it isolated/muted, so only a
/// genuinely stuck admitted transfer accumulates this many consecutive no-progress cranks.
const WEDGE_BUDGET: u64 = 2_000;

/// The per-`(node, gid)` lineage ledger. See the module docs.
pub(crate) struct LineageLedger {
  /// Distinct fork tokens seen, for interning (`ForkId` is `Eq` but not `Ord`/`Hash`).
  forks: Vec<ForkId>,
  /// Per [`ReplicaKey`] observed lineage state.
  replicas: BTreeMap<ReplicaKey, ReplicaLineage>,
  /// The phantom-quorum content map: [`ContentKey`] → the first replica's `(node, command
  /// bytes)`. A later replica reporting DIFFERENT bytes at the same key is a within-lineage
  /// split brain.
  content: BTreeMap<ContentKey, (u64, Vec<u8>)>,
  /// The wedge cursor per tracked `(gid, peer)` transfer.
  transfers: BTreeMap<(u64, u64), Transfer>,
  /// Accumulated CHIMERA verdicts (a cross-lineage install onto committed content).
  chimeras: Vec<String>,
  /// Accumulated PHANTOM-QUORUM verdicts.
  phantoms: Vec<String>,
  /// Accumulated WEDGE verdicts.
  wedges: Vec<String>,
  /// Non-vacuity: applied cells fed to the phantom-quorum map.
  cells_judged: u64,
  /// Non-vacuity: snapshot installs the chimera detector examined.
  installs_observed: u64,
}

impl LineageLedger {
  /// A fresh, empty ledger.
  pub(crate) fn new() -> Self {
    Self {
      forks: Vec::new(),
      replicas: BTreeMap::new(),
      content: BTreeMap::new(),
      transfers: BTreeMap::new(),
      chimeras: Vec::new(),
      phantoms: Vec::new(),
      wedges: Vec::new(),
      cells_judged: 0,
      installs_observed: 0,
    }
  }

  /// Intern a lineage token to its small-integer key. `None` (token-less) stays `None`.
  fn intern(&mut self, token: Option<&ForkId>) -> Lineage {
    token.map(|f| {
      self
        .forks
        .iter()
        .position(|seen| seen == f)
        .unwrap_or_else(|| {
          self.forks.push(f.clone());
          self.forks.len() - 1
        })
    })
  }

  /// Observe one replica's current committed content and lineage — its endpoint `fork_id` and the
  /// `(index, command)` cells it has applied. `replica` is the incarnation's identity
  /// `(node, gid, generation)`. Feeds the phantom-quorum content map and records the replica's
  /// live lineage so a later install's cross-lineage landing is detectable. Cheap
  /// re-presentation: a cell already recorded under an identical `(gid, gen, lineage, index)` key
  /// is a harmless re-observe; a DIFFERENT byte string at that key is a phantom quorum.
  pub(crate) fn observe_content(
    &mut self,
    seed: u64,
    tick: u64,
    replica: ReplicaKey,
    lineage: Option<&ForkId>,
    applied: &[(u64, Vec<u8>)],
  ) {
    let (node, gid, generation) = replica;
    let key = self.intern(lineage);
    let mut had_cells = false;
    for (index, cmd) in applied {
      had_cells = true;
      self.cells_judged += 1;
      match self.content.entry((gid, generation, key, *index)) {
        Entry::Vacant(v) => {
          v.insert((node, cmd.clone()));
        }
        Entry::Occupied(o) => {
          let (first, bytes) = o.get();
          if bytes != cmd {
            self.phantoms.push(std::format!(
              "phantom quorum: group {gid} (gen {generation}) index {index} holds DIVERGENT \
               committed bytes within ONE lineage — node {first} and node {node} agree on \
               (gid, lineage, index) but their command bytes differ\n  seed={seed} tick={tick}"
            ));
          }
        }
      }
    }
    let rl = self
      .replicas
      .entry((node, gid, generation))
      .or_insert(ReplicaLineage {
        lineage: key,
        has_committed: false,
      });
    if had_cells {
      rl.has_committed = true;
      rl.lineage = key;
    }
  }

  /// Observe a snapshot INSTALL on a replica — its `SnapshotMeta` lineage token and boundary.
  /// `replica` is the incarnation's identity `(node, gid, generation)`. This is the chimera
  /// decision point: an install of lineage `B` onto a replica that already holds committed
  /// content of a DIFFERENT lineage `A` is the cross-lineage fusion the door gate refuses (a
  /// lineage is adopted only from nothing, or re-installed from its own). The install then
  /// re-bases the replica onto the installed lineage.
  pub(crate) fn observe_install(
    &mut self,
    seed: u64,
    tick: u64,
    replica: ReplicaKey,
    install_lineage: Option<&ForkId>,
    boundary: u64,
  ) {
    let (node, gid, generation) = replica;
    let key = self.intern(install_lineage);
    self.installs_observed += 1;
    let rl = self
      .replicas
      .entry((node, gid, generation))
      .or_insert(ReplicaLineage {
        lineage: key,
        has_committed: false,
      });
    if rl.has_committed && rl.lineage != key {
      self.chimeras.push(std::format!(
        "chimera: group {gid} (gen {generation}) on node {node} held committed content of \
         lineage {:?}, then installed a snapshot of lineage {:?} at boundary {boundary} — a \
         coordinate proof fused across two fork lineages in one durable state\n  seed={seed} \
         tick={tick}",
        rl.lineage,
        key,
      ));
    }
    // The install re-bases the durable content onto its own lineage (pristine adoption, kin
    // retransfer, or — under a broken gate — the destructive cross-lineage replacement above).
    rl.lineage = key;
    rl.has_committed = true;
  }

  /// Observe the wedge cursor for a transfer the leader is driving to `peer` under `gid`.
  /// `in_flight` is `Some((match_index, acked_through))` when the leader's progress to `peer` is
  /// `Snapshot` AND `peer` is reachable, else `None` (no admitted transfer, or the peer is
  /// unreachable — a partition is not a wedge). `refused` is the peer's cross-lineage refusal
  /// count. Any of a match advance, a chunk-cursor advance, or a refusal climb is progress and
  /// re-opens the window.
  pub(crate) fn observe_transfer(
    &mut self,
    seed: u64,
    tick: u64,
    gid: u64,
    peer: u64,
    in_flight: Option<(u64, u64)>,
    refused: u64,
  ) {
    match in_flight {
      None => {
        self.transfers.remove(&(gid, peer));
      }
      Some((match_index, acked_through)) => match self.transfers.entry((gid, peer)) {
        Entry::Vacant(v) => {
          v.insert(Transfer {
            first_tick: tick,
            last_match: match_index,
            last_acked_through: acked_through,
            last_refused: refused,
          });
        }
        Entry::Occupied(mut o) => {
          let t = o.get_mut();
          let progressed = match_index > t.last_match
            || acked_through > t.last_acked_through
            || refused > t.last_refused;
          if progressed {
            t.first_tick = tick;
            t.last_match = match_index;
            t.last_acked_through = acked_through;
            t.last_refused = refused;
          } else if tick.saturating_sub(t.first_tick) > WEDGE_BUDGET {
            let stuck = tick.saturating_sub(t.first_tick);
            self.wedges.push(std::format!(
              "wedge: group {gid} leader held peer {peer} in Snapshot transfer for {stuck} \
               cranks with no chunk progress, no install, and no refusal — an admitted transfer \
               that neither progressed nor terminated\n  seed={seed} tick={tick}"
            ));
            // Re-open the window so one wedge is one verdict, not one per crank thereafter.
            t.first_tick = tick;
          }
        }
      },
    }
  }

  /// Applied cells fed to the phantom-quorum map — the phantom detector's non-vacuity witness.
  pub(crate) fn cells_judged(&self) -> u64 {
    self.cells_judged
  }

  /// Snapshot installs the chimera detector examined — its non-vacuity witness.
  pub(crate) fn installs_observed(&self) -> u64 {
    self.installs_observed
  }

  /// The run-end verdict: panic on the first accumulated CHIMERA, PHANTOM-QUORUM, or WEDGE, with
  /// the seed and tick for replay. A clean return means the durable state stayed single-lineage,
  /// every within-lineage quorum agreed byte-for-byte, and every admitted transfer terminated.
  pub(crate) fn finalize_or_panic(&self, seed: u64) {
    if let Some(v) = self
      .chimeras
      .iter()
      .chain(&self.phantoms)
      .chain(&self.wedges)
      .next()
    {
      panic!(
        "LINEAGE LEDGER VIOLATION: {v}\n  (replay: run_multi_vopr for seed {seed} and inspect \
         the reported tick)"
      );
    }
  }
}

#[cfg(test)]
mod tests;
