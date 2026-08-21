//! The per-group read-linearizability ledger — the single-group `ReadLedger` lifted to
//! `(group, key)` grain over gid-tagged payloads.
//!
//! Every accepted read records, at INVOCATION, the group's completed-write index floor (max
//! commit anywhere in the group) and the per-`(group, key)` committed value floor; every observed
//! confirmation must satisfy `index >= floor`, and once the serving replica has APPLIED to the
//! read's index the served value must be `>= v_inv`. An accepted read that never confirms is
//! legal under faults; never-confirmed beats wrongly confirmed, and a confirmed read whose key
//! changed hands between invocation and serve is retired unjudged for the same reason (the
//! serve-point drain's tenure guard).
//!
//! # Reconstructing a value under mixed index spaces
//!
//! An applied record MIXES index spaces once reshape has touched it: `LogSm::absorb` grafts the
//! absorbed source's cells KEEPING their source-log indices, and a fork's manufactured baseline
//! keeps the parent's (both are load-bearing contracts of the conservation walk). So the
//! index-bounded scan below ([`value_of_asof`]) can only ELIGIBILITY-filter on the group's NATIVE
//! cells — a folded-in cell's index names a foreign space and is neither above nor below the
//! group's own read index in any meaningful sense (which is also why that scan picks the max
//! VALUE among eligible cells rather than the value at the greatest index: index order ranks
//! nothing across two spaces, while the monotone value counter ranks every write). Folded content
//! is therefore carried by a second leg, the
//! per-group fold BASELINES ([`MultiWorld::fold_baseline_of`]): the value of each folded-in key
//! pinned at the fold's coordinate in THIS group's index space. Both legs feed both sides of the
//! assertion, and a fold's coordinate is always at or below the group's committed watermark at
//! the moment it is recorded (the resolving replica applied the merge entry there), hence at or
//! below the index floor of any read invoked after it — so the invocation side's unbounded view
//! of the baselines never outruns the bounded serve side's.
//!
//! A folded cell whose FOREIGN index happens to fall at or below a read's index is eligible on
//! the as-of leg, so its value can lift `observed` and mask a genuinely stale read. That only
//! matters while the cell's fold is NOT yet visible at the read's index: once the fold coordinate
//! is itself in range the baseline already carries the value, and nothing new is admitted. So the
//! oracle does not judge those checks at all — the serve-point drain retires any deferred check
//! whose read index predates a fold touching its key, the same never-judged-beats-wrongly-judged
//! rule the neighboring guards follow. What that costs is coverage, not soundness: reads that
//! straddle a later fold of their key go unjudged, rather than being judged against a splice the
//! index-bounded scan cannot classify. Classifying each cell exactly instead would need per-node
//! splice ranges, which a snapshot restore flattens away.

use super::*;

/// What one accepted read recorded at invocation.
#[derive(Debug, Clone, Copy)]
struct GInvocation {
  /// The group the read targets.
  gid: u64,
  /// The group's completed-write index floor at invocation.
  floor: sailing_proto::Index,
  /// The read's key.
  key: u16,
  /// The per-`(group, key)` committed VALUE floor at invocation (0 if never written).
  v_inv: u64,
  /// The group's OWNERSHIP EPOCH for `key` at invocation — the tenure this read describes.
  epoch: u64,
  /// The group's incarnation generation at invocation — the epoch counter's outer leg, since
  /// recreation restarts both the population and the counters.
  generation: u64,
}

/// A confirmed read awaiting its per-key VALUE assertion at the replica's serve point.
#[derive(Debug, Clone, Copy)]
struct PendingValueCheck {
  ctx: u64,
  node: u64,
  index: sailing_proto::Index,
  inv: GInvocation,
}

/// The multi-group read ledger (contexts minted globally; confirmations scanned per replica).
pub(super) struct MultiReadLedger {
  /// Monotone context mint (8-byte BE on the wire); never reused, even for refused issues.
  next_ctx: u64,
  /// Accepted, unconfirmed reads: context → invocation snapshot.
  inflight: BTreeMap<u64, GInvocation>,
  /// Per-`(node, gid)` scan offset into the world's monotone confirmation history.
  scan_off: BTreeMap<(u64, u64), usize>,
  /// Retired reads: context → confirming `(node, index)` (the duplicate-confirmation oracle).
  confirmed: BTreeMap<u64, (u64, sailing_proto::Index)>,
  /// Confirmed reads whose VALUE check waits for the serving replica to apply far enough.
  pending_value: Vec<PendingValueCheck>,
}

impl MultiReadLedger {
  pub(super) fn new() -> Self {
    Self {
      next_ctx: 0,
      inflight: BTreeMap::new(),
      scan_off: BTreeMap::new(),
      confirmed: BTreeMap::new(),
      pending_value: Vec::new(),
    }
  }

  /// Issue one read on `(target, gid)` for `key`, recording the invocation snapshot iff the
  /// replica accepts. The per-key value floor scans only the group's AUTHORITATIVE replicas —
  /// non-parked and caught up to the applied frontier ([`authoritative_nodes`]) — and reads their
  /// split-aware `applied()` record alone. The raw committed log is NOT a floor source: it is
  /// reshape-BLIND (`LogSm::split`/`absorb` rewrite `applied()` only), so an uncompacted pre-split
  /// entry there resurrects a value a later split moved away or a merge-back reset, lifting the
  /// floor above the live committed value the serve side (also `applied()`) can ever return — the
  /// exact inflation that let a parked, far-behind replica's stale log fail a correct read. A
  /// caught-up replica has already applied the group's committed prefix, so `applied()` alone is
  /// its complete committed value; the read's own index floor plus the serve-point apply wait
  /// carry any committed-not-yet-applied write the raw-log leg once covered.
  ///
  /// The floor's second leg is the group's fold BASELINES (see the [module docs]): content this
  /// group absorbed or inherited is committed here too, but no replica's record can place it by
  /// index or reach it by tag. Unbounded on this side, matching the record scan — every recorded
  /// fold sits at or below the group's committed watermark, so the serve side's index-bounded
  /// read of the same baselines covers whatever this one saw.
  ///
  /// [`authoritative_nodes`]: MultiWorld::authoritative_nodes
  /// [module docs]: self
  pub(super) fn issue(
    &mut self,
    w: &mut MultiWorld,
    gid: u64,
    target: u64,
    key: u16,
    report: &mut MultiVoprReport,
  ) {
    let ctx = self.next_ctx;
    self.next_ctx += 1;
    let floor = w.max_commit_of(gid);
    let v_inv = w
      .authoritative_nodes(gid)
      .into_iter()
      .filter_map(|n| value_of(&w.applied_of(n, gid), gid, key))
      .max()
      .unwrap_or(0)
      .max(w.fold_baseline_of(gid, key, u64::MAX));
    // Everything above reads ONE instant of the world, so the floor is internally coherent; the
    // tenure stamp is what lets the DEFERRED serve-point check tell whether the record it will be
    // judged against is still the one this instant described.
    let epoch = w.key_epoch_of(gid, key);
    let generation = w.generation_of(gid);
    if w.read_index_on(target, gid, &ctx.to_be_bytes()) {
      self.inflight.insert(
        ctx,
        GInvocation {
          gid,
          floor,
          key,
          v_inv,
          epoch,
          generation,
        },
      );
      report.reads_issued += 1;
    }
  }

  /// Scan every replica's newly-confirmed `ReadState`s and run the linearizability assertions.
  /// Panics (with seed + tick) on a violation, an unknown context, or a duplicate confirmation.
  pub(super) fn scan(&mut self, w: &MultiWorld, report: &mut MultiVoprReport, seed: u64) {
    for (node, gid) in w.read_state_keys() {
      let states = w.read_states_of(node, gid);
      let off = self.scan_off.entry((node, gid)).or_insert(0);
      while *off < states.len() {
        let rs = &states[*off];
        *off += 1;
        let raw: [u8; 8] = rs.context().as_ref().try_into().unwrap_or_else(|_| {
          panic!(
            "[multi read-linearizability] non-fuzzer context {:?} confirmed on (n{node}, g{gid}) \
             — the fuzzer mints every context\n  seed={seed} tick={}",
            rs.context(),
            w.ticks(),
          )
        });
        let ctx = u64::from_be_bytes(raw);
        match self.inflight.remove(&ctx) {
          Some(inv) => {
            assert_eq!(
              inv.gid,
              gid,
              "[multi read-linearizability] read ctx={ctx} issued on group {} confirmed under \
               group {gid} — a cross-group confirmation leak\n  seed={seed} tick={}",
              inv.gid,
              w.ticks(),
            );
            assert!(
              rs.index() >= inv.floor,
              "[multi read-linearizability] read ctx={ctx} on (n{node}, g{gid}) confirmed at \
               index {} BELOW the completed-write floor {} recorded at invocation\n  seed={seed} \
               tick={} (replay: run_multi_vopr({seed}, ticks, profile))",
              rs.index().get(),
              inv.floor.get(),
              w.ticks(),
            );
            self.pending_value.push(PendingValueCheck {
              ctx,
              node,
              index: rs.index(),
              inv,
            });
            self.confirmed.insert(ctx, (node, rs.index()));
            report.reads_confirmed += 1;
          }
          None => {
            let dup = self.confirmed.contains_key(&ctx);
            panic!(
              "[multi read-linearizability] {} for read ctx={ctx} on (n{node}, g{gid}) (index \
               {})\n  seed={seed} tick={}",
              if dup {
                "DUPLICATE confirmation — one read context confirmed twice"
              } else {
                "confirmation for a context the fuzzer never accepted"
              },
              rs.index().get(),
              w.ticks(),
            );
          }
        }
      }
    }

    // Drain the deferred VALUE checks at each read's true serve point (the replica has applied
    // to the read index). A replica retired mid-serve is unjudgeable — its pending check drops.
    let mut still_pending = Vec::with_capacity(self.pending_value.len());
    for p in core::mem::take(&mut self.pending_value) {
      if !w.hosts_group(p.node, p.inv.gid) {
        continue; // the replica was torn down before serving — never-served, not wrong
      }
      if !w.group_holds_key(p.inv.gid, p.inv.key) {
        // The key was handed to a child between invocation and serve: the invoked group's
        // applied record legitimately no longer carries it (the split moved the cells), so the
        // serve point is unjudgeable HERE — the retired-replica rule's key-grained twin. The
        // handover itself is the conservation ledger's to judge.
        continue;
      }
      if w.key_epoch_of(p.inv.gid, p.inv.key) != p.inv.epoch
        || w.generation_of(p.inv.gid) != p.inv.generation
      {
        // The same rule one step further: CURRENT ownership cannot distinguish held from
        // held-AGAIN. A key split away and then reacquired — an older source folding in, or the
        // child merging back — passes the check above while the record underneath was replaced
        // wholesale. Judging across that discontinuity is wrong in BOTH directions: a correct
        // pre-split read fails against a reacquired-lower anchor, and a genuinely stale serve is
        // blessed by same-tag cells the merge-back restored at their preserved indices. Retire
        // it — never-judged beats wrongly-judged, the rule the two drops above already follow.
        // (Only the deferred value check crosses time this way. The invocation floor reads one
        // instant, and the confirmation-side index assert is immune: contexts are globally
        // unique, so no endpoint can confirm a context minted for a tenure it never served.)
        continue;
      }
      if w.fold_after(p.inv.gid, p.inv.key, p.index.get()) {
        // A fold at or below this read's index is fully CLASSIFIED — the baseline carries its
        // content at that coordinate. A fold ABOVE it is not: it spliced cells whose preserved
        // foreign indices say nothing about this coordinate, and SAME-TAG ones can sit below it,
        // where the as-of scan admits them and inflates `observed` — blessing a serve that is
        // genuinely stale. Retire, under the same rule as the drops above. The guard keys on the
        // fold's COORDINATE, not on folds as such: a read at or past one stays judged. And it
        // only ever sees SAME-TENURE folds — a split takes its key's fold entries with it, and a
        // reacquisition is a new epoch the guard above already retired.
        continue;
      }
      if w.applied_index_of(p.node, p.inv.gid) < p.index {
        still_pending.push(p);
        continue;
      }
      // Both legs of the reconstruction (see the module docs): the native cells the read index
      // can order, and the folded-in content anchored at fold coordinates the read index has
      // reached.
      let observed = value_of_asof(
        &w.applied_of(p.node, p.inv.gid),
        p.inv.gid,
        p.inv.key,
        p.index.get(),
      )
      .unwrap_or(0)
      .max(w.fold_baseline_of(p.inv.gid, p.inv.key, p.index.get()));
      assert!(
        observed >= p.inv.v_inv,
        "[multi read-value-linearizability] read ctx={} key={} served on (n{}, g{}) at index {} \
         showed value {observed} BELOW the committed value {} at invocation — a stale per-key \
         read\n  seed={seed} tick={}",
        p.ctx,
        p.inv.key,
        p.node,
        p.inv.gid,
        p.index.get(),
        p.inv.v_inv,
        w.ticks(),
      );
      report.reads_value_checked += 1;
    }
    self.pending_value = still_pending;
  }
}

/// The LATEST gid-tagged value of `(gid, key)` among the entries at log index `<= upto` over an
/// applied/committed `(index, command)` sequence; `None` when none is eligible.
///
/// Latest is selected by VALUE, not by index: per-key values ride one global monotone counter, so
/// the max value IS the latest write, while index order carries no such meaning across the mixed
/// index spaces a reshaped record holds (see the module docs). Selecting by index would let an
/// old same-tag cell absorbed at a large FOREIGN index shadow a newer native write at a small
/// current-space one — under-reporting the committed value on both legs, which blesses a serve
/// that is genuinely stale against the newer write.
fn value_of_asof(entries: &[(u64, Vec<u8>)], gid: u64, key: u16, upto: u64) -> Option<u64> {
  entries
    .iter()
    .filter(|(idx, _)| *idx <= upto)
    .filter_map(|(_, cmd)| decode_gkv(cmd))
    .filter(|&(tag, k, _)| tag == gid && k == key)
    .map(|(_, _, value)| value)
    .max()
}

/// The current value of `(gid, key)` over an entire sequence (no index bound).
fn value_of(entries: &[(u64, Vec<u8>)], gid: u64, key: u16) -> Option<u64> {
  value_of_asof(entries, gid, key, u64::MAX)
}

#[cfg(test)]
mod tests;
