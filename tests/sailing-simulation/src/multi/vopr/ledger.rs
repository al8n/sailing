//! The per-group read-linearizability ledger — the single-group `ReadLedger` lifted to
//! `(group, key)` grain over gid-tagged payloads.
//!
//! Every accepted read records, at INVOCATION, the group's completed-write index floor (max
//! commit anywhere in the group) and the per-`(group, key)` committed value floor; every observed
//! confirmation must satisfy `index >= floor`, and once the serving replica has APPLIED to the
//! read's index the served value must be `>= v_inv`. An accepted read that never confirms is
//! legal under faults; never-confirmed beats wrongly confirmed.

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
  /// replica accepts. The value floor reads BOTH pure views per hosting node — the applied state
  /// (retains any compacted prefix) ⊔ the committed log tail (holds committed-not-yet-applied
  /// writes) — so neither apply lag nor compaction under-counts a completed write.
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
      .hosting_nodes(gid)
      .into_iter()
      .filter_map(|n| {
        value_of(&w.applied_of(n, gid), gid, key)
          .into_iter()
          .chain(value_of(&w.committed_entries_of(n, gid), gid, key))
          .max()
      })
      .max()
      .unwrap_or(0);
    if w.read_index_on(target, gid, &ctx.to_be_bytes()) {
      self.inflight.insert(
        ctx,
        GInvocation {
          gid,
          floor,
          key,
          v_inv,
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
      if w.applied_index_of(p.node, p.inv.gid) < p.index {
        still_pending.push(p);
        continue;
      }
      let observed = value_of_asof(
        &w.applied_of(p.node, p.inv.gid),
        p.inv.gid,
        p.inv.key,
        p.index.get(),
      )
      .unwrap_or(0);
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

/// The value of the LATEST gid-tagged entry for `(gid, key)` at log index `<= upto` over an
/// applied/committed `(index, command)` sequence; `None` if never written. Per-key values ride
/// the global monotone counter, so latest-index also carries the max value.
fn value_of_asof(entries: &[(u64, Vec<u8>)], gid: u64, key: u16, upto: u64) -> Option<u64> {
  let mut best: Option<(u64, u64)> = None;
  for (idx, cmd) in entries {
    if *idx > upto {
      continue;
    }
    if let Some((tag, k, v)) = decode_gkv(cmd)
      && tag == gid
      && k == key
      && best.is_none_or(|(bi, _)| *idx >= bi)
    {
      best = Some((*idx, v));
    }
  }
  best.map(|(_, v)| v)
}

/// The current value of `(gid, key)` over an entire sequence (no index bound).
fn value_of(entries: &[(u64, Vec<u8>)], gid: u64, key: u16) -> Option<u64> {
  value_of_asof(entries, gid, key, u64::MAX)
}
