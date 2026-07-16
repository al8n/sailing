use super::*;
use crate::multi::{MultiVoprReport, MultiWorld, decode_gkv, encode_gkv};
use std::collections::BTreeSet;

/// The value oracle stays SHARP after the floor was narrowed to authoritative replicas: an
/// authoritative, caught-up replica whose serve point shows a value BELOW a committed value that
/// IS in the authoritative `applied()` view still trips the assertion. The narrowing dropped the
/// split-blind raw-log SOURCE (a parked/behind replica's stale history), not the oracle's teeth —
/// a genuine per-key stale read must still panic.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_still_catches_a_genuine_authoritative_stale_read() {
  let gid = 100u64;
  let key = 5u16;
  let mut w = MultiWorld::new(7);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(gid, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(gid).is_some()));
  w.reconcile_membership(gid);

  // Two committed writes to (gid, key): an earlier value 3, then a newer value 5. Both apply on
  // every voter, so the whole group is authoritative and agrees on the latest committed value 5.
  propose_until_accepted(&mut w, gid, &encode_gkv(gid, key, 3));
  propose_until_accepted(&mut w, gid, &encode_gkv(gid, key, 5));
  assert!(
    w.run_until(3_000, |w| (0..3).all(|n| value_of(
      &w.applied_of(n, gid),
      gid,
      key
    ) == Some(5))),
    "both writes must apply on every voter"
  );

  // The floor a real invocation would record — read straight off an authoritative replica's live
  // applied() view, NOT a phantom: the committed value 5.
  let node = *w
    .authoritative_nodes(gid)
    .first()
    .expect("a caught-up authoritative replica");
  let applied = w.applied_of(node, gid);
  assert_eq!(
    value_of(&applied, gid, key),
    Some(5),
    "the authoritative floor is the committed value 5"
  );

  // A serve point that predates the value-5 commit: the index of the earlier value-3 write. Serving
  // there returns 3 though 5 was already committed at invocation — a genuine stale read the read's
  // own index floor forbids in a live run, injected here to drive the value assertion directly.
  let stale_index = applied
    .iter()
    .find(|(_, cmd)| decode_gkv(cmd) == Some((gid, key, 3)))
    .map(|(idx, _)| sailing_proto::Index::new(*idx))
    .expect("the value-3 write is in the applied record");

  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid,
      floor: stale_index,
      key,
      v_inv: 5,
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, 7);
}

/// Propose `payload` on `gid`, ticking through transient leaderless windows until accepted.
fn propose_until_accepted(w: &mut MultiWorld, gid: u64, payload: &[u8]) {
  for _ in 0..3_000 {
    if w.propose(gid, payload).is_some() {
      return;
    }
    w.tick();
  }
  panic!("proposal on g{gid} was never accepted");
}
