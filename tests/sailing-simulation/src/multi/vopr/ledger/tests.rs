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
      // The CURRENT tenure, so this check is JUDGED rather than retired: the group has owned the
      // key continuously since the invocation this stands in for.
      epoch: w.key_epoch_of(gid, key),
      generation: w.generation_of(gid),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, 7);
}

/// The group every mixed-index-space shape below is built on.
const PARENT: u64 = 100;
/// The fork child that carries [`PARENT`]'s cells away and hands them back at a merge.
const CHILD: u64 = 105;
/// The key the value assertions are made on.
const KEY: u16 = 2;
/// The first incarnation's last [`KEY`] value: inherited by the child at the fork, folded back
/// into the recreated incarnation at the merge.
const INHERITED: u64 = 200;
/// A native [`KEY`] write made AFTER the fold, above everything the fold froze.
const POST_FOLD: u64 = INHERITED + 20;
/// The source whose EARLIER, lower [`KEY`] write is the one the target's record keeps.
const OLDER_SOURCE: u64 = 110;
/// The source whose LATER, higher [`KEY`] write is folded in and then split away.
const NEWER_SOURCE: u64 = 115;
/// The child the target hands [`KEY`] to — taking the newer fold's cells with it.
const DEPARTED: u64 = 120;
/// [`OLDER_SOURCE`]'s [`KEY`] value: folded in LAST, and all the target's record ends up holding.
const OLDER_VALUE: u64 = 300;
/// [`NEWER_SOURCE`]'s [`KEY`] value: folded in first, then split away with its cells.
const NEWER_VALUE: u64 = 400;

/// The value reconstruction places FOLDED-IN content at its fold coordinate, not at the foreign
/// index the folded cell carries (#118). Both halves of the mixed-index-space shape are built
/// here end to end:
///
/// * a fork child's inherited cells keep the PARENT's tag, so the per-node scan reads nothing for
///   them — only the genesis baseline can floor a read invoked before the child's first own write;
/// * a RECREATED id runs a fresh index space while cells tagged with that id survive in the child
///   at the OLD incarnation's indices. Merging the child back grafts them in ABOVE the recreated
///   group's whole log, where the unbounded invocation scan sees them and the index-bounded serve
///   scan cannot — the disagreement that reported a correct read as stale.
#[test]
fn absorbed_content_is_reconstructed_at_its_fold_coordinate() {
  const SEED: u64 = 23;

  let mut w = world_after_the_fork(SEED);
  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();

  // THE GENESIS LEG: the child has written nothing, and every cell it holds for KEY is
  // parent-tagged — without the baseline the floor would read 0 and the oracle would accept any
  // value the child ever served for KEY.
  let child_leader = w.leader_of(CHILD).expect("the child has a leader");
  let child_ctx = issue_until_accepted(&mut w, &mut ledger, CHILD, child_leader, KEY, &mut report);
  assert_eq!(
    ledger.inflight[&child_ctx].v_inv, INHERITED,
    "a read on the fork child floors at the value it inherited"
  );

  fold_the_child_back(&mut w);

  // THE REPRODUCTION SHAPE, pinned: cells tagged with the target's OWN gid now sit at foreign
  // indices above everything the recreated incarnation has committed, carrying a value the
  // index-bounded scan cannot reach. Without the fold baseline the two legs disagree by exactly
  // this gap and the read below trips the oracle.
  let node = authoritative_node(&w, PARENT);
  let record = w.applied_of(node, PARENT);
  let commit = w.max_commit_of(PARENT).get();
  assert!(
    max_value_above(&record, PARENT, KEY, commit)
      > value_of_asof(&record, PARENT, KEY, commit).unwrap_or(0),
    "the absorbed cells must strand a KEY value above the recreated id's own index space"
  );

  // THE ABSORB LEG: a correct read on the recreated id, served at its own index. Its floor
  // reflects the fold (the folded value, not the fresh incarnation's short history), and the
  // serve point clears that floor through the same baseline — no stale-read verdict.
  let v_inv = serve_one_read(&mut w, &mut ledger, &mut report, node, KEY, SEED);
  assert!(
    v_inv >= INHERITED,
    "the read floor must reflect the folded-in value"
  );
}

/// A write made AFTER the fold is the group's value, however the fold's own cells are indexed.
/// The frozen baseline is a floor for what the fold carried, never a ceiling on what the group
/// commits next — and the record scan must RANK the newer write above an absorbed cell that
/// merely sits at a greater FOREIGN index. Ranking by index instead reports the older absorbed
/// value as the committed one, which understates the floor and blesses a stale serve (the teeth
/// are pinned by [`value_oracle_still_catches_a_stale_serve_after_a_fold`]).
#[test]
fn a_post_fold_write_outranks_an_absorbed_cell_at_a_greater_index() {
  const SEED: u64 = 29;

  let mut w = world_after_the_fork(SEED);
  fold_the_child_back(&mut w);
  let newest = commit_key_value(&mut w, POST_FOLD);

  // The SHADOWING shape: an absorbed cell carrying a LOWER value sits at a GREATER index than
  // the newest committed write — the exact pair an index-ranked scan gets backwards.
  let node = authoritative_node(&w, PARENT);
  let record = w.applied_of(node, PARENT);
  let shadowing = max_value_above(&record, PARENT, KEY, newest.get());
  assert!(
    shadowing > 0 && shadowing < POST_FOLD,
    "an absorbed cell must sit above the newest write carrying a lower value (found {shadowing})"
  );

  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();
  let v_inv = serve_one_read(&mut w, &mut ledger, &mut report, node, KEY, SEED);
  assert_eq!(
    v_inv, POST_FOLD,
    "the read floor is the newest committed write, not the value the fold froze"
  );
}

/// The oracle keeps its teeth across a fold: a serve point that predates a post-fold write is
/// still stale, and the fold baseline — a floor for the folded content — must not paper over it.
/// The invocation floor is read straight off an authoritative replica's live view, so the arm
/// this pins is the SELECTION: rank the absorbed cell at the greater index highest and the floor
/// sinks below the baseline, where a stale serve passes unnoticed.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_still_catches_a_stale_serve_after_a_fold() {
  const SEED: u64 = 31;

  let mut w = world_after_the_fork(SEED);
  fold_the_child_back(&mut w);
  let newest = commit_key_value(&mut w, POST_FOLD);

  let node = authoritative_node(&w, PARENT);
  let applied = w.applied_of(node, PARENT);
  let v_inv = value_of(&applied, PARENT, KEY).expect("the key is written");

  // A serve point one index BELOW the post-fold write: everything visible there is folded
  // content, so serving it returns a value the newer committed write has superseded. The read's
  // own index floor forbids this in a live run — injected here to drive the value assertion.
  assert!(newest.get() > 1, "the post-fold write has a predecessor");
  let stale_index = sailing_proto::Index::new(newest.get() - 1);
  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid: PARENT,
      floor: stale_index,
      key: KEY,
      v_inv,
      // The CURRENT tenure, so this check is JUDGED rather than retired.
      epoch: w.key_epoch_of(PARENT, KEY),
      generation: w.generation_of(PARENT),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, SEED);
}

/// `LogSm::absorb` appends the source's WHOLE record, PARKED keys included: an accepted-but-lost
/// split shrinks a population while its cells stay in the record, and a later split at a lower
/// point hands those cells to a child that never owned the keys. Anchoring only the source's LIVE
/// population would leave exactly those cells uncovered — and a recreated target owns the key
/// again, so a read routes to it and meets the same mixed-index-space gap.
#[test]
fn parked_cells_absorbed_outside_the_live_population_are_anchored() {
  const SEED: u64 = 37;
  /// The key the doomed split parks: written LAST, so its cells sit at the highest indices of the
  /// first incarnation's space — above everything the recreated one reaches.
  const PARKED_KEY: u16 = 5;
  const PARKED_VALUE: u64 = 500;
  /// The child of the lost split: proposed, never committed, never materialized.
  const DOOMED: u64 = 900;
  /// The child of the later, lower split — the one that carries the parked cells away.
  const CARRIER: u64 = 901;

  let mut w = MultiWorld::new(SEED);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(PARENT, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(PARENT).is_some()));
  w.reconcile_membership(PARENT);
  for step in 1..=INHERITED / 10 {
    propose_until_accepted(&mut w, PARENT, &encode_gkv(PARENT, KEY, step * 10));
  }
  for value in [300, 400, PARKED_VALUE] {
    propose_until_accepted(&mut w, PARENT, &encode_gkv(PARENT, PARKED_KEY, value));
  }
  assert!(
    w.run_until(3_000, |w| (0..3).all(|n| value_of(
      &w.applied_of(n, PARENT),
      PARENT,
      PARKED_KEY
    ) == Some(PARKED_VALUE))),
    "every replica must hold the pre-split history"
  );

  // The DOOMED split: accepted, then deposed and truncated away. PARKED_KEY is unroutable from
  // the accept onward, while its cells stay in every parent record.
  lose_a_split(&mut w, DOOMED, PARKED_KEY, PARKED_VALUE + 50);

  // The later, LOWER split: its instruction moves every cell at or above KEY — the parked key's
  // included, though the child's live population never carries it.
  split_until_accepted(&mut w, PARENT, CARRIER, KEY);
  assert!(
    w.run_until(3_000, |w| w.leader_of(CARRIER).is_some()),
    "the carrier child never elected: {}",
    w.dbg_group(CARRIER)
  );
  assert!(
    !w.group_holds_key(CARRIER, PARKED_KEY),
    "the parked key stays out of the child's live population"
  );
  let carrier_host = authoritative_node(&w, CARRIER);
  assert!(
    w.applied_of(carrier_host, CARRIER)
      .iter()
      .any(|(_, cmd)| decode_gkv(cmd) == Some((PARENT, PARKED_KEY, PARKED_VALUE))),
    "the parked key's cells must ride the split into the child"
  );

  // Recreate the parent (owning its whole key domain again) and fold the carrier back.
  recreate(&mut w, PARENT);
  merge_back(&mut w, CARRIER, PARENT);
  assert!(
    w.group_holds_key(PARENT, PARKED_KEY),
    "the recreated incarnation owns the parked key again"
  );

  // The shape: the parked cells are the target's OWN tag at foreign indices above its whole log,
  // and nothing the index-bounded scan can reach carries them.
  let node = authoritative_node(&w, PARENT);
  let record = w.applied_of(node, PARENT);
  let commit = w.max_commit_of(PARENT).get();
  assert_eq!(
    value_of_asof(&record, PARENT, PARKED_KEY, commit),
    None,
    "no parked cell may be index-eligible at the recreated id's own commit"
  );
  assert_eq!(
    max_value_above(&record, PARENT, PARKED_KEY, commit),
    PARKED_VALUE,
    "the parked cells must strand their value above the recreated id's index space"
  );

  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();
  let v_inv = serve_one_read(&mut w, &mut ledger, &mut report, node, PARKED_KEY, SEED);
  assert_eq!(
    v_inv, PARKED_VALUE,
    "the read floor must carry the parked cells the fold absorbed"
  );
}

/// A fold anchor is INVALIDATED when a split moves its key's cells out of the group.
/// `LogSm::split` removes every instruction-matched cell from the parent's record, so an anchor
/// outliving the cells answers with a value the record no longer contains — and the group
/// REACQUIRES the key the moment an older source folds in, which is exactly when both oracle legs
/// would read that phantom maximum and bless a serve that is stale against it.
#[test]
fn a_split_takes_its_keys_fold_anchors_with_the_cells() {
  const SEED: u64 = 41;

  let (mut w, _) = world_with_a_split_away_anchor(SEED);
  let node = authoritative_node(&w, PARENT);

  // The record's own testimony: the newer fold's cells left with the split, so the older source's
  // value is the ONLY one the target can serve for KEY.
  let carried: Vec<u64> = w
    .applied_of(node, PARENT)
    .iter()
    .filter_map(|(_, cmd)| decode_gkv(cmd))
    .filter(|&(_, key, _)| key == KEY)
    .map(|(_, _, value)| value)
    .collect();
  assert_eq!(
    carried,
    std::vec![OLDER_VALUE],
    "only the older source's cell may survive in the target's record"
  );

  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();
  let v_inv = serve_one_read(&mut w, &mut ledger, &mut report, node, KEY, SEED);
  assert_eq!(
    v_inv, OLDER_VALUE,
    "the read floor is the surviving fold's anchor, not the one the split took away"
  );
}

/// The teeth around that invalidation: the surviving fold's anchor is a floor for what the fold
/// carried, and a serve point that predates a LATER native write is still stale against it. The
/// serve point sits at or past the fold's coordinate, so the check is judged rather than retired
/// — the reacquired tenure is judged as sharply as any other.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_still_catches_a_stale_serve_past_a_reacquired_folds_coordinate() {
  const SEED: u64 = 43;
  /// A native write above everything the surviving fold carried.
  const RECLAIMED: u64 = NEWER_VALUE + 100;

  let (mut w, _) = world_with_a_split_away_anchor(SEED);
  let newest = commit_key_value(&mut w, RECLAIMED);
  let node = authoritative_node(&w, PARENT);

  // The floor a real invocation records: the reclaimed native write, above the fold's anchor.
  let v_inv = value_of(&w.applied_of(node, PARENT), PARENT, KEY)
    .expect("the reclaimed write is the target's own cell")
    .max(w.fold_baseline_of(PARENT, KEY, u64::MAX));
  assert_eq!(v_inv, RECLAIMED, "the floor is the newest committed write");

  // One index below that write: the only thing visible for KEY there is the fold's anchored
  // content, which the newer write has superseded. At or past the fold's own coordinate, so
  // nothing above it can splice cells this scan cannot classify.
  assert!(newest.get() > 1, "the reclaimed write has a predecessor");
  let stale_index = sailing_proto::Index::new(newest.get() - 1);
  assert!(
    !w.fold_after(PARENT, KEY, stale_index.get()),
    "the serve point must sit at or past every fold of KEY, or the check retires instead"
  );
  assert_eq!(
    w.fold_baseline_of(PARENT, KEY, stale_index.get()),
    OLDER_VALUE,
    "the anchored content is all the serve point can show"
  );
  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid: PARENT,
      floor: stale_index,
      key: KEY,
      v_inv,
      // The CURRENT tenure, so this check is JUDGED rather than retired.
      epoch: w.key_epoch_of(PARENT, KEY),
      generation: w.generation_of(PARENT),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, SEED);
}

/// The key-ownership ABA, FALSE-PANIC direction: a read accepted while the group holds [`KEY`] at
/// the newer fold's value confirms only after the key was split away and REACQUIRED from an older
/// source. Current ownership reads true again — "holds the key" cannot distinguish held from
/// held-AGAIN — but the content underneath was replaced wholesale, so judging the old read there
/// condemns a read that was correct when it was served.
#[test]
fn a_read_that_crossed_a_key_reacquisition_is_retired_unjudged() {
  const SEED: u64 = 47;

  let (mut w, _) = world_after_the_newer_fold(SEED);
  let node = authoritative_node(&w, PARENT);
  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();

  // A read invoked — and served — against the tenure that holds KEY at the newer value. Its
  // confirmation is left UNDRAINED across the ownership change below.
  let ctx = issue_until_accepted(&mut w, &mut ledger, PARENT, node, KEY, &mut report);
  assert_eq!(
    ledger.inflight[&ctx].v_inv, NEWER_VALUE,
    "the read floors at the folded value its own tenure holds"
  );
  assert!(
    w.run_until(3_000, |w| (0..3)
      .any(|n| !w.read_states_of(n, PARENT).is_empty())),
    "the read never confirmed, so nothing crosses the discontinuity"
  );

  take_the_key_away_and_fold_the_older_source(&mut w);
  assert!(
    w.group_holds_key(PARENT, KEY),
    "the guard is needed precisely because current ownership reads true again"
  );

  drain_to_quiescence(&mut w, &mut ledger, &mut report, SEED);
  assert_eq!(
    report.reads_confirmed, 1,
    "the delayed confirmation must be observed — otherwise nothing was tested"
  );
  assert!(
    ledger.pending_value.is_empty(),
    "the cross-tenure check must be retired, not left pending forever"
  );
  assert_eq!(
    report.reads_value_checked, 0,
    "a check that crossed an ownership discontinuity is never judged"
  );
}

/// The key-ownership ABA, BLESS direction: the child that took [`KEY`] merges straight back,
/// restoring the group's OWN cells at their preserved indices — so the post-round-trip record
/// resembles the pre-split one closely enough that a crossed check passes and is counted as
/// verified, though the group did not own the key for part of that read's life. Retired instead;
/// a same-tenure read stays as sharply judged as ever (see
/// [`value_oracle_still_catches_a_serve_below_a_restored_tenure`]).
#[test]
fn a_read_that_crossed_a_child_merge_back_is_retired_unjudged() {
  const SEED: u64 = 53;

  let mut w = world_with_a_key_history(SEED);
  let node = authoritative_node(&w, PARENT);
  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();

  let ctx = issue_until_accepted(&mut w, &mut ledger, PARENT, node, KEY, &mut report);
  assert_eq!(
    ledger.inflight[&ctx].v_inv, INHERITED,
    "the read floors at the value its own tenure holds"
  );
  assert!(
    w.run_until(3_000, |w| (0..3)
      .any(|n| !w.read_states_of(n, PARENT).is_empty())),
    "the read never confirmed, so nothing crosses the discontinuity"
  );

  fork_and_fold_back(&mut w);

  drain_to_quiescence(&mut w, &mut ledger, &mut report, SEED);
  assert_eq!(
    report.reads_confirmed, 1,
    "the delayed confirmation must be observed — otherwise nothing was tested"
  );
  assert!(
    ledger.pending_value.is_empty(),
    "the cross-tenure check must be retired, not left pending forever"
  );
  assert_eq!(
    report.reads_value_checked, 0,
    "a check that crossed the round trip is retired, never counted as verified"
  );
}

/// The tenure guard retires checks that CROSSED a discontinuity, not checks that merely live in a
/// group where one happened: a read invoked after the round trip carries the current tenure and is
/// judged exactly as before — a serve point that predates a later native write still trips.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn value_oracle_still_catches_a_stale_serve_in_a_restored_tenure() {
  const SEED: u64 = 59;
  /// A native write above everything the round trip restored.
  const RESTORED_WRITE: u64 = INHERITED + 100;

  let mut w = world_after_a_key_round_trip(SEED);
  let newest = commit_key_value(&mut w, RESTORED_WRITE);
  let node = authoritative_node(&w, PARENT);
  let v_inv = value_of(&w.applied_of(node, PARENT), PARENT, KEY)
    .expect("the restored cells carry KEY")
    .max(w.fold_baseline_of(PARENT, KEY, u64::MAX));
  assert_eq!(
    v_inv, RESTORED_WRITE,
    "the floor is the newest committed write"
  );

  // One index below that write: everything visible for KEY there is restored content the newer
  // write superseded — genuinely stale. Judged, because the serve point sits past the merge-back's
  // coordinate and the tenure is the one the group is in now.
  assert!(newest.get() > 1, "the restored write has a predecessor");
  let stale_index = sailing_proto::Index::new(newest.get() - 1);
  assert!(
    !w.fold_after(PARENT, KEY, stale_index.get()),
    "the serve point must sit at or past every fold of KEY, or the check retires instead"
  );
  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid: PARENT,
      floor: stale_index,
      key: KEY,
      v_inv,
      epoch: w.key_epoch_of(PARENT, KEY),
      generation: w.generation_of(PARENT),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, SEED);
}

/// An accepted split can be DEPOSED and truncated with its cells never leaving the parent, so the
/// fold anchors must retire when the record actually partitions, not when the proposal lands.
/// Retiring at proposal strips the anchor off content the record still holds — and once a lower
/// source reacquires the parked key, the serve leg floors below what the invocation legitimately
/// saw and a correct read is condemned.
#[test]
fn a_lost_split_leaves_the_fold_anchors_it_never_moved() {
  const SEED: u64 = 71;
  /// The child of the split that is accepted and then truncated away.
  const LOST: u64 = 130;
  /// The reclaiming source's [`KEY`] value: written FIRST, so the monotone counter makes it the
  /// LOWER one — below everything the old incarnation went on to write.
  const RECLAIM_VALUE: u64 = 5;

  let mut w = MultiWorld::new(SEED);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(OLDER_SOURCE, &voters);
  w.create_group(PARENT, &voters);
  assert!(w.run_until(4_000, |w| w.leader_of(OLDER_SOURCE).is_some()
    && w.leader_of(PARENT).is_some()));
  w.reconcile_membership(OLDER_SOURCE);
  w.reconcile_membership(PARENT);
  propose_until_accepted(
    &mut w,
    OLDER_SOURCE,
    &encode_gkv(OLDER_SOURCE, KEY, RECLAIM_VALUE),
  );
  for step in 1..=INHERITED / 10 {
    propose_until_accepted(&mut w, PARENT, &encode_gkv(PARENT, KEY, step * 10));
  }
  assert!(
    w.run_until(3_000, |w| (0..3).all(|n| value_of(
      &w.applied_of(n, PARENT),
      PARENT,
      KEY
    ) == Some(INHERITED))),
    "every replica must hold the pre-split KEY history"
  );

  // Same-gid cells folded into a RECREATED parent: they carry the parent's own tag at the old
  // incarnation's indices, and the fold anchors them above the fresh log.
  split_until_accepted(&mut w, PARENT, CHILD, KEY);
  assert!(
    w.run_until(4_000, |w| (0..3).all(|n| w.hosts_group(n, CHILD))),
    "the child never materialized everywhere: {}",
    w.dbg_group(CHILD)
  );
  recreate(&mut w, PARENT);
  merge_back(&mut w, CHILD, PARENT);
  assert_eq!(
    w.fold_baseline_of(PARENT, KEY, u64::MAX),
    INHERITED,
    "the fold anchors the spliced cells at their own value"
  );

  // A split covering KEY is ACCEPTED and then lost: the key parks, but `LogSm::split` never runs,
  // so every one of those cells is still in the parent's record.
  lose_a_split(&mut w, LOST, KEY, INHERITED + 10);
  assert_eq!(
    w.fold_baseline_of(PARENT, KEY, u64::MAX),
    INHERITED,
    "a split that moved no cell must leave their anchor alone"
  );

  // The lower-valued source reclaims the parked key.
  merge_back(&mut w, OLDER_SOURCE, PARENT);
  assert!(
    w.group_holds_key(PARENT, KEY),
    "the reclaiming fold hands KEY back"
  );

  // The shape: the parent's own top cell is stranded above the recreated index space, so only the
  // surviving anchor can carry it to the serve leg.
  let node = authoritative_node(&w, PARENT);
  let record = w.applied_of(node, PARENT);
  let commit = w.max_commit_of(PARENT).get();
  assert_eq!(
    max_value_above(&record, PARENT, KEY, commit),
    INHERITED,
    "the old incarnation's newest KEY cell must sit above the recreated id's own index space"
  );

  // A correct read, judged: the tenure it captures is the one it is judged in, and both folds sit
  // below its index. It must not be condemned.
  let mut ledger = MultiReadLedger::new();
  let mut report = MultiVoprReport::default();
  let v_inv = serve_one_read(&mut w, &mut ledger, &mut report, node, KEY, SEED);
  assert_eq!(
    v_inv, INHERITED,
    "the floor is the spliced cells' value, carried by the anchor the lost split left alone"
  );
}

/// Drive `parent` into the ACCEPTED-BUT-LOST split shape at `point`: a fully isolated leader
/// accepts the split, the survivors depose it, and `load` — committed to key 0, which no `point`
/// here covers — truncates the entry away as the healed ex-leader catches up. The population
/// stays conservatively shrunk (the keys are PARKED and unroutable) while their CELLS never leave
/// the parent's record.
fn lose_a_split(w: &mut MultiWorld, child: u64, point: u16, load: u64) {
  let doomed = w.leader_of(PARENT).expect("elected");
  w.isolate(doomed);
  assert!(matches!(w.propose_split(PARENT, child, point), Some(Ok(_))));
  assert!(
    !w.group_holds_key(PARENT, point),
    "the accepted split parks its keys at once"
  );
  assert!(
    w.run_until(4_000, |w| w.leader_of(PARENT).is_some_and(|l| l != doomed)),
    "the survivors never deposed the isolated leader"
  );
  w.heal(doomed);
  propose_until_accepted(w, PARENT, &encode_gkv(PARENT, 0, load));
  assert!(
    w.run_until(3_000, |w| (0..3).all(|n| !w.hosts_group(n, child))),
    "a lost split must never materialize its child"
  );
}

/// A fold's anchor names the keys it SPLICED, so a merge that carried nothing of the read's key
/// leaves that read judgeable. Were the map built from the post-fold union it would name every
/// key the target already held too, and any unrelated merge landing above a pending check would
/// retire it — judgeable reads dropped wholesale during ordinary merges, and a real stale serve
/// escaping among them.
#[test]
#[should_panic(expected = "read-value-linearizability")]
fn a_fold_that_spliced_another_key_leaves_the_check_judged() {
  const SEED: u64 = 67;
  /// The only key the folded-in source ever writes — never [`KEY`].
  const OTHER_KEY: u16 = KEY + 1;
  const OTHER_VALUE: u64 = INHERITED + 100;

  let mut w = world_with_a_key_history(SEED);

  // A source whose whole record is OTHER_KEY, folded into the target above the serve point below.
  // The target already owns every key handed over, so the tenure is untouched and the tenure
  // guard cannot be what decides this test.
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(NEWER_SOURCE, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(NEWER_SOURCE).is_some()));
  w.reconcile_membership(NEWER_SOURCE);
  propose_until_accepted(
    &mut w,
    NEWER_SOURCE,
    &encode_gkv(NEWER_SOURCE, OTHER_KEY, OTHER_VALUE),
  );
  assert!(
    w.run_until(3_000, |w| value_of(
      &w.applied_of(0, NEWER_SOURCE),
      NEWER_SOURCE,
      OTHER_KEY
    ) == Some(OTHER_VALUE)),
    "the source must commit its OTHER_KEY write"
  );
  merge_back(&mut w, NEWER_SOURCE, PARENT);
  assert_eq!(
    w.key_epoch_of(PARENT, KEY),
    0,
    "the target already owned KEY, so the fold leaves the tenure unbroken"
  );

  let node = authoritative_node(&w, PARENT);
  let applied = w.applied_of(node, PARENT);
  let v_inv = value_of(&applied, PARENT, KEY).expect("the target wrote KEY itself");
  assert_eq!(
    v_inv, INHERITED,
    "the floor is the target's own newest write"
  );

  // A serve point at the target's OLDEST native KEY cell: genuinely stale against everything it
  // has committed since, and BELOW the fold's coordinate. Whether that fold retires the check is
  // what this test is FOR, so it is left to the outcome rather than asserted — but that a fold
  // really does land above the serve point is pinned here, or the test would be vacuous.
  let earliest = applied
    .iter()
    .filter(
      |(_, cmd)| matches!(decode_gkv(cmd), Some((tag, key, _)) if tag == PARENT && key == KEY),
    )
    .map(|(idx, _)| *idx)
    .min()
    .expect("the target's own KEY cells are in the record");
  assert!(
    w.fold_after(PARENT, OTHER_KEY, earliest),
    "the fold must sit above the serve point for this to test anything"
  );

  let stale_index = sailing_proto::Index::new(earliest);
  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid: PARENT,
      floor: stale_index,
      key: KEY,
      v_inv,
      epoch: w.key_epoch_of(PARENT, KEY),
      generation: w.generation_of(PARENT),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, SEED);
}

/// The SAME-TENURE splice: a fold into a target that ALREADY owns the key does not bump the
/// tenure — correctly, nothing changed hands — yet it grafts same-tag cells at their preserved
/// foreign indices, and those can land BELOW a deferred check's read index while the fold's own
/// coordinate lands above it. The as-of scan admits them with no anchor in range to classify
/// them, inflating `observed` and blessing a serve the group could not have made at that
/// coordinate. So the check retires on the fold's coordinate instead of being judged.
#[test]
fn a_check_predating_a_same_tenure_fold_is_retired_unjudged() {
  const SEED: u64 = 61;

  let mut w = world_after_the_fork(SEED);
  recreate(&mut w, PARENT);
  // Advance the recreated log on ANOTHER key, so coordinates exist that are below the fold and
  // above some of the spliced cells' low foreign indices.
  for step in 1..=3 {
    propose_until_accepted(
      &mut w,
      PARENT,
      &encode_gkv(PARENT, 0, INHERITED + step * 10),
    );
  }
  merge_back(&mut w, CHILD, PARENT);
  assert_eq!(
    w.key_epoch_of(PARENT, KEY),
    0,
    "the target already owned KEY, so the fold leaves the tenure unbroken and the tenure guard \
     cannot be what retires this check"
  );

  let node = authoritative_node(&w, PARENT);
  let record = w.applied_of(node, PARENT);
  // The widest coordinate at which the fold is still INVISIBLE — anything the as-of scan shows
  // for KEY there arrived by a splice it has no anchor to classify.
  let before_the_fold = (1..=w.applied_index_of(node, PARENT).get())
    .take_while(|&idx| w.fold_baseline_of(PARENT, KEY, idx) == 0)
    .last()
    .expect("the fold's coordinate is above the log's start");
  assert!(
    w.fold_baseline_of(PARENT, KEY, u64::MAX) > 0,
    "the fold anchored KEY somewhere above that coordinate"
  );
  let spliced = value_of_asof(&record, PARENT, KEY, before_the_fold)
    .expect("a spliced same-tag cell is index-eligible below the fold");
  assert!(
    spliced > 0,
    "the splice is what inflates the scan at that coordinate"
  );

  // A serve showing exactly what the splice inflated the scan to: judged, it passes — the group
  // held nothing for KEY at that coordinate, and the check would be counted as verified.
  let stale_index = sailing_proto::Index::new(before_the_fold);
  let mut ledger = MultiReadLedger::new();
  ledger.pending_value.push(PendingValueCheck {
    ctx: 0,
    node,
    index: stale_index,
    inv: GInvocation {
      gid: PARENT,
      floor: stale_index,
      key: KEY,
      v_inv: spliced,
      epoch: w.key_epoch_of(PARENT, KEY),
      generation: w.generation_of(PARENT),
    },
  });
  let mut report = MultiVoprReport::default();
  ledger.scan(&w, &mut report, SEED);
  assert!(
    ledger.pending_value.is_empty(),
    "the check must be retired, not left pending forever"
  );
  assert_eq!(
    report.reads_value_checked, 0,
    "a check predating an unclassifiable fold is never judged — judged, the spliced cells bless it"
  );
}

/// [`KEY`] leaves to [`CHILD`] and comes straight back: the child's absorb restores the group's
/// own cells at their original indices, so ownership — and the record — look unbroken afterwards.
fn fork_and_fold_back(w: &mut MultiWorld) {
  split_until_accepted(w, PARENT, CHILD, KEY);
  assert!(
    w.run_until(4_000, |w| (0..3).all(|n| w.hosts_group(n, CHILD))),
    "the child never materialized everywhere: {}",
    w.dbg_group(CHILD)
  );
  assert!(
    !w.group_holds_key(PARENT, KEY),
    "the split moved KEY to the child"
  );
  merge_back(w, CHILD, PARENT);
  assert!(
    w.group_holds_key(PARENT, KEY),
    "the merge-back hands KEY to the parent again"
  );
}

/// [`world_with_a_key_history`] plus a complete [`fork_and_fold_back`] round trip.
fn world_after_a_key_round_trip(seed: u64) -> MultiWorld {
  let mut w = world_with_a_key_history(seed);
  fork_and_fold_back(&mut w);
  w
}

/// Tick and scan until the ledger holds no deferred check — every confirmation drained and every
/// value check judged or retired.
fn drain_to_quiescence(
  w: &mut MultiWorld,
  ledger: &mut MultiReadLedger,
  report: &mut MultiVoprReport,
  seed: u64,
) {
  for _ in 0..2_000 {
    w.tick();
    ledger.scan(w, report, seed);
    if ledger.pending_value.is_empty() && report.reads_confirmed > 0 {
      return;
    }
  }
  panic!("the deferred check never drained");
}

/// Build the anchor-invalidation shape: [`NEWER_SOURCE`] folds [`NEWER_VALUE`] into [`PARENT`],
/// PARENT then SPLITS [`KEY`] away (the folded cells leave with the child), and [`OLDER_SOURCE`]
/// folds its earlier [`OLDER_VALUE`] back in — reacquiring the key. Returns the world and
/// PARENT's commit right after the FIRST fold: at or above the departed anchor's coordinate and
/// strictly below the surviving one's, since the split and the second merge are appended past it.
fn world_with_a_split_away_anchor(seed: u64) -> (MultiWorld, u64) {
  let (mut w, after_the_first_fold) = world_after_the_newer_fold(seed);
  take_the_key_away_and_fold_the_older_source(&mut w);
  (w, after_the_first_fold)
}

/// The shape's first half, stopping where [`KEY`] is held at [`NEWER_VALUE`] — the instant a read
/// can be invoked against a tenure the second half then breaks.
fn world_after_the_newer_fold(seed: u64) -> (MultiWorld, u64) {
  const GROUPS: [u64; 3] = [PARENT, OLDER_SOURCE, NEWER_SOURCE];

  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  for gid in GROUPS {
    w.create_group(gid, &voters);
  }
  assert!(w.run_until(4_000, |w| {
    GROUPS.iter().all(|&gid| w.leader_of(gid).is_some())
  }));
  for gid in GROUPS {
    w.reconcile_membership(gid);
  }

  // Values ascend in write order (the workload's global monotone-counter contract), so the source
  // that writes FIRST is the one carrying the lower value.
  propose_until_accepted(
    &mut w,
    OLDER_SOURCE,
    &encode_gkv(OLDER_SOURCE, KEY, OLDER_VALUE),
  );
  propose_until_accepted(
    &mut w,
    NEWER_SOURCE,
    &encode_gkv(NEWER_SOURCE, KEY, NEWER_VALUE),
  );
  assert!(
    w.run_until(3_000, |w| {
      value_of(&w.applied_of(0, OLDER_SOURCE), OLDER_SOURCE, KEY) == Some(OLDER_VALUE)
        && value_of(&w.applied_of(0, NEWER_SOURCE), NEWER_SOURCE, KEY) == Some(NEWER_VALUE)
    }),
    "both sources must commit their KEY write"
  );

  merge_back(&mut w, NEWER_SOURCE, PARENT);
  assert_eq!(
    w.fold_baseline_of(PARENT, KEY, u64::MAX),
    NEWER_VALUE,
    "the newer fold must anchor KEY at its own value"
  );
  let after_the_first_fold = w.max_commit_of(PARENT).get();
  (w, after_the_first_fold)
}

/// The shape's second half: the split takes [`KEY`]'s cells out of the target's record — every
/// cell, by the instruction — and [`OLDER_SOURCE`] then folds its earlier value back in, handing
/// the key to the target again.
fn take_the_key_away_and_fold_the_older_source(w: &mut MultiWorld) {
  split_until_accepted(w, PARENT, DEPARTED, KEY);
  assert!(
    w.run_until(4_000, |w| (0..3).all(|n| w.hosts_group(n, DEPARTED))),
    "the child never materialized everywhere: {}",
    w.dbg_group(DEPARTED)
  );
  assert!(
    !w.group_holds_key(PARENT, KEY),
    "the split moved KEY to the child"
  );
  merge_back(w, OLDER_SOURCE, PARENT);
  assert!(
    w.group_holds_key(PARENT, KEY),
    "the fold hands KEY back to the target"
  );
}

/// [`PARENT`] alone, holding [`KEY`] across a RANGE of its own index space (values ascending, the
/// workload's global monotone-counter contract) — the state every fork shape below starts from,
/// and the instant a read can be invoked against a tenure a later split breaks.
fn world_with_a_key_history(seed: u64) -> MultiWorld {
  let mut w = MultiWorld::new(seed);
  for n in 0..3 {
    w.add_node(n);
  }
  let voters: BTreeSet<u64> = (0..3).collect();
  w.create_group(PARENT, &voters);
  assert!(w.run_until(3_000, |w| w.leader_of(PARENT).is_some()));
  w.reconcile_membership(PARENT);
  for step in 1..=INHERITED / 10 {
    propose_until_accepted(&mut w, PARENT, &encode_gkv(PARENT, KEY, step * 10));
  }
  assert!(
    w.run_until(3_000, |w| (0..3).all(|n| value_of(
      &w.applied_of(n, PARENT),
      PARENT,
      KEY
    ) == Some(INHERITED))),
    "every replica must hold the pre-split KEY history"
  );
  w
}

/// Build the fork half of the mixed-index-space shape: [`world_with_a_key_history`], then a split
/// of KEY away to [`CHILD`], whose inherited cells keep the parent's tag AND the parent's indices.
fn world_after_the_fork(seed: u64) -> MultiWorld {
  let mut w = world_with_a_key_history(seed);
  split_until_accepted(&mut w, PARENT, CHILD, KEY);
  assert!(
    w.run_until(3_000, |w| w.leader_of(CHILD).is_some()),
    "the fork child never elected: {}",
    w.dbg_group(CHILD)
  );
  assert!(
    !w.group_holds_key(PARENT, KEY),
    "the split moved KEY to the child"
  );
  w
}

/// Complete the shape: recreate [`PARENT`] on a fresh index space, then merge [`CHILD`] BACK,
/// grafting the old incarnation's cells in at indices spanning and overshooting the whole
/// recreated log. The recreated incarnation writes [`KEY`] nothing of its own first — its own
/// history must not dominate the folded content, or the two reconstruction legs agree for
/// reasons that have nothing to do with the fold.
fn fold_the_child_back(w: &mut MultiWorld) {
  recreate(w, PARENT);
  merge_back(w, CHILD, PARENT);
}

/// Commit `value` to [`KEY`] on [`PARENT`] and settle until every authoritative replica has
/// applied it; returns the write's index in the group's own space.
fn commit_key_value(w: &mut MultiWorld, value: u64) -> sailing_proto::Index {
  propose_until_accepted(w, PARENT, &encode_gkv(PARENT, KEY, value));
  assert!(
    w.run_until(3_000, |w| {
      w.authoritative_nodes(PARENT).iter().all(|&n| {
        w.applied_of(n, PARENT)
          .iter()
          .any(|(_, cmd)| decode_gkv(cmd) == Some((PARENT, KEY, value)))
      })
    }),
    "the write of {value} never applied on the authoritative replicas"
  );
  let node = authoritative_node(w, PARENT);
  w.applied_of(node, PARENT)
    .iter()
    .find(|(_, cmd)| decode_gkv(cmd) == Some((PARENT, KEY, value)))
    .map(|(idx, _)| sailing_proto::Index::new(*idx))
    .expect("the write is in the applied record")
}

/// Retire and recreate `gid`: the same logical group on a FRESH index space, owning its whole key
/// domain again.
fn recreate(w: &mut MultiWorld, gid: u64) {
  let mut retired = false;
  for _ in 0..3_000 {
    if w.remove_group(gid) {
      retired = true;
      break;
    }
    w.tick();
  }
  assert!(retired, "g{gid} never retired");
  w.recreate_group(gid);
  assert!(
    w.run_until(4_000, |w| w.leader_of(gid).is_some()),
    "the recreated incarnation never elected: {}",
    w.dbg_group(gid)
  );
  w.reconcile_membership(gid);
}

/// Merge `source` into `target` and settle until the source is gone. Leadership is colocated
/// first: the commit barrier is observable only on the source leader's tracker.
fn merge_back(w: &mut MultiWorld, source: u64, target: u64) {
  let host = w.leader_of(target).expect("the target has a leader");
  for _ in 0..2_000 {
    if w.leader_of(source) == Some(host) {
      break;
    }
    w.transfer_group_leader(source, host);
    w.tick();
  }
  merge_verb_until_accepted(w, 3_000, "the freeze", |w| {
    w.propose_prepare_merge(source, target)
  });
  merge_verb_until_accepted(w, 4_000, "the commit", |w| {
    w.propose_commit_merge(target, source)
  });
  assert!(
    w.run_until(8_000, |w| !w.live_groups().contains(&source)),
    "g{source} never absorbed into g{target}: {}",
    w.dbg_group(source)
  );
}

/// Propose the split of `parent` at `point` until accepted, ticking through refusals.
fn split_until_accepted(w: &mut MultiWorld, parent: u64, child: u64, point: u16) {
  for _ in 0..3_000 {
    if matches!(w.propose_split(parent, child, point), Some(Ok(_))) {
      return;
    }
    w.tick();
  }
  panic!("the split of g{parent} at {point} was never accepted");
}

/// The first authoritative replica of `gid` — the view an invocation's floor is read from.
fn authoritative_node(w: &MultiWorld, gid: u64) -> u64 {
  *w.authoritative_nodes(gid)
    .first()
    .unwrap_or_else(|| panic!("g{gid} has an authoritative replica"))
}

/// The greatest `(gid, key)` value carried by a cell indexed ABOVE `upto` — the content an
/// index-bounded reconstruction cannot reach, `0` when there is none.
fn max_value_above(entries: &[(u64, Vec<u8>)], gid: u64, key: u16, upto: u64) -> u64 {
  entries
    .iter()
    .filter(|(idx, _)| *idx > upto)
    .filter_map(|(_, cmd)| decode_gkv(cmd))
    .filter(|&(tag, k, _)| tag == gid && k == key)
    .map(|(_, _, value)| value)
    .max()
    .unwrap_or(0)
}

/// Issue a read on `(node, gid)` for `key`, drive it to its SERVE point through the ledger's own
/// scan, and return the floor the invocation recorded.
fn serve_one_read(
  w: &mut MultiWorld,
  ledger: &mut MultiReadLedger,
  report: &mut MultiVoprReport,
  node: u64,
  key: u16,
  seed: u64,
) -> u64 {
  let checked = report.reads_value_checked;
  let ctx = issue_until_accepted(w, ledger, PARENT, node, key, report);
  let v_inv = ledger.inflight[&ctx].v_inv;
  for _ in 0..3_000 {
    w.tick();
    ledger.scan(w, report, seed);
    if report.reads_value_checked > checked {
      return v_inv;
    }
  }
  panic!("the read on (n{node}, g{PARENT}) never reached its serve point");
}

/// Issue reads on `(node, gid)` for `key` until one is ACCEPTED, returning its context. Refusals
/// (leaderless instants, capacity) are ticked through.
fn issue_until_accepted(
  w: &mut MultiWorld,
  ledger: &mut MultiReadLedger,
  gid: u64,
  node: u64,
  key: u16,
  report: &mut MultiVoprReport,
) -> u64 {
  let before = ledger.inflight.len();
  for _ in 0..3_000 {
    ledger.issue(w, gid, node, key, report);
    if ledger.inflight.len() > before {
      return *ledger
        .inflight
        .keys()
        .next_back()
        .expect("the accepted read is the newest context");
    }
    w.tick();
  }
  panic!("no read on (n{node}, g{gid}) was ever accepted");
}

/// Tick until `verb` lands on some leader, panicking with `what` if the budget runs out.
fn merge_verb_until_accepted(
  w: &mut MultiWorld,
  budget: u32,
  what: &str,
  mut verb: impl FnMut(
    &mut MultiWorld,
  ) -> Option<Result<sailing_proto::Index, sailing_proto::MergeError<u64>>>,
) {
  for _ in 0..budget {
    if matches!(verb(w), Some(Ok(_))) {
      return;
    }
    w.tick();
  }
  panic!("{what} was never accepted");
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
