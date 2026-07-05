//! The committed-split apply arm: `fsm.split` at the deterministic point, the staged pending
//! fork with its apply-derived blob, the fork durability barrier (`snapshot_cap`), the G-free
//! `SplitApplied` event, and the lineage (`shape_gen`) fold into snapshot meta.
use super::*;
use crate::{Config, Data, SplitPayload, wire::encode_split_payload};
use std::collections::BTreeMap;

/// A keyed store: `Command` = an 8-byte BE key (apply bumps its counter), `split` moves every
/// key at or above the instruction's 8-byte BE threshold into the returned child half.
#[derive(Debug, Default, PartialEq, Eq)]
struct KeyedSm {
  map: BTreeMap<u64, u64>,
}

fn be_key(k: u64) -> bytes::Bytes {
  bytes::Bytes::copy_from_slice(&k.to_be_bytes())
}

/// A keyed command as ENTRY data: the `Data`-encoded form `propose` would append (apply decodes
/// it back to the raw 8-byte key).
fn key_cmd(k: u64) -> bytes::Bytes {
  let mut buf = Vec::new();
  be_key(k).encode(&mut buf);
  bytes::Bytes::from(buf)
}

/// A stored snapshot BLOB for `sm`: the `Data`-encoded capture, exactly what `maybe_snapshot`
/// persists (and what the apply arm derives for a forked half).
fn sm_blob(sm: &KeyedSm) -> bytes::Bytes {
  let snap = StateMachine::snapshot(sm).unwrap();
  let mut buf = Vec::new();
  snap.encode(&mut buf);
  bytes::Bytes::from(buf)
}

impl StateMachine for KeyedSm {
  type Command = bytes::Bytes;
  type Response = u64;
  type Snapshot = bytes::Bytes;
  type Error = crate::DecodeError;

  fn apply(&mut self, _index: Index, cmd: bytes::Bytes) -> Result<u64, Self::Error> {
    let key = u64::from_be_bytes(
      cmd
        .as_ref()
        .try_into()
        .map_err(|_| crate::DecodeError::Invalid("key"))?,
    );
    let v = self.map.entry(key).or_insert(0);
    *v += 1;
    Ok(*v)
  }

  fn snapshot(&self) -> Result<bytes::Bytes, Self::Error> {
    let mut out = Vec::with_capacity(self.map.len() * 16);
    for (k, v) in &self.map {
      out.extend_from_slice(&k.to_be_bytes());
      out.extend_from_slice(&v.to_be_bytes());
    }
    Ok(bytes::Bytes::from(out))
  }

  fn restore(&mut self, snapshot: bytes::Bytes) -> Result<(), Self::Error> {
    if !snapshot.len().is_multiple_of(16) {
      return Err(crate::DecodeError::Invalid("keyed snapshot"));
    }
    self.map.clear();
    for pair in snapshot.chunks(16) {
      let k = u64::from_be_bytes(pair[..8].try_into().expect("8-byte key"));
      let v = u64::from_be_bytes(pair[8..].try_into().expect("8-byte value"));
      self.map.insert(k, v);
    }
    Ok(())
  }

  fn split(&mut self, instruction: &[u8]) -> Option<Self> {
    let threshold = u64::from_be_bytes(instruction.try_into().ok()?);
    let child = self.map.split_off(&threshold);
    Some(Self { map: child })
  }
}

/// A splitting FSM whose `snapshot` always fails — the blob-derivation poison shape.
#[derive(Debug, Default)]
struct SnapErrSm;

impl StateMachine for SnapErrSm {
  type Command = bytes::Bytes;
  type Response = ();
  type Snapshot = bytes::Bytes;
  type Error = crate::DecodeError;

  fn apply(&mut self, _: Index, _: bytes::Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  fn snapshot(&self) -> Result<bytes::Bytes, Self::Error> {
    Err(crate::DecodeError::Invalid("capture refused"))
  }

  fn restore(&mut self, _: bytes::Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  fn split(&mut self, _instruction: &[u8]) -> Option<Self> {
    Some(Self)
  }
}

fn split_entry_data(child: u64, child_gen: u64, parent_gen_after: u64, threshold: u64) -> Bytes {
  let mut child_bytes = Vec::new();
  child.encode(&mut child_bytes);
  let payload = SplitPayload::new(
    bytes::Bytes::from(child_bytes),
    child_gen,
    parent_gen_after,
    be_key(threshold),
  );
  let mut buf = Vec::new();
  encode_split_payload(&payload, &mut buf);
  bytes::Bytes::from(buf)
}

fn follower_cfg<F: StateMachine>(fsm: F) -> (Endpoint<u64, F>, VecLog, AsyncStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(1);
  let ep = Endpoint::new(cfg, Instant::ORIGIN, 1, fsm);
  (ep, VecLog::default(), AsyncStable::default())
}

/// Deliver `entries` to a fresh follower from leader 1 at term 1, committing them all.
fn deliver_committed<F>(
  ep: &mut Endpoint<u64, F>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  entries: Vec<Entry>,
) where
  F: StateMachine,
  F::Command: crate::Data,
  F::Snapshot: crate::Data,
  F::Error: core::error::Error,
{
  let commit = entries.last().expect("non-empty").index();
  ep.handle_message(
    Instant::ORIGIN,
    log,
    stable,
    1u64,
    Message::AppendEntries(crate::AppendEntries::new(
      Term::new(1),
      1u64,
      Index::ZERO,
      Term::ZERO,
      entries,
      commit,
    )),
  );
  // Drain append completions so the durable read view covers the committed range.
  ep.handle_storage(Instant::ORIGIN, log, stable);
}

#[test]
fn committed_split_forks_at_apply_and_caps_snapshots() {
  let (mut ep, mut log, mut stable) = follower_cfg(KeyedSm::default());
  let entries = std::vec![
    Entry::new(Term::new(1), Index::new(1), EntryKind::Normal, key_cmd(3)),
    Entry::new(Term::new(1), Index::new(2), EntryKind::Normal, key_cmd(9)),
    Entry::new(
      Term::new(1),
      Index::new(3),
      EntryKind::Split,
      split_entry_data(77, 0, 1, 5),
    ),
    Entry::new(Term::new(1), Index::new(4), EntryKind::Normal, key_cmd(3)),
  ];
  deliver_committed(&mut ep, &mut log, &mut stable, entries);

  assert!(
    !ep.is_poisoned(),
    "a supported split never poisons; got {:?}",
    ep.poison_reason()
  );
  assert_eq!(
    ep.applied_index(),
    Index::new(4),
    "apply continues PAST the split (the fork is staged, not a barrier)"
  );
  // The parent kept only the below-threshold keys; the post-split entry applied to the SHRUNK map.
  assert_eq!(ep.state_machine().map, BTreeMap::from([(3, 2)]));

  // The staged fork carries the child half + the apply-derived blob + the voters at the entry.
  let (fork, child_fsm) = ep.pop_pending_fork().expect("one fork staged");
  assert_eq!(fork.index, Index::new(3));
  assert_eq!(fork.child_gen, 0);
  assert_eq!(fork.parent_gen_after, 1);
  assert_eq!(fork.voters, std::vec![1u64, 2]);
  assert_eq!(
    fork.read_only, None,
    "a never-migrated parent hands the child no explicit mode (config fallback)"
  );
  assert_eq!(
    u64::decode_exact(fork.child_bytes.clone()).expect("child id decodes"),
    77
  );
  assert_eq!(child_fsm.map, BTreeMap::from([(9, 1)]));
  assert!(
    ep.pop_pending_fork().is_none(),
    "exactly one fork was staged"
  );

  // The G-free event surfaced at the split index.
  let mut saw_split_event = false;
  while let Some(ev) = ep.poll_event() {
    if let Event::SplitApplied(sa) = ev {
      assert_eq!(sa.index(), Index::new(3));
      assert_eq!(
        u64::decode_exact(sa.child()).expect("event child id decodes"),
        77
      );
      saw_split_event = true;
    }
  }
  assert!(saw_split_event, "Event::SplitApplied surfaced");

  // THE FORK DURABILITY BARRIER: applied (4) is at/past the cap (3) and the threshold (1) is
  // long crossed, yet maybe_snapshot refuses while the fork's baseline is not locally durable —
  // a parent snapshot past the split would orphan the child's only recovery source.
  ep.maybe_snapshot(&log, &mut stable);
  assert!(
    stable.snapshot().is_none(),
    "no snapshot may be captured at/past an unlifted split index"
  );

  // Lifting at the split index clears the cap (no earlier fork remains queued) and the ordinary
  // snapshot cadence resumes, stamping the bumped lineage into the meta.
  ep.lift_snapshot_cap(Index::new(3));
  ep.maybe_snapshot(&log, &mut stable);
  let (meta, _blob) = stable.snapshot().expect("the lifted parent snapshots");
  assert_eq!(meta.last_index(), Index::new(4));
  assert_eq!(
    meta.shape_gen(),
    1,
    "the parent's snapshot meta carries parent_gen_after"
  );
}

#[test]
fn split_on_default_fsm_poisons_unsupported() {
  // CountSm never overrides `split`: a committed Split against it is a deterministic fail-stop.
  let (mut ep, mut log, mut stable) = follower_cfg(CountSm::default());
  let entries = std::vec![Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Split,
    split_entry_data(77, 0, 1, 5),
  )];
  deliver_committed(&mut ep, &mut log, &mut stable, entries);
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SplitUnsupported));
  assert_eq!(
    ep.applied_index(),
    Index::ZERO,
    "the unsupported split did not apply"
  );
}

#[test]
fn undecodable_split_payload_poisons() {
  let (mut ep, mut log, mut stable) = follower_cfg(KeyedSm::default());
  let entries = std::vec![Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Split,
    bytes::Bytes::from_static(b"\xff\xff"),
  )];
  deliver_committed(&mut ep, &mut log, &mut stable, entries);
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SplitDecode));
}

#[test]
fn blob_derivation_failure_poisons_snapshot_capture() {
  // `split` succeeds but the child half's snapshot capture fails: the blob is derived AT APPLY
  // (blob and FSM correspond by construction), so the capture failure is the committed-entry
  // fail-stop class — a split whose recovery blob cannot exist must not stage a fork.
  let (mut ep, mut log, mut stable) = follower_cfg(SnapErrSm);
  let entries = std::vec![Entry::new(
    Term::new(1),
    Index::new(1),
    EntryKind::Split,
    split_entry_data(77, 0, 1, 5),
  )];
  deliver_committed(&mut ep, &mut log, &mut stable, entries);
  assert_eq!(ep.poison_reason(), Some(PoisonReason::SnapshotCapture));
  assert!(
    ep.pop_pending_fork().is_none(),
    "no fork is staged when its blob cannot be derived"
  );
}

#[test]
fn staged_blob_corresponds_to_the_forked_half() {
  let (mut ep, mut log, mut stable) = follower_cfg(KeyedSm::default());
  let entries = std::vec![
    Entry::new(Term::new(1), Index::new(1), EntryKind::Normal, key_cmd(1)),
    Entry::new(Term::new(1), Index::new(2), EntryKind::Normal, key_cmd(8)),
    Entry::new(Term::new(1), Index::new(3), EntryKind::Normal, key_cmd(8)),
    Entry::new(
      Term::new(1),
      Index::new(4),
      EntryKind::Split,
      split_entry_data(9, 0, 1, 5),
    ),
  ];
  deliver_committed(&mut ep, &mut log, &mut stable, entries);
  let (fork, child_fsm) = ep.pop_pending_fork().expect("fork staged");

  // decode(blob) reconstructs EXACTLY the child half (the restore round-trip): the blob was
  // captured from the just-forked FSM at apply, so the two cannot diverge.
  let snapshot =
    <KeyedSm as StateMachine>::Snapshot::decode_exact(fork.blob.clone()).expect("blob decodes");
  let mut restored = KeyedSm::default();
  restored.restore(snapshot).expect("blob restores");
  assert_eq!(restored, child_fsm);
  assert_eq!(restored.map, BTreeMap::from([(8, 2)]));
}

#[test]
fn replay_after_restart_re_forks_deterministically() {
  // Crash after apply, before the fork's baseline flushed (row 1): restart from a PRE-split
  // store replays the entry, re-runs fsm.split, and re-derives the SAME blob and half — the
  // re-staged fork is byte-identical, so re-materialization converges.
  fn boot() -> (Endpoint<u64, KeyedSm>, VecLog, AsyncStable) {
    use core::time::Duration;
    let cfg = Config::try_new(
      1u64,
      std::vec![1u64],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
    .with_snapshot_threshold(1);
    let mut log = VecLog::default();
    let mut stable = AsyncStable::default();
    log.force_append(&[
      Entry::new(Term::new(1), Index::new(1), EntryKind::Normal, key_cmd(2)),
      Entry::new(Term::new(1), Index::new(2), EntryKind::Normal, key_cmd(7)),
      Entry::new(
        Term::new(1),
        Index::new(3),
        EntryKind::Split,
        split_entry_data(42, 3, 9, 5),
      ),
    ]);
    stable.force_state(Term::new(1), Some(1u64), Index::new(3));
    let ep = Endpoint::restart(
      cfg,
      Instant::ORIGIN,
      1,
      KeyedSm::default(),
      1,
      &mut log,
      &mut stable,
    );
    (ep, log, stable)
  }

  let (mut a, la, mut sa) = boot();
  let (mut b, _lb, _sb) = boot();
  assert!(!a.is_poisoned());
  assert_eq!(a.applied_index(), Index::new(3), "replay applied the split");

  let (fork_a, fsm_a) = a.pop_pending_fork().expect("replay re-staged the fork");
  let (fork_b, fsm_b) = b.pop_pending_fork().expect("replay re-staged the fork");
  assert_eq!(fork_a.child_bytes, fork_b.child_bytes);
  assert_eq!(fork_a.blob, fork_b.blob, "identical re-derived blob bytes");
  assert_eq!(
    (fork_a.child_gen, fork_a.parent_gen_after, fork_a.index),
    (3, 9, Index::new(3))
  );
  assert_eq!(fsm_a, fsm_b, "identical re-forked halves");
  assert_eq!(fsm_a.map, BTreeMap::from([(7, 1)]));
  assert_eq!(a.state_machine().map, BTreeMap::from([(2, 1)]));

  // The lineage recovered pre-replay is the DURABLE value (none here — no snapshot), while the
  // live counter already folded the replayed split: the container seeds its replay guard from
  // the former, so a re-staged fork is never mistaken for an already-relayed one.
  assert_eq!(a.restored_lineage(), 0);
  assert_eq!(a.shape_gen(), 9);

  // The re-staged fork re-arms the durability barrier: threshold long crossed, yet no capture.
  a.maybe_snapshot(&la, &mut sa);
  assert!(
    sa.snapshot().is_none(),
    "a replayed, still-unflushed fork holds the snapshot cap across restart"
  );
}

#[test]
fn restart_from_post_split_snapshot_recovers_lineage_without_reforking() {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  // A post-split snapshot at (5, term 1) carrying the bumped lineage; nothing left to replay.
  let meta = crate::SnapshotMeta::new(
    Index::new(5),
    Term::new(1),
    crate::ConfState::from_voters(std::vec![1u64]),
  )
  .with_shape_gen(9);
  let sm = KeyedSm {
    map: BTreeMap::from([(2, 1)]),
  };
  stable.force_snapshot(meta, sm_blob(&sm));
  stable.force_state(Term::new(1), Some(1u64), Index::new(5));
  log.restore(Index::new(5), Term::new(1));

  let mut ep = Endpoint::restart(
    cfg,
    Instant::ORIGIN,
    1,
    KeyedSm::default(),
    1,
    &mut log,
    &mut stable,
  );
  assert!(!ep.is_poisoned());
  assert_eq!(
    ep.pop_pending_fork().map(|_| ()),
    None,
    "a post-split snapshot restore never re-forks"
  );
  assert_eq!(ep.shape_gen(), 9, "lineage recovered from the meta");
  assert_eq!(ep.restored_lineage(), 9);
}

#[test]
fn installed_snapshot_adopts_shape_gen() {
  // A straggler that receives a post-split parent snapshot adopts its lineage, so ITS own later
  // snapshots keep carrying it (a leader that installed, then compacts, must not drop the fold).
  let (mut ep, mut log, mut stable) = follower_cfg(KeyedSm::default());
  let sm = KeyedSm {
    map: BTreeMap::from([(2, 1)]),
  };
  let meta = crate::SnapshotMeta::new(
    Index::new(7),
    Term::new(1),
    crate::ConfState::from_voters(std::vec![1u64, 2]),
  )
  .with_shape_gen(4);
  ep.handle_message(
    Instant::ORIGIN,
    &mut log,
    &mut stable,
    1u64,
    Message::InstallSnapshot(crate::InstallSnapshot::new(
      Term::new(1),
      1u64,
      meta,
      sm_blob(&sm),
    )),
  );
  // Drain the blob write + deferred install.
  ep.handle_storage(Instant::ORIGIN, &mut log, &mut stable);
  assert!(!ep.is_poisoned());
  assert_eq!(ep.applied_index(), Index::new(7), "the install landed");
  assert_eq!(ep.shape_gen(), 4, "the installed meta's lineage is adopted");
}
