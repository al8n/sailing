use core::time::Duration;

use sailing_driver::GroupBlueprint;
use sailing_proto::Config;

use super::blueprint_names;

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// The plain bootstrap shape: the seed voters authorize exactly themselves — a peer outside the
/// list is refused even though the group id and the blueprint itself are valid.
#[test]
fn a_blueprint_names_exactly_its_seed_voters() {
  let blueprint = GroupBlueprint::new(
    Config::try_new(2u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
    0,
  );
  assert!(blueprint_names(&blueprint, &1));
  assert!(blueprint_names(&blueprint, &2));
  assert!(
    !blueprint_names(&blueprint, &3),
    "a solicitor the seed config does not name is refused"
  );
}

/// The learner-join (observer) shape: the joining HOST's own id is absent from the seed voters
/// by construction (`try_new_observer`), and that own id never authorizes a remote solicitor —
/// only the voter list does, which names the soliciting leader.
#[test]
fn an_observer_seed_names_the_remote_voters_not_its_own_id() {
  let blueprint = GroupBlueprint::new(
    Config::try_new_observer(3u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
    0,
  );
  assert!(
    blueprint_names(&blueprint, &1),
    "the soliciting leader is named by the seed voters"
  );
  assert!(
    !blueprint_names(&blueprint, &3),
    "the config's own id is the HOST identity, not solicitor authorization"
  );
}

/// A target with a PARKED merge is never quiesce-eligible: the park is resolved by the
/// per-crank merge service, which a quiesced group would never reach. Driven through the real
/// container + engine (public API only): two single-voter groups, freeze → parked commit, then
/// the idle predicate — and after the service resolves, the merged target settles idle again.
#[test]
fn a_parked_merge_blocks_quiescence() {
  use sailing_proto::{GroupEngine, Instant, MultiRaft, StateMachine};

  #[derive(Default)]
  struct Sm(u64);
  impl StateMachine for Sm {
    type Command = bytes::Bytes;
    type Response = u64;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: sailing_proto::Index, _: bytes::Bytes) -> Result<u64, Self::Error> {
      self.0 += 1;
      Ok(self.0)
    }
    fn snapshot(&self) -> Result<u64, Self::Error> {
      Ok(self.0)
    }
    fn restore(&mut self, s: u64) -> Result<(), Self::Error> {
      self.0 = s;
      Ok(())
    }
    fn absorb(&mut self, source: Self) -> bool {
      self.0 += source.0;
      true
    }
  }

  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut multi: MultiRaft<u64, u64, Sm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  for gid in [1u64, 2] {
    assert!(engine.add_group(gid));
    multi
      .create_group(
        gid,
        0,
        Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap(),
        now,
        7,
        Sm::default(),
      )
      .unwrap();
    // Elect the single voter: campaign, then flush+drain until the no-op applies.
    let d = multi.group(&gid).unwrap().poll_timeout().unwrap();
    let (log, stable) = engine.stores(&gid).unwrap();
    multi.handle_timeout(&gid, d, log, stable).unwrap();
    for _ in 0..4 {
      engine.flush();
      let (log, stable) = engine.stores(&gid).unwrap();
      let _ = multi.handle_storage(&gid, d, log, stable).unwrap();
    }
    assert!(multi.group(&gid).unwrap().role().is_leader());
  }
  assert!(
    super::group_idle(multi.group(&2).unwrap()),
    "the target is idle before the merge"
  );

  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi
      .prepare_merge(&1, now, log, stable, &2)
      .unwrap()
      .unwrap();
  }
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&1).unwrap();
    let _ = multi.handle_storage(&1, now, log, stable).unwrap();
  }
  assert!(multi.group(&1).unwrap().is_frozen());
  {
    let (log, stable) = engine.stores(&2).unwrap();
    multi
      .commit_merge(&2, now, log, stable, &1)
      .unwrap()
      .unwrap();
  }
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&2).unwrap();
    let _ = multi.handle_storage(&2, now, log, stable).unwrap();
  }
  let target = multi.group(&2).unwrap();
  assert!(target.pending_merge().is_some(), "parked");
  assert!(
    !super::group_idle(target),
    "a parked merge is never quiesce-eligible"
  );

  // The service resolves it: the first pass seals the park's abort window (a leader no-op at
  // the coordinate after the parked entry), the drain commits the seal, the next pass absorbs;
  // once the resumed drain settles, the merged target is idle again.
  let mut resolved = multi.service_merge_applies(now, &mut engine);
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&2).unwrap();
    let _ = multi.handle_storage(&2, now, log, stable).unwrap();
  }
  resolved.extend(multi.service_merge_applies(now, &mut engine));
  assert_eq!(resolved.len(), 1);
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&2).unwrap();
    let _ = multi.handle_storage(&2, now, log, stable).unwrap();
  }
  assert!(
    super::group_idle(multi.group(&2).unwrap()),
    "the merged target settles idle after the resolution"
  );
}
