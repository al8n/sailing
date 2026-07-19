use core::time::Duration;

use sailing_driver::GroupBlueprint;
use sailing_proto::Config;

use super::{blueprint_names, reshape_born_factory_config, reshape_born_prevention};

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);

/// Reshape-born prevention: a split child / factory-materialized group's config is FORCED
/// to run pre-vote + check-quorum, whatever the seed config carried — the exact transformation the
/// host applies at every reshape-birth site before building the endpoint. The library defaults are
/// off (etcd parity), so this proves the force actually flips both.
#[test]
fn reshape_born_config_forces_pre_vote_and_check_quorum() {
  let base = Config::try_new(1u64, vec![1, 2, 3], ELECTION, HEARTBEAT).unwrap();
  assert!(!base.pre_vote(), "the library default leaves pre-vote off");
  assert!(
    !base.check_quorum(),
    "the library default leaves check-quorum off"
  );
  let forced = reshape_born_prevention(base);
  assert!(forced.pre_vote(), "a reshape-born group runs pre-vote");
  assert!(
    forced.check_quorum(),
    "a reshape-born group runs check-quorum"
  );
}

/// The factory gate forces prevention only on the reshape/rejoin subset. A day-0 full-voter blueprint
/// (generation 0, self a voter) keeps the caller's config byte-for-byte in BOTH directions; a
/// fork-born observer blueprint (self absent from the seed voters) or a reshaped incarnation
/// (generation > 0) is forced. Observer-shape is the fork-born marker precisely because a full-voter
/// fork-born blueprint fuses histories (the loopback `full_voter_blueprint_for_a_fork_born_id_fuses_histories`).
#[test]
fn factory_gate_forces_only_reshape_born_blueprints() {
  // Day-0, flags OFF in: gen 0, self (1) a voter → passthrough, both stay off.
  let passed = reshape_born_factory_config(
    0,
    Config::try_new(1u64, vec![1, 2, 3], ELECTION, HEARTBEAT).unwrap(),
  );
  assert!(
    !passed.pre_vote() && !passed.check_quorum(),
    "a day-0 full-voter blueprint keeps the caller's config (both off in, both off out)"
  );
  // Day-0, flags ON in: gen 0, self a voter → passthrough, both stay on (byte-for-byte).
  let passed_on = reshape_born_factory_config(
    0,
    Config::try_new(1u64, vec![1, 2, 3], ELECTION, HEARTBEAT)
      .unwrap()
      .with_pre_vote(true)
      .with_check_quorum(true),
  );
  assert!(
    passed_on.pre_vote() && passed_on.check_quorum(),
    "a day-0 blueprint passes the caller's flags through unchanged"
  );
  // Fork-born observer: gen 0, self (3) ABSENT from the seed voters → forced.
  let forced_obs = reshape_born_factory_config(
    0,
    Config::try_new_observer(3u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
  );
  assert!(
    forced_obs.pre_vote() && forced_obs.check_quorum(),
    "a fork-born observer blueprint is forced even at generation 0"
  );
  // Reshaped incarnation: gen > 0, self a voter → forced.
  let forced_gen = reshape_born_factory_config(
    5,
    Config::try_new(1u64, vec![1, 2, 3], ELECTION, HEARTBEAT).unwrap(),
  );
  assert!(
    forced_gen.pre_vote() && forced_gen.check_quorum(),
    "a reshaped incarnation (generation > 0) is forced"
  );
}

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

/// Quiesce-ineligibility of pending farewell work through the container: a leader that removes a
/// lagging voter still owes blind farewell re-deliveries (the removed peer's ack is unobservable), so
/// `group_idle` refuses the group until the budget drains — otherwise a quiesced group would strand
/// the remaining shots until unrelated traffic woke it.
#[test]
fn a_pending_farewell_blocks_quiescence() {
  use core::time::Duration;
  use sailing_proto::{
    AppendResponse, ConfChange, ConfChangeType, GroupEngine, Index, Instant, Message, MultiRaft,
    StateMachine, Term, VoteResponse,
  };

  #[derive(Default)]
  struct Sm(u64);
  impl StateMachine for Sm {
    type Command = bytes::Bytes;
    type Response = u64;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: Index, _: bytes::Bytes) -> Result<u64, Self::Error> {
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
  }

  let gid = 1u64;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut multi: MultiRaft<u64, u64, Sm> = MultiRaft::new();
  assert!(engine.add_group(gid));
  multi
    .create_group(
      gid,
      0,
      Config::try_new(1u64, vec![1, 2, 3], ELECTION, HEARTBEAT).unwrap(),
      Instant::ORIGIN,
      7,
      Sm::default(),
    )
    .unwrap();

  macro_rules! drain {
    () => {{
      while multi.poll_message().is_some() {}
      while multi.poll_event().is_some() {}
    }};
  }
  macro_rules! crank {
    ($now:expr) => {{
      for _ in 0..4 {
        engine.flush();
        let (log, stable) = engine.stores(&gid).unwrap();
        let _ = multi.handle_storage(&gid, $now, log, stable).unwrap();
      }
      drain!();
    }};
  }

  // Elect node 1 (a 3-voter group needs node 2's grant).
  let td = multi.group(&gid).unwrap().poll_timeout().unwrap();
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi.handle_timeout(&gid, td, log, stable).unwrap();
  }
  drain!();
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .handle_message(
        &gid,
        td,
        log,
        stable,
        2u64,
        Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
      )
      .unwrap();
  }
  drain!();
  crank!(td);
  assert!(multi.group(&gid).unwrap().role().is_leader());

  // Node 2 acks the no-op@1 (commit it); node 3 never acks, so it stays at match 0.
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .handle_message(
        &gid,
        td,
        log,
        stable,
        2u64,
        Message::AppendResponse(AppendResponse::new(
          Term::new(1),
          2u64,
          false,
          Index::ZERO,
          Term::ZERO,
          Index::new(1),
        )),
      )
      .unwrap();
  }
  drain!();
  crank!(td);

  // Remove node 3; node 2's ack completes the quorum and the change applies, pruning node 3 lagging
  // (match 0 < removal index) — so the fold fires an APPEND farewell and populates the retry budget.
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .propose_conf_change(
        &gid,
        td,
        log,
        stable,
        ConfChange::new(ConfChangeType::RemoveNode, 3u64, bytes::Bytes::new()),
      )
      .unwrap()
      .unwrap();
  }
  drain!();
  crank!(td);
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .handle_message(
        &gid,
        td,
        log,
        stable,
        2u64,
        Message::AppendResponse(AppendResponse::new(
          Term::new(1),
          2u64,
          false,
          Index::ZERO,
          Term::ZERO,
          Index::new(2),
        )),
      )
      .unwrap();
  }
  drain!();
  crank!(td);

  // The group is otherwise quiesce-eligible — node 2 caught up, commit == applied — but the leader
  // still owes farewell re-deliveries, so it is NOT idle.
  assert!(multi.group(&gid).unwrap().has_pending_farewells());
  assert!(
    !super::group_idle(multi.group(&gid).unwrap()),
    "a pending farewell blocks quiescence"
  );

  // Drain the budget over its bounded ticks (a schedule tick, then two election-timeout-spaced shots).
  let et = ELECTION;
  for step in [
    Duration::from_millis(150),
    Duration::from_millis(150) + et,
    Duration::from_millis(150) + et + et,
  ] {
    {
      let (log, stable) = engine.stores(&gid).unwrap();
      multi.handle_timeout(&gid, td + step, log, stable).unwrap();
    }
    crank!(td + step);
  }
  assert!(
    !multi.group(&gid).unwrap().has_pending_farewells(),
    "the blind budget drained"
  );
  assert!(
    super::group_idle(multi.group(&gid).unwrap()),
    "idle-eligible once the farewell budget drains"
  );
}
