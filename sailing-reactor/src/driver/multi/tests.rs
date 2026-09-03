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
    fn supports_absorb(&self) -> bool {
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
  // The direction rule makes the encoding-minimal id the survivor: group 1 is the target, group 2
  // the source that dissolves (2's LE encoding sorts strictly above 1's).
  assert!(
    super::group_idle(multi.group(&1).unwrap()),
    "the target is idle before the merge"
  );

  multi
    .prepare_merge(&2, now, &mut engine, &1)
    .unwrap()
    .unwrap();
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&2).unwrap();
    let _ = multi.handle_storage(&2, now, log, stable).unwrap();
  }
  assert!(multi.group(&2).unwrap().is_frozen());
  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi
      .commit_merge(&1, now, log, stable, &2)
      .unwrap()
      .unwrap();
  }
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&1).unwrap();
    let _ = multi.handle_storage(&1, now, log, stable).unwrap();
  }
  let target = multi.group(&1).unwrap();
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
    let (log, stable) = engine.stores(&1).unwrap();
    let _ = multi.handle_storage(&1, now, log, stable).unwrap();
  }
  resolved.extend(multi.service_merge_applies(now, &mut engine));
  assert_eq!(resolved.len(), 1);
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&1).unwrap();
    let _ = multi.handle_storage(&1, now, log, stable).unwrap();
  }
  assert!(
    super::group_idle(multi.group(&1).unwrap()),
    "the merged target settles idle after the resolution"
  );
}

/// An adopter with a STANDING owed capture is never quiesce-eligible: the adopting install moved
/// state to the cure's boundary without persisting the blob, and the per-crank merge service —
/// which a quiesced group would never reach — is the only thing that stages that capture. Driven
/// through the real container + engine (public API plus the shape-payload minting seam): a
/// restored single-voter target parked over a source hosted nowhere is cured by the adopting
/// install, then leads and settles commit == applied — and is NOT idle until the service stages
/// the owed capture, after which it settles idle again.
#[test]
fn an_owed_adopt_capture_blocks_quiescence() {
  use bytes::Bytes;
  use sailing_proto::{
    CommitMergePayload, Data, Entry, EntryKind, GroupEngine, HardState, Index, InstallSnapshot,
    Instant, LogStore, Message, MultiRaft, OpId, SnapshotMeta, StableStore, StateMachine, Term,
    conf::ConfState,
  };

  #[derive(Default)]
  struct Sm(u64);
  impl StateMachine for Sm {
    type Command = Bytes;
    type Response = u64;
    type Snapshot = u64;
    type Error = core::convert::Infallible;
    fn apply(&mut self, _: Index, _: Bytes) -> Result<u64, Self::Error> {
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
    fn supports_absorb(&self) -> bool {
      true
    }
  }

  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut multi: MultiRaft<u64, u64, Sm> = MultiRaft::new();
  let now = Instant::ORIGIN;
  let gid = 1u64;
  assert!(engine.add_group(gid));
  // A durable log parked at 2 on a `CommitMerge` naming source 42 — hosted nowhere — with a
  // committed entry above it: the boundary the cure will cover.
  let cmd = {
    let mut buf = Vec::new();
    Bytes::from_static(b"c").encode(&mut buf);
    Bytes::from(buf)
  };
  let park = {
    let mut sb = Vec::new();
    42u64.encode(&mut sb);
    sailing_proto::fuzz_internals::shape_payload::commit_merge(&CommitMergePayload::new(
      Bytes::from(sb),
      Index::new(5),
      Term::new(1),
      1,
      1,
    ))
  };
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    log.submit_append(
      OpId::ZERO,
      &[
        Entry::new(Term::new(1), Index::new(1), EntryKind::Normal, cmd.clone()),
        Entry::new(Term::new(1), Index::new(2), EntryKind::CommitMerge, park),
        Entry::new(Term::new(1), Index::new(3), EntryKind::Normal, cmd),
      ],
    );
    stable.submit_write(
      OpId::ZERO,
      HardState::initial()
        .with_term(Term::new(1))
        .with_commit(Index::new(3))
        .with_vote(Some(1u64)),
    );
  }
  engine.flush();
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .restore_group_unchecked(
        gid,
        Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap(),
        now,
        7,
        Sm::default(),
        1,
        log,
        stable,
      )
      .unwrap();
  }
  assert!(
    multi.group(&gid).unwrap().pending_merge().is_some(),
    "parked over a source hosted nowhere"
  );
  // The service classifies the park as needing a cure and walks the interval; the cure adopts.
  assert!(multi.service_merge_applies(now, &mut engine).is_empty());
  let blob = {
    let mut buf = Vec::new();
    5u64.encode(&mut buf);
    Bytes::from(buf)
  };
  let meta = SnapshotMeta::new(
    Index::new(3),
    Term::new(1),
    ConfState::from_voters(vec![1u64]),
  )
  .with_shape_gen(1);
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .handle_message(
        &gid,
        now,
        log,
        stable,
        9u64,
        Message::InstallSnapshot(InstallSnapshot::new(Term::new(1), 9u64, meta, blob)),
      )
      .unwrap();
  }
  assert!(
    multi.group(&gid).unwrap().pending_merge().is_none(),
    "the cure adopted the union"
  );
  assert!(
    multi.group(&gid).unwrap().adopt_capture_owed(),
    "and the adopt owes its forced capture"
  );
  // The adopter leads and settles — commit == applied, its own match at commit — the shape every
  // other quiesce leg accepts.
  let d = multi.group(&gid).unwrap().poll_timeout().unwrap();
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi.handle_timeout(&gid, d, log, stable).unwrap();
  }
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&gid).unwrap();
    let _ = multi.handle_storage(&gid, d, log, stable).unwrap();
  }
  let ep = multi.group(&gid).unwrap();
  assert!(ep.role().is_leader(), "leading");
  assert_eq!(ep.commit_index(), ep.applied_index(), "settled");
  assert!(
    !super::group_idle(ep),
    "an owed adopt capture is standing merge work — never quiesce-eligible"
  );

  // The service stages the owed capture; once it drains, the adopter is idle again.
  assert!(multi.service_merge_applies(now, &mut engine).is_empty());
  for _ in 0..4 {
    engine.flush();
    let (log, stable) = engine.stores(&gid).unwrap();
    let _ = multi.handle_storage(&gid, d, log, stable).unwrap();
  }
  assert!(
    !multi.group(&gid).unwrap().adopt_capture_owed(),
    "the owed capture staged"
  );
  assert!(
    super::group_idle(multi.group(&gid).unwrap()),
    "the adopter settles idle once its owed capture is staged"
  );
}

/// Quiesce-ineligibility of pending farewell work through the container, the CAUGHT-UP arm: a leader
/// that removes a caught-up voter fires a commit-carrying heartbeat and — with the both-arms fix —
/// still owes blind re-deliveries (the removed peer's ack is unobservable), so `group_idle` refuses
/// the group until the budget drains, else a quiesced group would strand the remaining shots.
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

  // Remove node 3.
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
  // Node 3 ACKS the removal (match = 2): its ack completes the quorum {1,3}, the change applies, and
  // node 3 is pruned CAUGHT-UP (match >= removal) — so the fold fires the commit-carrying HEARTBEAT
  // arm and, with the both-arms fix, still populates the retry budget.
  {
    let (log, stable) = engine.stores(&gid).unwrap();
    multi
      .handle_message(
        &gid,
        td,
        log,
        stable,
        3u64,
        Message::AppendResponse(AppendResponse::new(
          Term::new(1),
          3u64,
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
  // Node 2 then catches up to the removal (match = 2), so the surviving voter is not lagging — the
  // group's ONLY reason to stay non-idle is now the pending caught-up farewell.
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

/// The factory's PRE-BUILD gate refuses a debt-named gid, and the debt leg is the only conjunct
/// that does. Post-defer the consumed source id looks entirely admissible — no tombstone, no
/// terminal floor (both arrive only at the discharge), no split reservation — so without this leg
/// a solicitation for it would materialize a fresh husk beside the union its preserved stores
/// still back. Driven through the real container + engine on the public API: a parked fork's
/// standing barrier defers the absorb's capture into a debt, then each gate conjunct is evaluated
/// at the seam the driver reads it from. The async pump itself is out of scope here; what is
/// pinned is that the conjunction refuses, and refuses ONLY because of the debt.
#[test]
fn the_factory_gate_refuses_a_debt_named_source_and_releases_at_the_discharge() {
  use sailing_proto::{
    FloorStore, GroupEngine, Index, InstallOutcome, Instant, MergeResolution, MultiRaft, NoHold,
    StateMachine, StorageProgress, floor_admits,
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
    fn split(&mut self, instruction: &[u8]) -> Option<Self> {
      let give = u64::from(*instruction.first()?).min(self.0);
      self.0 -= give;
      Some(Self(give))
    }
    fn absorb(&mut self, source: Self) -> bool {
      self.0 += source.0;
      true
    }
    fn supports_split(&self) -> bool {
      true
    }
    fn supports_absorb(&self) -> bool {
      true
    }
  }

  let cfg = || Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  let now = Instant::ORIGIN;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut multi: MultiRaft<u64, u64, Sm> = MultiRaft::new();

  // `drain` settles one group's storage; `elect` drives a single voter to leadership.
  macro_rules! drain {
    ($gid:expr, $at:expr) => {
      for _ in 0..8 {
        engine.flush();
        let (log, stable) = engine.stores(&$gid).unwrap();
        if !matches!(
          multi.handle_storage(&$gid, $at, log, stable),
          Some(StorageProgress::MorePending)
        ) {
          break;
        }
      }
      engine.flush();
      {
        let (log, stable) = engine.stores(&$gid).unwrap();
        let _ = multi.handle_storage(&$gid, $at, log, stable);
      }
    };
  }
  let boot = |multi: &mut MultiRaft<u64, u64, Sm>, engine: &mut GroupEngine<u64, u64>, gid| {
    assert!(engine.add_group(gid));
    multi
      .create_group(gid, 0, cfg(), now, 7, Sm::default())
      .unwrap();
    let d = multi.group(&gid).unwrap().poll_timeout().unwrap();
    {
      let (log, stable) = engine.stores(&gid).unwrap();
      multi.handle_timeout(&gid, d, log, stable).unwrap();
    }
    for _ in 0..4 {
      engine.flush();
      let (log, stable) = engine.stores(&gid).unwrap();
      let _ = multi.handle_storage(&gid, d, log, stable).unwrap();
    }
    assert!(multi.group(&gid).unwrap().role().is_leader());
    d
  };

  // Group 1 leads and takes on load, then splits out child 300 — whose id is ALREADY hosted by
  // the time the committed split applies (the reachable conflict shape: an admission the
  // propose-time gate could not see). The fork parks and its durability barrier stands.
  let d = boot(&mut multi, &mut engine, 1u64);
  for _ in 0..3 {
    {
      let (log, stable) = engine.stores(&1).unwrap();
      multi
        .propose(&1, d, log, stable, &bytes::Bytes::from_static(b"c"))
        .unwrap()
        .unwrap();
    }
    drain!(1u64, d);
  }
  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi
      .propose_split(
        &1,
        d,
        log,
        stable,
        &300,
        0,
        bytes::Bytes::from_static(b"\x02"),
      )
      .unwrap()
      .unwrap();
  }
  assert!(engine.add_group(300));
  multi
    .create_group(300, 0, cfg(), d, 43, Sm::default())
    .unwrap();
  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi.flush_appends(&1, d, log, stable).unwrap();
  }
  drain!(1u64, d);
  assert!(
    multi.peek_yieldable_fork(&NoHold).is_none(),
    "the fork parks on the hosted child, leaving its barrier standing"
  );

  // Group 2 freezes into group 1. The standing fork barrier turns the absorb into a DEFER: the
  // union applies and serves, and its capture becomes a debt naming the consumed source.
  let ds = boot(&mut multi, &mut engine, 2u64);
  {
    let (log, stable) = engine.stores(&2).unwrap();
    multi
      .propose(&2, ds, log, stable, &bytes::Bytes::from_static(b"c"))
      .unwrap()
      .unwrap();
  }
  drain!(2u64, ds);
  multi
    .prepare_merge(&2, ds, &mut engine, &1)
    .unwrap()
    .unwrap();
  drain!(2u64, ds);
  assert!(multi.group(&2).unwrap().is_frozen());
  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi.commit_merge(&1, d, log, stable, &2).unwrap().unwrap();
  }
  drain!(1u64, d);
  assert!(multi.group(&1).unwrap().pending_merge().is_some(), "parked");
  assert!(multi.service_merge_applies(d, &mut engine).is_empty());
  drain!(1u64, d);
  assert_eq!(
    multi.service_merge_applies(d, &mut engine),
    vec![MergeResolution::Absorbed {
      source: 2,
      target: 1
    }]
  );

  // The gate's conjuncts, at the seams the driver reads them from. A gen-0 blueprint naming the
  // solicitor is the ordinary factory answer for a solicited id.
  let blueprint = GroupBlueprint::new(
    Config::try_new(1u64, vec![1, 2], ELECTION, HEARTBEAT).unwrap(),
    0,
  );
  assert!(blueprint_names(&blueprint, &2), "the solicitor is named");
  assert!(
    floor_admits(engine.floor(&2), blueprint.generation()),
    "no terminal floor yet — it arrives only with the discharge"
  );
  assert!(
    !multi.split_reserved(&2),
    "no split reserves the absorbed source's id"
  );
  assert!(
    multi.debt_names(&2),
    "the debt leg is the ONLY conjunct refusing — without it the factory builds a husk"
  );

  // The discharge releases the gate: the conflict resolves, the fence lifts, the capture stages,
  // and the id then falls to the ordinary terminal-floor refusal instead.
  multi.remove_group(&300, &mut engine).unwrap();
  // The driver drops the removed group's storage too; without that the engine still reports the
  // id occupied and the relay keeps holding on it.
  engine.remove_group(&300);
  let InstallOutcome::Installed { split_index, .. } =
    multi.install_yieldable_fork(&1, &300, &mut engine, &NoHold, d, 43)
  else {
    panic!("the fork survived the wait and installs once its squatter leaves")
  };
  multi.lift_fork_barrier(&1, split_index);
  assert_eq!(
    multi.service_merge_applies(d, &mut engine),
    vec![MergeResolution::Merged {
      source: 2,
      target: 1
    }]
  );
  assert!(
    !multi.debt_names(&2),
    "the debt window's refusal is self-releasing"
  );
}

/// THE DRIVER'S TEARDOWN AFTER A FAILED ABSORB CAPTURE. `CaptureFailed` leaves the consumed
/// source's stores in the engine as the union's only restart derivation, and the driver's remove
/// command runs the container's participant gate FIRST — before its floor write and its engine
/// teardown — so the refusal here is the driver's refusal: the source's stores stay in the engine,
/// its floor stays 0 (a removal floor admits above itself, and a factory build there would
/// manufacture a blank source for the re-parked merge to trip on), and the factory gate's debt
/// conjunct keeps refusing the id. The poisoned holder refuses its own removal too; the restart
/// is the exit. The state machine advertises absorb support and refuses the fold — the
/// deterministic fail-stop shape.
#[test]
fn a_failed_absorb_capture_keeps_the_sources_stores_in_the_engine() {
  use sailing_proto::{
    CreateGroupError, GroupEngine, Index, Instant, MergeResolution, MultiRaft, RemoveError,
    StateMachine, StorageProgress,
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
    // Advertised so the propose gate admits the merge; `absorb` keeps its refusing default.
    fn supports_absorb(&self) -> bool {
      true
    }
  }

  let cfg = || Config::try_new(1u64, vec![1], ELECTION, HEARTBEAT).unwrap();
  let now = Instant::ORIGIN;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  let mut multi: MultiRaft<u64, u64, Sm> = MultiRaft::new();

  macro_rules! drain {
    ($gid:expr, $at:expr) => {
      for _ in 0..8 {
        engine.flush();
        let (log, stable) = engine.stores(&$gid).unwrap();
        if !matches!(
          multi.handle_storage(&$gid, $at, log, stable),
          Some(StorageProgress::MorePending)
        ) {
          break;
        }
      }
      engine.flush();
      {
        let (log, stable) = engine.stores(&$gid).unwrap();
        let _ = multi.handle_storage(&$gid, $at, log, stable);
      }
    };
  }
  let boot = |multi: &mut MultiRaft<u64, u64, Sm>, engine: &mut GroupEngine<u64, u64>, gid| {
    assert!(engine.add_group(gid));
    multi
      .create_group(gid, 0, cfg(), now, 7, Sm::default())
      .unwrap();
    let d = multi.group(&gid).unwrap().poll_timeout().unwrap();
    {
      let (log, stable) = engine.stores(&gid).unwrap();
      multi.handle_timeout(&gid, d, log, stable).unwrap();
    }
    for _ in 0..4 {
      engine.flush();
      let (log, stable) = engine.stores(&gid).unwrap();
      let _ = multi.handle_storage(&gid, d, log, stable).unwrap();
    }
    assert!(multi.group(&gid).unwrap().role().is_leader());
    d
  };

  // Group 2 takes on state and freezes into group 1; group 1 commits the absorb and parks.
  let d = boot(&mut multi, &mut engine, 1u64);
  let ds = boot(&mut multi, &mut engine, 2u64);
  {
    let (log, stable) = engine.stores(&2).unwrap();
    multi
      .propose(&2, ds, log, stable, &bytes::Bytes::from_static(b"c"))
      .unwrap()
      .unwrap();
  }
  drain!(2u64, ds);
  multi
    .prepare_merge(&2, ds, &mut engine, &1)
    .unwrap()
    .unwrap();
  drain!(2u64, ds);
  assert!(multi.group(&2).unwrap().is_frozen());
  {
    let (log, stable) = engine.stores(&1).unwrap();
    multi.commit_merge(&1, d, log, stable, &2).unwrap().unwrap();
  }
  drain!(1u64, d);
  assert!(multi.group(&1).unwrap().pending_merge().is_some(), "parked");
  assert!(
    multi.service_merge_applies(d, &mut engine).is_empty(),
    "the first pass seals the window"
  );
  drain!(1u64, d);
  assert_eq!(
    multi.service_merge_applies(d, &mut engine),
    vec![MergeResolution::CaptureFailed {
      source: 2,
      target: 1
    }],
    "the refused fold surfaces CaptureFailed"
  );
  assert!(multi.group(&1).unwrap().is_poisoned());
  assert!(
    !multi.contains_group(&2),
    "the source endpoint was consumed"
  );
  assert!(
    engine.contains_group(&2) && engine.stores(&2).is_some(),
    "the source's stores survive the consumption"
  );

  // The driver's remove command for the source: the container gate refuses before any floor
  // write or engine teardown, so the stores and the floor are exactly as CaptureFailed left them.
  assert!(
    matches!(
      multi.remove_group(&2, &mut engine),
      Err(RemoveError::SpokenFor)
    ),
    "the pinned source refuses removal"
  );
  assert!(engine.contains_group(&2), "nothing was torn down");
  assert_eq!(engine.group_floor(&2), 0, "no removal floor was written");
  assert!(
    multi.debt_names(&2),
    "the factory gate's debt conjunct refuses the pinned id"
  );
  assert!(
    matches!(
      multi.create_group(2, 0, cfg(), d, 7, Sm::default()),
      Err(CreateGroupError::AbsorbPending)
    ),
    "the pinned id refuses admission"
  );
  assert!(
    matches!(
      multi.remove_group(&1, &mut engine),
      Err(RemoveError::OwesRecovery)
    ),
    "the poisoned holder refuses its own removal: the teardown would shed the pin"
  );
  assert!(engine.contains_group(&1), "the holder's stores stay too");
}

/// A SPLIT READS TWO IDS, so its floor seam must be keyed by id. Both of the coordinator's split
/// legs consult it — the parent's own incarnation and the caller's claim about the child — and a
/// one-id snapshot answers whatever it was built from for BOTH of them. Built from the child, it
/// carries a terminally floored parent's doomed split through on an unfloored child, and fences a
/// live parent behind a child legitimately recreated above a nonzero floor.
#[test]
fn a_split_floor_seam_answers_each_id_its_own_floor() {
  use sailing_proto::{FloorStore, GroupEngine};

  use super::{FloorSnapshot, PairFloors};

  const PARENT: u64 = 1;
  const CHILD: u64 = 2;
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  engine.add_group(PARENT);
  engine.add_group(CHILD);
  engine.set_group_floor(&PARENT, sailing_proto::MERGED_FLOOR);
  engine.flush();

  // The child-keyed snapshot reports the CHILD's floor for the parent, so the parent's terminal
  // fence disappears at exactly the leg that reads it.
  let child_only = FloorSnapshot {
    floor: FloorStore::floor(&engine, &CHILD),
    lineage: FloorStore::lineage(&engine, &CHILD),
  };
  assert_eq!(FloorStore::floor(&child_only, &CHILD), 0);
  assert_eq!(
    FloorStore::floor(&child_only, &PARENT),
    0,
    "a one-id seam loses the parent's fence — the direction that lets a doomed split through"
  );

  // The pair snapshot answers each id from its own record.
  let pair = PairFloors::snapshot(&engine, &PARENT, &CHILD);
  assert_eq!(
    FloorStore::floor(&pair, &PARENT),
    sailing_proto::MERGED_FLOOR,
    "a terminally floored parent still reads as fenced"
  );
  assert_eq!(FloorStore::floor(&pair, &CHILD), 0);

  // THE OTHER DIRECTION. A child recreated above a nonzero floor is legitimate, and a live parent
  // at a lower generation must not inherit that floor.
  let mut engine: GroupEngine<u64, u64> = GroupEngine::new();
  engine.add_group(PARENT);
  engine.add_group(CHILD);
  engine.set_group_floor(&CHILD, 7);
  engine.flush();
  let child_only = FloorSnapshot {
    floor: FloorStore::floor(&engine, &CHILD),
    lineage: FloorStore::lineage(&engine, &CHILD),
  };
  assert_eq!(
    FloorStore::floor(&child_only, &PARENT),
    7,
    "a one-id seam hands the child's floor to the parent — the false-rejection direction"
  );
  let pair = PairFloors::snapshot(&engine, &PARENT, &CHILD);
  assert_eq!(
    FloorStore::floor(&pair, &PARENT),
    0,
    "a live parent keeps its own floor whatever the child was recreated above"
  );
  assert_eq!(FloorStore::floor(&pair, &CHILD), 7);
}

/// THE TWO FLOOR REFUSALS REACH A CALLER DISTINGUISHABLY. Both proto shapes carry a single `floor`
/// field, so the drivers' `{:?}` reason would render them identically — and they need opposite
/// recovery: reroute or retire a fenced parent, versus raise the child generation. A caller that
/// cannot tell them apart retries the cure that cannot work, forever.
#[test]
fn the_split_floor_refusals_surface_with_their_participant() {
  use sailing_proto::{ProposeError, SplitError};

  use super::map_split_err;
  use crate::DriverError;

  let parent: SplitError<u64> = SplitError::Propose(ProposeError::BelowFloor {
    floor: sailing_proto::MERGED_FLOOR,
  });
  let child: SplitError<u64> = SplitError::BelowFloor { floor: 7 };

  let (DriverError::Rejected { reason: p }, DriverError::Rejected { reason: c }) =
    (map_split_err(parent), map_split_err(child))
  else {
    panic!("both floor refusals map to the drivers' rejection shape");
  };
  assert_ne!(p, c, "the two refusals must not read identically");
  assert!(
    p.contains("parent"),
    "the parent's refusal names the parent: {p}"
  );
  assert!(
    c.contains("child"),
    "the child's refusal names the child: {c}"
  );
}
