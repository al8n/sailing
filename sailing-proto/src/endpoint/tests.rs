use super::*;
use crate::{
  AppendResponse, Entry, Instant, VoteResponse,
  testkit::{AsyncStable, CountSm, NoopStable, VecLog},
};
use core::time::Duration;

struct Noop;

impl StateMachine for Noop {
  type Command = bytes::Bytes;
  type Response = ();
  type Snapshot = ();
  type Error = core::convert::Infallible;

  fn apply(&mut self, _: Index, _: bytes::Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  fn snapshot(&self) -> Result<(), Self::Error> {
    Ok(())
  }

  fn restore(&mut self, _: ()) -> Result<(), Self::Error> {
    Ok(())
  }
}

// --- restart test ---

/// Encode a Bytes command through the Data codec (as propose does internally).
fn encode_cmd(b: &[u8]) -> bytes::Bytes {
  use crate::Data;
  let mut buf = Vec::new();
  bytes::Bytes::copy_from_slice(b).encode(&mut buf);
  bytes::Bytes::from(buf)
}

// ---- snapshot threshold + deferred compaction ----

/// Helper: elect a single-node leader, drain the no-op, and apply `n` Normal entries.
/// Returns the endpoint with `applied == n + 1` (no-op + n commands, all committed).
fn make_single_node_leader_with_entries(
  n: usize,
  threshold: usize,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(threshold);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // campaign
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain no-op (LeaderAppend for index 1).
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Propose and commit `n` Normal entries one at a time.
  for i in 0..n {
    let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
    // Drain storage each time to let the self-append complete (quorum=1: auto-commits).
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }
  (ep, log, stable)
}

/// Like `make_single_node_leader_with_entries`, but the stable store is armed to DROP the
/// `SnapshotWritten` completion of the threshold-crossing snapshot while still making the blob
/// durable. Models a store that coalesces/loses the completion. After this returns,
/// `pending_compact` is `Some`, the durable snapshot is readable, but no completion is queued.
fn make_single_node_leader_dropping_snapshot_completion(
  n: usize,
  threshold: usize,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_snapshot_threshold(threshold);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  // The threshold is crossed exactly once during the drive, so the only `submit_snapshot` is
  // the one whose completion we want dropped — arming at the start is sufficient and precise.
  stable.drop_next_snapshot_completion();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // campaign
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable); // drain no-op
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  for i in 0..n {
    let cmd = bytes::Bytes::copy_from_slice(&[i as u8]);
    let _ = ep.propose(d, &mut log, &stable, &cmd).unwrap();
    ep.handle_storage(d, &mut log, &mut stable);
    while ep.poll_message().is_some() {}
    while ep.poll_event().is_some() {}
  }
  (ep, log, stable)
}

// ---- send InstallSnapshot to lagging follower ----

/// Helper: build a 3-voter leader (node 1) with a compacted log.
/// Returns the endpoint, a VecLog compacted up to `offset` with the snapshot persisted
/// in an AsyncStable, and the stable store.
///
/// Log after setup: entries [offset+1 ..= offset+n_tail], first_index = offset + 1.
/// Stable holds a snapshot with last_index = offset.
fn make_leader_with_compacted_log(
  offset: u64,
  n_tail: usize,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{
    Config, Entry, EntryKind, Index, Instant, Message, Term, VoteResponse, conf::ConfState,
  };
  use core::time::Duration;

  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_max_size_per_msg(u64::MAX);

  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();

  // Elect node 1 as leader.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain again so the no-op append completion is processed.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}

  // Compact the log up to `offset`: seed boundary entries then compact.
  if offset > 0 {
    // Force the log to have entries 1..=offset+n_tail to give compact() something to drop.
    let all: Vec<Entry> = (1u64..=offset + n_tail as u64)
      .map(|i| {
        Entry::new(
          Term::new(1),
          Index::new(i),
          EntryKind::Normal,
          bytes::Bytes::from_static(b"x"),
        )
      })
      .collect();
    log.force_append(&all);
    // Compact up to offset, retaining entries [offset+1 ..= offset+n_tail].
    log.compact(Index::new(offset));
  }

  // Persist a snapshot with last_index = offset in stable.
  let meta = crate::SnapshotMeta::new(
    Index::new(offset),
    Term::new(1),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  let data = bytes::Bytes::from_static(b"snap-data");
  stable.submit_snapshot(crate::OpId::new(99), meta, data);
  // Drain the SnapshotWritten completion so stable.snapshot() is readable.
  while stable.poll().is_some() {}

  (ep, log, stable)
}

// ---- heartbeat-driven snapshot resend (no wedge on dropped InstallSnapshot) ----

/// Helper: drive `make_leader_with_compacted_log` peer 2 into Snapshot state and DROP the
/// resulting InstallSnapshot (clear the outgoing queue), simulating the §11 message loss.
/// Returns the leader, log, stable, and the snapshot's pending index (= offset).
fn wedged_snapshot_follower(
  offset: u64,
  n_tail: usize,
) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Index) {
  use crate::Index;

  let (mut ep, log, stable) = make_leader_with_compacted_log(offset, n_tail);

  // Peer 2 far behind: next_index < first_index = offset + 1.
  if let Some(p) = ep.tracker.progress_mut(&2u64) {
    p.become_probe();
    p.set_next_index(Index::new(2));
  }

  // First send: emits the InstallSnapshot and moves peer 2 into Snapshot(offset).
  ep.maybe_send_append(crate::Now::monotonic(Instant::ORIGIN), 2u64, &log, &stable);
  assert!(
    ep.tracker.progress(&2u64).unwrap().state().is_snapshot(),
    "peer 2 must be in Snapshot state after the first send"
  );

  // DROP the InstallSnapshot — simulate the loss by clearing the outgoing queue.
  while ep.poll_message().is_some() {}

  (ep, log, stable, Index::new(offset))
}

// ---- InstallSnapshot receive + SnapshotResponse ----

/// Encode a `u64` snapshot value into a `Bytes` blob (the wire format used by CountSm).
fn encode_snapshot(v: u64) -> bytes::Bytes {
  use crate::Data as _;
  let mut buf = Vec::new();
  v.encode(&mut buf);
  bytes::Bytes::from(buf)
}

/// Build a follower endpoint (node 2 in a 3-voter cluster, term 1) with an empty log.
fn make_follower() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{Config, Instant};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let ep = Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default());
  let log = VecLog::default();
  let stable = AsyncStable::default();
  (ep, log, stable)
}

// ---- restore-from-snapshot on restart ----

/// Build a `CountSm` snapshot blob for the given count value.
fn encode_count_snapshot(count: u64) -> bytes::Bytes {
  use crate::Data as _;
  let mut buf = Vec::new();
  count.encode(&mut buf);
  bytes::Bytes::from(buf)
}

// ── deferred snapshot install (golden, core-enforced durability ordering) ──────────

/// A 3-voter follower (node 2) at term 2 with a durable, committed log `[1..=3]` (commit=3), ready to
/// receive a snapshot install. Returns `(ep, log, stable, cfg)` — `cfg` for a later `restart`.
fn follower_committed_to_3() -> (Endpoint<u64, CountSm>, VecLog, AsyncStable, Config<u64>) {
  use crate::{AppendEntries, Config, Entry, EntryKind, Index, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg.clone(), Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = AsyncStable::default();
  let d = Instant::ORIGIN;
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    1u64,
    Message::AppendEntries(AppendEntries::new(
      Term::new(2),
      1u64,
      Index::ZERO,
      Term::ZERO,
      std::vec![
        Entry::new(
          Term::new(2),
          Index::new(1),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(2),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
        Entry::new(
          Term::new(2),
          Index::new(3),
          EntryKind::Empty,
          bytes::Bytes::new()
        ),
      ],
      Index::new(3),
    )),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_message().is_some() {}
  (ep, log, stable, cfg)
}

fn install_at(boundary: u64) -> Message<u64> {
  use crate::{Index, InstallSnapshot, Message, SnapshotMeta, Term, conf::ConfState};
  let meta = SnapshotMeta::new(
    Index::new(boundary),
    Term::new(2),
    ConfState::from_voters(std::vec![1u64, 2u64, 3u64]),
  );
  Message::InstallSnapshot(InstallSnapshot::new(
    Term::new(2),
    1u64,
    meta,
    encode_count_snapshot(boundary),
  ))
}

// ── propose_conf_change + apply-at-commit tests ────────────────────────────────────────

/// Helper: build a single-node leader (node 1) with a VecLog + NoopStable, and drain storage
/// so the no-op entry at index 1 is committed and applied. Returns (ep, log, stable, d).
fn make_single_node_leader() -> (Endpoint<u64, CountSm>, VecLog, NoopStable, Instant) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 42, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // campaign (quorum=1)
  // Self-vote must be durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain again so the no-op at index 1 commits and applies.
  ep.handle_storage(d, &mut log, &mut stable);
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  (ep, log, stable, d)
}

// ── leader step-down on self-removal/demotion ─────────────────────────────────────────

/// Helper: elect node 1 as leader of a 3-voter cluster {1, 2, 3}, drive the no-op to
/// committed+applied, then return (ep, log, stable, d).
fn make_three_node_leader() -> (Endpoint<u64, CountSm>, VecLog, NoopStable, Instant) {
  use crate::{Message, Term, VoteResponse};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // candidate
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain storage again: no-op LeaderAppend fires → self match → commit advances.
  ep.handle_storage(d, &mut log, &mut stable);
  // Need peer ack to commit the no-op in a 3-voter cluster (quorum=2).
  use crate::{AppendResponse, Index};
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  (ep, log, stable, d)
}

// ─── CheckQuorum tests ────────────────────────────────────────────────────────────────

/// Helper: build a Config with check_quorum=true for a cluster of `voters` with 1s/100ms.
fn cq_config(id: u64, voters: Vec<u64>) -> Config<u64> {
  Config::try_new(
    id,
    voters,
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true)
}

// ── ReadIndex tests ─────────────────────────────────────────────────────────────────────

/// Helper: elect node 1 leader in a 3-voter cluster, drain the no-op so the leader has
/// a committed current-term entry.  Returns (ep, log, stable, now).
fn make_leader_with_current_term_commit() -> (Endpoint<u64, CountSm>, VecLog, NoopStable, Instant) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable); // candidate
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain storage again: no-op LeaderAppend fires → self match_index advances.
  ep.handle_storage(d, &mut log, &mut stable);
  // Peer 2 acks the no-op → quorum (self + peer2) → commit advances to 1.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  (ep, log, stable, d)
}

// ---- self-validating lease regressions ----

/// Build a `LeaseBased + check_quorum` leader at term 1 with a current-term commit (no lease yet).
fn leasebased_leader() -> (Endpoint<u64, CountSm>, VecLog, NoopStable) {
  use crate::{Config, Instant, Message, Term};
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseBased);
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  ep.handle_storage(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_event().is_some() {}
  while ep.poll_message().is_some() {}
  (ep, log, stable)
}

/// Fire one CheckQuorum heartbeat round; return (now, the round's lease token).
fn tick_lease_round(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut NoopStable,
) -> (Instant, u64) {
  let at = ep.poll_timeout().expect("a timer is armed");
  ep.handle_timeout(at, log, stable);
  let mut lr = None;
  while let Some(out) = ep.poll_message() {
    if let Message::Heartbeat(hb) = out.message() {
      lr = Some(hb.lease_round());
    }
  }
  (at, lr.expect("the heartbeat carried a lease round"))
}

// ---- persist the lease-support PROMISE across restart (config-drift safety) ----

/// Build a fresh enforcing follower (node 2 in {1,2,3}, check_quorum on, LeaseBased) at term 0 on an
/// async store. Drive it with `follower_advertised_support` to exercise the persist-before-advertise gate.
fn enforcing_follower(et: Duration) -> (Endpoint<u64, CountSm>, VecLog, AsyncStable) {
  use crate::{Config, Instant};
  let cfg = Config::try_new(
    2u64,
    std::vec![1u64, 2u64, 3u64],
    et,
    Duration::from_millis(50),
  )
  .unwrap()
  .with_check_quorum(true)
  .with_read_only(ReadOnlyOption::LeaseBased);
  (
    Endpoint::new(cfg, Instant::ORIGIN, 7, CountSm::default()),
    VecLog::default(),
    AsyncStable::default(),
  )
}

/// Deliver one Heartbeat (leader 1, term `t`, lease round `r`) and return the `lease_support` the
/// follower advertised in its HeartbeatResponse.
fn follower_advertised_support(
  ep: &mut Endpoint<u64, CountSm>,
  log: &mut VecLog,
  stable: &mut AsyncStable,
  now: Instant,
  t: u64,
  r: u64,
) -> Duration {
  use crate::Message;
  ep.handle_message(
    now,
    log,
    stable,
    1u64,
    Message::Heartbeat(
      crate::Heartbeat::new(Term::new(t), 1u64, Index::ZERO, bytes::Bytes::new())
        .with_lease_round(r),
    ),
  );
  let mut support = None;
  while let Some(out) = ep.poll_message() {
    if let Message::HeartbeatResponse(hr) = out.message() {
      support = Some(hr.lease_support());
    }
  }
  support.expect("the follower produced a HeartbeatResponse")
}

// ─── leader transfer tests ────────────────────────────────────────────

/// Elect node 1 as leader and return (ep, log, stable) ready for transfer tests.
/// The log has the no-op at index 1 committed; peer 2's match_index is caught up.
fn setup_leader_with_peer2_caught_up() -> (Endpoint<u64, CountSm>, VecLog, NoopStable) {
  use core::time::Duration;
  let cfg = Config::try_new(
    1u64,
    std::vec![1u64, 2u64, 3u64],
    Duration::from_millis(1000),
    Duration::from_millis(100),
  )
  .unwrap();
  let mut ep = Endpoint::new(cfg, Instant::ORIGIN, 1, CountSm::default());
  let mut log = VecLog::default();
  let mut stable = NoopStable::default();

  // Elect node 1.
  let d = ep.poll_timeout().unwrap();
  ep.handle_timeout(d, &mut log, &mut stable);
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::VoteResponse(VoteResponse::new(Term::new(1), 2u64, false, false)),
  );
  // Self-vote must become durable before become_leader fires (persist-before-ACT).
  ep.handle_storage(d, &mut log, &mut stable);
  assert!(ep.role().is_leader());
  // Drain the no-op append from storage so self-match advances.
  ep.handle_storage(d, &mut log, &mut stable);
  // Peer 2 acks the no-op (index 1) → match_index=1.
  ep.handle_message(
    d,
    &mut log,
    &mut stable,
    2u64,
    Message::AppendResponse(AppendResponse::new(
      Term::new(1),
      2u64,
      false,
      Index::ZERO,
      Term::ZERO,
      Index::new(1),
    )),
  );
  while ep.poll_message().is_some() {}
  while ep.poll_event().is_some() {}
  (ep, log, stable)
}

// ── fatal apply_committed errors poison (no silent stall) + carry a cause ──────

/// A state machine whose `apply` returns `Err` for a sentinel command. `Error` is a real
/// `core::error::Error` (the §6.3 bound). Used to exercise the `PoisonReason::Apply` path.
#[derive(Debug, Default)]
struct FailSm;

/// Apply failure for `FailSm`. Implements `core::error::Error` (available under both std and
/// no_std) so it satisfies the `apply_committed` bound without pulling in `std` — keeps the
/// test module compiling under `--no-default-features --features alloc`.
#[derive(Debug)]
struct FailSmError;

impl core::fmt::Display for FailSmError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("apply failed")
  }
}

impl core::error::Error for FailSmError {}

impl StateMachine for FailSm {
  type Command = Bytes;
  type Response = usize;
  type Snapshot = u64;
  type Error = FailSmError;

  fn apply(&mut self, _index: Index, cmd: Bytes) -> Result<usize, Self::Error> {
    // Sentinel: a single 0xFF byte means "fail". Any other payload applies successfully.
    if cmd.as_ref() == [0xFFu8] {
      return Err(FailSmError);
    }
    Ok(cmd.len())
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(0)
  }

  fn restore(&mut self, _snapshot: u64) -> Result<(), Self::Error> {
    Ok(())
  }
}

/// Encode `payload` as a `Normal` entry's `data` using the `Bytes` codec (length-prefixed),
/// so `<F::Command as Data>::decode` reads it back as the SM command.
fn normal_entry(term: u64, index: u64, payload: &[u8]) -> Entry {
  use crate::Data as _;
  let mut buf = Vec::new();
  bytes::Bytes::copy_from_slice(payload).encode(&mut buf);
  Entry::new(
    Term::new(term),
    Index::new(index),
    crate::EntryKind::Normal,
    bytes::Bytes::from(buf),
  )
}

// Tests are split by concern into these submodules.
mod election;
mod lease;
mod leaseguard;
mod membership;
mod misc;
mod read_index;
mod replication;
mod restart;
mod snapshot;
mod transfer;
