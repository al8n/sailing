//! Endpoint hot-path benchmarks: a baseline for the per-round allocation cost the audit's
//! perf-mediums target (`peers()` BTreeSet, `committed_index` Vec sort, `vote_result` allocs,
//! `maybe_send_append` Progress clone, `apply_committed` per-entry store calls).
//!
//! A single leader is driven with the sim's in-memory stores and synthesized follower
//! responses — realistic full rounds, not micro-isolation of each allocator. A perf-medium fix
//! that removes a per-round allocation moves the round number; that pairing is the point.

#![allow(missing_docs)] // criterion_group! generates an undocumented public item

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use sailing_proto::{
  AppendResponse, Config, Endpoint, HeartbeatResponse, Index, Instant, Message, ReadOnlyOption,
  Term, VoteResponse,
};
use sailing_simulation::{LogSm, MemLog, MemStable};
use std::{hint::black_box, time::Duration};

const ELECTION: Duration = Duration::from_millis(1000);
const HEARTBEAT: Duration = Duration::from_millis(100);
/// The lease window a synthesized follower advertises — non-zero so the leader's lease /
/// `vote_result` bookkeeping actually runs (it is gated on `lease_support > 0`).
const LEASE_SUPPORT: Duration = ELECTION;

type Ep = Endpoint<u64, LogSm>;
type Harness = (Ep, MemLog, MemStable<u64>, Instant);

/// Drive the leader to FULL quiescence: repeatedly drain its outbound messages and answer each
/// (every `AppendEntries` with an `AppendResponse` at the implied match, every `Heartbeat` with a
/// lease-bearing `HeartbeatResponse`), making each resulting commit durable, until the leader emits
/// nothing more. This brings EVERY follower (not just a quorum) to `match == last` / Replicate
/// and drains all setup residue — the true steady state, so the benches time their claimed round
/// and not leftover catch-up traffic.
fn settle(ep: &mut Ep, log: &mut MemLog, stable: &mut MemStable<u64>, now: Instant) {
  let term = ep.term();
  for _ in 0..64 {
    // The highest match each peer implies from this drain (and any heartbeats to answer).
    let mut appends: std::collections::BTreeMap<u64, Index> = std::collections::BTreeMap::new();
    let mut beats: std::vec::Vec<(u64, Bytes, u64)> = std::vec::Vec::new();
    let mut drained = false;
    while let Some(out) = ep.poll_message() {
      drained = true;
      let peer = out.to();
      match out.message() {
        Message::AppendEntries(ae) => {
          let end = Index::new(ae.prev_log_index().get() + ae.entries().len() as u64);
          let slot = appends.entry(peer).or_insert(Index::ZERO);
          if end > *slot {
            *slot = end;
          }
        }
        Message::Heartbeat(hb) => beats.push((peer, hb.context_bytes(), hb.lease_round())),
        _ => {}
      }
    }
    if !drained {
      return; // quiescent: every follower is caught up
    }
    for (peer, m) in appends {
      ep.handle_message(
        now,
        log,
        stable,
        peer,
        Message::AppendResponse(AppendResponse::new(
          term,
          peer,
          false,
          Index::ZERO,
          Term::ZERO,
          m,
        )),
      );
    }
    for (peer, ctx, lease) in beats {
      ep.handle_message(
        now,
        log,
        stable,
        peer,
        Message::HeartbeatResponse(
          HeartbeatResponse::new(term, peer, ctx)
            .with_lease_round(lease)
            .with_lease_support(LEASE_SUPPORT),
        ),
      );
    }
    let _ = ep.handle_storage(now, log, stable); // commit advances; apply_committed runs
  }
  panic!("the harness leader failed to quiesce within 64 settle rounds");
}

/// Build an `n`-voter cluster's leader (node 0) at a real STEADY state: campaign, durably
/// self-vote, collect a quorum of votes, then SETTLE the become-leader no-op — driving EVERY
/// follower to Replicate so it is committed-and-applied and no catch-up traffic remains. Without
/// settling, the leader retransmits the unacked no-op and the benches would time retransmission,
/// not their claimed rounds. CheckQuorum + LeaseBased are armed so the lease path is live.
fn elect_leader(n: usize) -> Harness {
  let voters: Vec<u64> = (0..n as u64).collect();
  let cfg = Config::try_new(0, voters, ELECTION, HEARTBEAT)
    .expect("valid config")
    .with_check_quorum(true)
    .with_read_only(ReadOnlyOption::LeaseBased);
  let mut ep: Ep = Endpoint::new(cfg, Instant::ORIGIN, 0, LogSm::new());
  let mut log = MemLog::new();
  let mut stable = MemStable::<u64>::new();

  let now = ep
    .poll_timeout()
    .expect("a fresh voter arms an election timer");
  ep.handle_timeout(now, &mut log, &mut stable); // → Candidate, campaigns
  let _ = ep.handle_storage(now, &mut log, &mut stable); // self-vote durable
  let term = ep.term();
  // A strict majority is the self-vote plus floor(n/2) peers.
  for from in 1..=(n / 2) as u64 {
    ep.handle_message(
      now,
      &mut log,
      &mut stable,
      from,
      Message::VoteResponse(VoteResponse::new(term, from, false, false)),
    );
  }
  let _ = ep.handle_storage(now, &mut log, &mut stable); // become_leader appends + stores the no-op
  settle(&mut ep, &mut log, &mut stable, now); // commit + apply it; every follower → Replicate
  assert!(
    ep.role().is_leader(),
    "the harness must elect a leader (n={n})"
  );
  assert!(
    ep.commit_index() > Index::ZERO,
    "the election no-op must be committed (n={n})"
  );
  (ep, log, stable, now)
}

/// One steady-state heartbeat round (no new entries ⇒ commit does not advance ⇒ repeatable):
/// fire the leader's heartbeat timer, then answer every outgoing message — a `Heartbeat` with a
/// lease-bearing `HeartbeatResponse`, any stray `AppendEntries` with an `AppendResponse` — so
/// `recent_active` and the lease quorum stay live (keeping check-quorum from stepping the leader
/// down) and the `peers()`/broadcast/`vote_result`/lease work actually runs.
fn heartbeat_round(ep: &mut Ep, log: &mut MemLog, stable: &mut MemStable<u64>, now: &mut Instant) {
  *now = ep.poll_timeout().unwrap_or(*now); // the next due (heartbeat) deadline
  ep.handle_timeout(*now, log, stable);
  enum Answer {
    Hb(u64, Term, Bytes, u64),
    Ae(u64, Term, Index),
  }
  let mut answers = std::vec::Vec::new();
  while let Some(out) = ep.poll_message() {
    let peer = out.to();
    match out.message() {
      Message::Heartbeat(hb) => answers.push(Answer::Hb(
        peer,
        hb.term(),
        hb.context_bytes(),
        hb.lease_round(),
      )),
      Message::AppendEntries(ae) => answers.push(Answer::Ae(
        peer,
        ae.term(),
        Index::new(ae.prev_log_index().get() + ae.entries().len() as u64),
      )),
      _ => {}
    }
  }
  for a in answers {
    match a {
      Answer::Hb(peer, term, ctx, lease) => ep.handle_message(
        *now,
        log,
        stable,
        peer,
        Message::HeartbeatResponse(
          HeartbeatResponse::new(term, peer, ctx)
            .with_lease_round(lease)
            .with_lease_support(LEASE_SUPPORT),
        ),
      ),
      Answer::Ae(peer, term, m) => ep.handle_message(
        *now,
        log,
        stable,
        peer,
        Message::AppendResponse(AppendResponse::new(
          term,
          peer,
          false,
          Index::ZERO,
          Term::ZERO,
          m,
        )),
      ),
    }
  }
  let _ = ep.handle_storage(*now, log, stable);
}

/// Propose one entry and carry it to committed-and-applied via a quorum of `AppendResponse`s.
/// Exercises `committed_index` (the per-ack match-vector sort), `maybe_send_append`, and
/// `apply_committed`. RETURNS the harness so criterion drops it OUTSIDE the timed interval
/// (otherwise the teardown of the endpoint + stores pollutes a sub-µs measurement).
fn replicate_one(mut h: Harness, n: usize) -> Harness {
  {
    let (ep, log, stable, now) = (&mut h.0, &mut h.1, &mut h.2, h.3);
    let cmd = Bytes::from_static(b"payload");
    let idx = ep
      .propose(now, log, &*stable, &cmd)
      .expect("leader accepts the proposal");
    let _ = ep.handle_storage(now, log, stable);
    let term = ep.term();
    while ep.poll_message().is_some() {} // we synthesize the acks directly
    for from in 1..=(n / 2) as u64 {
      ep.handle_message(
        now,
        log,
        stable,
        from,
        Message::AppendResponse(AppendResponse::new(
          term,
          from,
          false,
          Index::ZERO,
          Term::ZERO,
          idx,
        )),
      );
    }
    let _ = ep.handle_storage(now, log, stable); // commit advances; apply_committed runs
    black_box(idx);
  }
  h
}

fn bench_heartbeat_round(c: &mut Criterion) {
  let mut g = c.benchmark_group("heartbeat_round");
  for n in [3usize, 5] {
    g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
      let (mut ep, mut log, mut stable, mut now) = elect_leader(n);
      b.iter(|| heartbeat_round(&mut ep, &mut log, &mut stable, &mut now));
    });
  }
  g.finish();
}

fn bench_replicate_one(c: &mut Criterion) {
  let mut g = c.benchmark_group("replicate_one");
  for n in [3usize, 5] {
    g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
      b.iter_batched(
        || elect_leader(n),
        |h| replicate_one(h, n),
        BatchSize::SmallInput,
      );
    });
  }
  g.finish();
}

criterion_group!(benches, bench_heartbeat_round, bench_replicate_one);
criterion_main!(benches);
