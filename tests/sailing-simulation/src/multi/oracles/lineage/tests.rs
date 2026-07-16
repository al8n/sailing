use super::LineageLedger;
use sailing_proto::{ForkId, Index, Term};

/// A distinct fork provenance token for `child`, minted from synthetic split coordinates.
fn token(child: u8) -> ForkId {
  ForkId::new(
    bytes::Bytes::from_static(&[100]),
    1,
    Index::new(5),
    Term::new(2),
    bytes::Bytes::copy_from_slice(&[child]),
    0,
  )
}

/// CHIMERA teeth: a replica that committed TOKEN-LESS content then installs a FORK-lineage
/// snapshot has fused two lineages' coordinate proofs in one durable state — exactly the
/// destructive install the door gate refuses. The ledger must trip at finalize.
#[test]
#[should_panic(expected = "chimera")]
fn chimera_trips_on_a_fork_install_over_token_less_content() {
  let mut l = LineageLedger::new();
  let fork = token(200);
  // The squatter commits its own token-less content.
  l.observe_content(
    7,
    10,
    (0, 200, 0),
    None,
    &[(1, b"own-a".to_vec()), (2, b"own-b".to_vec())],
  );
  // A genuine fork baseline (a DIFFERENT lineage) installs over it.
  l.observe_install(7, 11, (0, 200, 0), Some(&fork), 1);
  l.finalize_or_panic(7);
}

/// The MUST-KEEP pristine adopter: an EMPTY replica (no committed content) installing a fork
/// baseline is legitimate — a lineage is adopted from nothing. No chimera.
#[test]
fn no_chimera_when_a_pristine_replica_adopts_a_fork_baseline() {
  let mut l = LineageLedger::new();
  let fork = token(200);
  // No prior content observed: the replica is pristine.
  l.observe_install(7, 11, (1, 200, 0), Some(&fork), 1);
  // Post-install content in the fork lineage is consistent.
  l.observe_content(7, 12, (1, 200, 0), Some(&fork), &[(1, b"base".to_vec())]);
  l.finalize_or_panic(7); // no panic
}

/// The MUST-KEEP kin retransfer: a replica already bearing a token re-installs the SAME token
/// (an interrupted adoption completing on the retransfer). Same lineage — no chimera.
#[test]
fn no_chimera_on_a_same_token_retransfer() {
  let mut l = LineageLedger::new();
  let fork = token(200);
  l.observe_install(7, 11, (1, 200, 0), Some(&fork), 1);
  l.observe_content(7, 12, (1, 200, 0), Some(&fork), &[(1, b"base".to_vec())]);
  // The leader retransfers the identical baseline; the kin slot re-installs freely.
  l.observe_install(7, 13, (1, 200, 0), Some(&fork), 1);
  l.finalize_or_panic(7); // no panic
}

/// A recreated incarnation opens pristine: the ledger keys per `(node, gid, generation)`, so a
/// fresh generation installing a new lineage over the id is not a chimera (the group re-creation
/// is a legitimate content reset, distinct from a same-incarnation cross-lineage install).
#[test]
fn no_chimera_across_a_recreation_reset() {
  let mut l = LineageLedger::new();
  let fork = token(200);
  l.observe_content(7, 10, (0, 200, 0), None, &[(1, b"own".to_vec())]);
  // gen 1: a fresh incarnation adopts a fork baseline from nothing (a different generation key).
  l.observe_install(7, 11, (0, 200, 1), Some(&fork), 1);
  l.finalize_or_panic(7); // no panic
}

/// PHANTOM-QUORUM teeth: two replicas agree on `(gid, lineage, index)` but hold DIFFERENT command
/// bytes — a within-lineage split brain the lineage-keyed agreement leg catches.
#[test]
#[should_panic(expected = "phantom quorum")]
fn phantom_quorum_trips_on_divergent_bytes_within_one_lineage() {
  let mut l = LineageLedger::new();
  l.observe_content(7, 10, (0, 100, 0), None, &[(3, b"alpha".to_vec())]);
  l.observe_content(7, 10, (1, 100, 0), None, &[(3, b"beta".to_vec())]);
  l.finalize_or_panic(7);
}

/// The lineage key is load-bearing: the SAME `(gid, index)` legitimately holds different bytes in
/// DIFFERENT lineages (a fork boundary re-uses coordinates across lineages). No phantom.
#[test]
fn no_phantom_across_distinct_lineages_at_one_coordinate() {
  let mut l = LineageLedger::new();
  let a = token(200);
  let b = token(201);
  l.observe_content(7, 10, (0, 100, 0), Some(&a), &[(3, b"alpha".to_vec())]);
  l.observe_content(7, 10, (1, 100, 0), Some(&b), &[(3, b"beta".to_vec())]);
  // Identical bytes within one lineage across replicas are fine (the agreement case).
  l.observe_content(7, 11, (2, 100, 0), Some(&a), &[(3, b"alpha".to_vec())]);
  l.finalize_or_panic(7); // no panic
}

/// WEDGE teeth: an admitted transfer that stays in Snapshot with no chunk/install/refusal progress
/// past the budget is a wedge. Feed a stuck cursor across a wide tick gap.
#[test]
#[should_panic(expected = "wedge")]
fn wedge_trips_on_a_stalled_admitted_transfer() {
  let mut l = LineageLedger::new();
  // The transfer opens at tick 0 (match 1, cursor 0, no refusals)…
  l.observe_transfer(7, 0, 100, 2, Some((1, 0)), 0);
  // …and is still exactly there far past the budget.
  l.observe_transfer(7, super::WEDGE_BUDGET + 1, 100, 2, Some((1, 0)), 0);
  l.finalize_or_panic(7);
}

/// A chunked transfer whose cursor keeps advancing is progress, never a wedge — and a peer the
/// world reports unreachable (`None`) clears the window rather than accruing a false wedge.
#[test]
fn no_wedge_while_the_chunk_cursor_advances_or_the_peer_is_unreachable() {
  let mut l = LineageLedger::new();
  l.observe_transfer(7, 0, 100, 2, Some((1, 0)), 0);
  // The chunk cursor climbs each observation — real progress well past the budget span.
  l.observe_transfer(7, super::WEDGE_BUDGET / 2, 100, 2, Some((1, 512)), 0);
  l.observe_transfer(7, super::WEDGE_BUDGET + 5, 100, 2, Some((1, 1024)), 0);
  // Then the peer goes unreachable: the window clears, no wedge.
  l.observe_transfer(7, super::WEDGE_BUDGET * 2, 100, 2, None, 0);
  l.finalize_or_panic(7); // no panic
}

/// A refusal is a terminal resolution: a leader pinned in Snapshot toward a squatter whose refusal
/// count climbs is progressing (the standing-conflict posture the gate resolves by placement), not
/// wedged.
#[test]
fn no_wedge_when_the_refusal_count_climbs() {
  let mut l = LineageLedger::new();
  l.observe_transfer(7, 0, 100, 2, Some((0, 0)), 1);
  // Same match and cursor, but the refusal count keeps climbing on each resend/refuse.
  l.observe_transfer(7, super::WEDGE_BUDGET + 1, 100, 2, Some((0, 0)), 9);
  l.finalize_or_panic(7); // no panic
}
