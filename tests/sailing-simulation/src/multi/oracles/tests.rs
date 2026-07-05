use super::*;
use crate::MultiWorld;
use sailing_proto::Term;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn gkv_roundtrips_and_rejects_foreign_shapes() {
  let cmd = encode_gkv(100, 7, 42);
  assert_eq!(cmd.len(), 18, "gid(8) ++ key(2) ++ value(8)");
  assert_eq!(decode_gkv(&cmd), Some((100, 7, 42)));
  // Non-gkv payloads (empty, conf-change, ad-hoc test commands) decode to None and are skipped.
  assert_eq!(decode_gkv(b""), None);
  assert_eq!(decode_gkv(b"a-100"), None);
  assert_eq!(decode_gkv(&[0u8; 17]), None);
  assert_eq!(decode_gkv(&[0u8; 19]), None);
}

/// Cross-talk teeth: a forged applied entry carrying ANOTHER group's gid-tagged payload must
/// panic the sweep at the exact (node, gid). Synthetic view — no live world can produce this
/// without the very routing bug the oracle exists to catch.
#[test]
#[should_panic(expected = "cross-group")]
fn cross_talk_sweep_panics_on_a_foreign_gid_tag() {
  let forged = [(4u64, encode_gkv(200, 3, 9))]; // applied under group 100, tagged for 200
  assert_no_cross_talk(7, 12, 0, 100, &std::collections::BTreeSet::new(), &forged);
}

/// The sweep stays quiet on the group's own tag and on untagged (non-gkv) payloads.
#[test]
fn cross_talk_sweep_accepts_own_tag_and_untagged_payloads() {
  let own = [
    (4u64, encode_gkv(100, 3, 9)),
    (5u64, b"raw".to_vec()),
    (6u64, Vec::new()),
  ];
  assert_no_cross_talk(7, 12, 0, 100, &std::collections::BTreeSet::new(), &own);
}

/// One-identity teeth: a second distinct grantee for the same (granter, gid, gen, term) is the
/// double-vote class the oracle guards (the P3b map-skew shape).
#[test]
#[should_panic(expected = "one-identity")]
fn one_identity_panics_on_a_second_grantee_same_incarnation() {
  let mut grants: BTreeMap<GrantKey, u64> = BTreeMap::new();
  note_grant(&mut grants, 7, 12, (1, 100, 0, Term::new(3)), 2);
  note_grant(&mut grants, 7, 13, (1, 100, 0, Term::new(3)), 3);
}

/// The recreation edge: the SAME (granter, gid, term) at gen+1 is a fresh incarnation — terms
/// restart across recreations, so a grant there is legitimate, not a double vote.
#[test]
fn one_identity_allows_the_same_term_across_incarnations() {
  let mut grants: BTreeMap<GrantKey, u64> = BTreeMap::new();
  note_grant(&mut grants, 7, 12, (1, 100, 0, Term::new(3)), 2);
  note_grant(&mut grants, 7, 13, (1, 100, 1, Term::new(3)), 3); // gen bumped: allowed
  // And a duplicate grant to the SAME candidate stays fine (idempotent under dup/reorder).
  note_grant(&mut grants, 7, 14, (1, 100, 0, Term::new(3)), 2);
}

/// Live: the Task-1 shape under gid-tagged payloads, with the per-group checker suites and the
/// cross-talk sweep running on every tick and once more explicitly at the end.
#[test]
fn live_world_runs_green_under_gkv_payloads() {
  let mut w = MultiWorld::new(11);
  for n in 0..3 {
    w.add_node(n);
  }
  let all: BTreeSet<u64> = (0..3).collect();
  w.create_group(100, &all);
  w.create_group(200, &all);
  assert!(w.run_until(400, |w| w.leader_of(100).is_some()
    && w.leader_of(200).is_some()));
  assert!(w.propose(100, &encode_gkv(100, 1, 1)).is_some());
  assert!(w.propose(200, &encode_gkv(200, 1, 2)).is_some());
  assert!(w.run_until(400, |w| {
    w.agreement_holds(100)
      && w.agreement_holds(200)
      && (0..3).all(|n| !w.applied_of(n, 100).is_empty() && !w.applied_of(n, 200).is_empty())
  }));
  w.check_now();
}
