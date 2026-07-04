//! The multi-group oracle helpers: the gid-tagged keyed-value payload codec, the per-apply
//! cross-group cross-talk sweep, and the one-identity grants tripwire.
//!
//! Cross-talk: every client payload the multi tier proposes carries its group id
//! ([`encode_gkv`]), so a routing/delivery/apply leak across groups is caught at the exact
//! `(node, gid, index)` in O(1) per applied entry — the always-on form of the isolation oracle
//! (the replay-and-diff variant is a deep-soak opt-in, deferred).
//!
//! One-identity: a node grants at most one REAL vote per `(group, incarnation, term)` across
//! every replica object it ever hosts for that group. The incarnation (gen) is in the key
//! because a recreated group restarts terms — `(granter, gid, term)` alone would false-positive
//! across incarnations, and dropping gen entirely would miss the re-admission double-vote class.

use sailing_proto::Term;
use std::{collections::BTreeMap, vec::Vec};

/// Encode a gid-tagged keyed-value client command: `gid` (8 bytes LE) ++ `key` (2 bytes LE) ++
/// `value` (8 bytes LE) — 18 bytes exactly.
#[cfg_attr(
  not(test),
  expect(dead_code, reason = "the multi VOPR client load wires this")
)]
pub(crate) fn encode_gkv(gid: u64, key: u16, value: u64) -> Vec<u8> {
  let mut buf = Vec::with_capacity(18);
  buf.extend_from_slice(&gid.to_le_bytes());
  buf.extend_from_slice(&key.to_le_bytes());
  buf.extend_from_slice(&value.to_le_bytes());
  buf
}

/// Decode a gid-tagged keyed-value command, the inverse of [`encode_gkv`]. `Some((gid, key,
/// value))` iff `cmd` is EXACTLY 18 bytes; `None` otherwise — empty / conf-change entries and
/// ad-hoc untagged test payloads carry no tag and are skipped by the cross-talk sweep.
pub(crate) fn decode_gkv(cmd: &[u8]) -> Option<(u64, u16, u64)> {
  if cmd.len() != 18 {
    return None;
  }
  let gid = u64::from_le_bytes(cmd[0..8].try_into().expect("8-byte gid"));
  let key = u16::from_le_bytes([cmd[8], cmd[9]]);
  let value = u64::from_le_bytes(cmd[10..18].try_into().expect("8-byte value"));
  Some((gid, key, value))
}

/// Assert that none of `new_entries` — the entries applied under `gid` on `node` since the last
/// sweep — carries ANOTHER group's tag. A mismatch is a cross-group leak (wrong-group delivery
/// or apply) and panics with the exact seed/tick/node/gid/index for replay.
pub(crate) fn assert_no_cross_talk(
  seed: u64,
  tick: u64,
  node: u64,
  gid: u64,
  new_entries: &[(u64, Vec<u8>)],
) {
  for (index, cmd) in new_entries {
    if let Some((tag, key, value)) = decode_gkv(cmd) {
      assert!(
        tag == gid,
        "cross-group leak: node {node} applied an entry tagged for group {tag} under group \
         {gid} (index={index} key={key} value={value})\n  seed={seed} tick={tick} \
         (replay: run_multi_vopr(seed, ticks) and inspect tick {tick})",
      );
    }
  }
}

/// The one-identity grant key: `(granter, gid, generation, term)`. Gen is the group's INCARNATION
/// (bumped by a harness recreation), never reused for a different logical group.
pub(crate) type GrantKey = (u64, u64, u64, Term);

/// Record a REAL-vote grant under `key = (granter, gid, generation, term)` and assert
/// one-identity: a second DISTINCT grantee under the same key is a double vote (fatal). A
/// duplicate grant to the SAME candidate is fine (idempotency under duplication/reorder), and
/// the same `(granter, gid, term)` at another generation is a different incarnation (terms
/// restart across recreations).
pub(crate) fn note_grant(
  grants: &mut BTreeMap<GrantKey, u64>,
  seed: u64,
  tick: u64,
  key: GrantKey,
  grantee: u64,
) {
  let (granter, gid, generation, term) = key;
  match grants.get(&key) {
    Some(&prev) => assert_eq!(
      prev, grantee,
      "one-identity violated: node {granter} granted its vote for group {gid} (gen {generation}) \
       in term {term:?} to both {prev} and {grantee}\n  seed={seed} tick={tick}",
    ),
    None => {
      grants.insert(key, grantee);
    }
  }
}

#[cfg(test)]
mod tests;
