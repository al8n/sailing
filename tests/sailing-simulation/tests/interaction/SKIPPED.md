# Interaction corpus — etcd scenarios NOT ported (feature gaps)

This directory ports etcd/raft's `testdata/*.txt` data-driven scenarios to sailing (see
`../../src/interaction.rs`). All **27** *applicable* scenarios are ported and pass the
`interaction_corpus` gate.

Four of etcd's 28 testdata files are **not ported** because they exercise features sailing does not
implement. They are listed here so the coverage gap is explicit, not silent.

| etcd scenario | Why it can't be ported |
|---|---|
| `forget_leader` | Exercises etcd's `MsgForgetLeader` (a follower voluntarily forgetting its leader so a healthier node can take over). sailing has no `ForgetLeader` message. |
| `forget_leader_prevote_checkquorum` | Same — `MsgForgetLeader` under PreVote + CheckQuorum. |
| `forget_leader_read_only_lease_based` | Same — `MsgForgetLeader` plus lease-based read-only, which sailing also does not expose as a distinct read mode here. |
| `confchange_disable_validation` | Exercises etcd's option to DISABLE conf-change validation (intentionally applying invalid joint transitions). sailing always validates conf changes (`ConfChangeError`); there is no "disable validation" switch by design. |

If any of these features are added to sailing later, port the corresponding scenario(s) here.

## What the 27 ported scenarios cover

Every Raft milestone M1–M8: election, replication/apply, persistence (crash/restart), flow-control
(Progress probe/replicate/pause), snapshots (InstallSnapshot catch-up incl. across compaction),
membership (v1 add/remove/remove-leader/learner and v2 joint add/replace/explicit), PreVote, leader
transfer, ReadIndex, CheckQuorum, async-storage durability gating, the §5.3 truncation overwrite, the
learner-must-vote regression (etcd #10998), and the async truncation/durability ABA (the seed-3395
class). Goldens are sailing-native (regenerated via `SAILING_REWRITE=1`), not copies of etcd's text.
