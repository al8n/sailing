# sailing wire format (normative)

This document pins the byte-level encoding of everything `sailing-proto` puts on a wire or a disk:
the consensus envelope, the embedder-id (`Data`) seam, the stream-transport frame, and the
`Labeled` hello. **Any change to anything below MUST bump `LABEL_VERSION`**
(`src/transport/labeled/mod.rs`) so mixed-version nodes reject each other at the handshake instead of
mis-decoding consensus traffic. The golden byte vectors in `src/wire/tests.rs`
(`golden_byte_vectors`) pin representative encodings; a deliberate format change updates this
document, the schema, the vectors, and the version byte in the same commit.

## 1. The consensus envelope (`Message` and entry payloads)

The envelope is **protobuf (proto3)**, defined normatively by
[`proto/sailing/v1/messages.proto`](proto/sailing/v1/messages.proto) and generated into the crate
at build time (via `buffa`). One transport frame carries a multi-Raft group-demux tag and exactly one
`sailing.v1.Message` (see §3); a
`ConfChange` entry's payload carries one `sailing.v1.ConfChangeV2`. The schema file is the field
reference — this section pins the SEMANTICS:

**Envelope semantics (protobuf, accepted as-is):**

- Absent scalar fields decode as zero/empty — identical in meaning to an explicit zero.
- Duplicate fields follow protobuf merge semantics precisely: duplicate singular
  SCALAR fields are last-wins; duplicate singular EMBEDDED-MESSAGE fields MERGE their
  field sets (their repeated sub-fields concatenate — sailing's set validation runs on
  the post-merge result, so the ascending discipline cannot be split around duplicate
  occurrences); a `oneof` re-occurrence of the SAME message-typed variant MERGES like
  any embedded message, while a DIFFERENT variant REPLACES the body wholesale; repeated
  fields concatenate. An independent implementation must reproduce these rules exactly
  — in particular, validating a set on anything other than the post-merge result
  accepts encodings sailing rejects (or vice versa).
- Unknown fields are skipped (bounded, validated before any allocation) — FORWARD
  COMPATIBILITY: a newer node may add fields without breaking an older decoder. A new field
  whose MEANING old nodes must not ignore still requires a `LABEL_VERSION` bump.
- Varints reject overlong encodings; nested messages are recursion-depth-limited; every
  declared length is bounds-checked against the remaining input BEFORE any allocation.

**Sailing's validation (enforced at the wire→programming conversion, `src/wire/mod.rs`):**

- An id field (`*_id`, set elements, `ConfChangeSingle.node_id`) carries the embedder
  `NodeId`'s `Data` encoding. It must be **1..=1024 bytes** (the hello's bound) and must decode
  consuming EXACTLY its length (`decode_exact`; trailing bytes reject). An absent/empty id
  field rejects.
- A membership set (`voters`, `learners`, `voters_outgoing`, `learners_next`) must be
  **strictly ascending by decoded value** — duplicates and disorder reject, so one set has
  exactly one accepted encoding.
- `lease_support_nanos` must be `< 1_000_000_000`.
- `Entry.timestamp` is the leader's append-time clock (nanos since its monotonic ORIGIN), read
  ONLY by the LeaseGuard read mode to age an entry across a leader change. It is `0` (and absent
  on the wire) in every other mode, so a non-LeaseGuard `Entry` is byte-identical to before the
  field existed. Cross-leader comparability requires the deployment to anchor each node's ORIGIN
  to a synchronized epoch within the configured skew bound — the LeaseGuard mode's documented
  clock assumption, NOT a property the protocol can enforce.
- `Entry.lease_window` (and `SnapshotMeta.max_lease_window`) carry the LeaseGuard commit-wait window
  of the appending leader (nanos) — the exact `lease_duration·(lease_duration + clock_drift_bound) /
  (lease_duration − clock_drift_bound)`, which covers a slow deposed leader and a fast successor (see
  `Config::clock_drift_bound`). A successor sizes its post-election commit-wait by the MAX over
  inherited entries — self-describing cross-leader safety with no assumption about other nodes'
  config. `0` (and absent on the wire) in every other mode.
  **Deployment contract:** this is safe only on a fresh, fully-LeaseGuard-aware cluster whose storage
  PRESERVES these fields. On a partially-upgraded cluster, or storage that strips unknown proto
  fields, a stored window can read `0` while the true window is nonzero; the duplicate AppendEntries /
  snapshot runtime paths re-fold a newly-visible window, but durable survival across a restart of a
  stripped window is the operator's responsibility (mid-life WIRE-FORMAT migration is out of scope — like
  `LeaseBased`'s bounded-drift contract, the protocol consumes the bound, it cannot enforce it). (The read
  MODE itself IS migratable on a running cluster, via the `SetReadMode` entry kind below.)
- `Entry.wall_timestamp` (7), `SnapshotMeta.max_wall_plus_window` (5), and
  `SnapshotMeta.max_unwalled_lease_window` (6) carry the synchronized wall-clock data the LeaseGuard
  FAILOVER tier needs (inherited reads + the precise commit-anchor). `wall_timestamp` and
  `max_wall_plus_window` are `0` (absent on the wire — byte-identical to a pre-failover peer) unless the
  cluster opts into the failover tier (`bounded_clock_uncertainty` set). `wall_timestamp` is a
  SYNCHRONIZED-EPOCH stamp and is NEVER compared against the per-node monotonic `timestamp` (5);
  `max_wall_plus_window` is the max per-entry `wall_timestamp + lease_window` over the subsumed entries
  (paired per entry). `max_unwalled_lease_window` is its dual — the max `lease_window` over subsumed
  entries that are LEASE-bearing but WALL-ABSENT — folded by the ENTRY property on EVERY node, so it is
  NOT zero off-tier (it equals `max_lease_window` in a non-failover LeaseGuard cluster) but is inert
  there (only the failover tier reads it). **The precise commit-anchor is the first CONSUMER of these
  release floors, so a pre-anchor peer is fenced by `LABEL_VERSION`:** a peer predating the consumed floors would feed a
  successor an under-sized release bound (a stale read), so it is rejected at the handshake — the
  mixed-version / field-strip fence (the fresh-cluster / matched-schema contract above) is ENFORCED for
  the failover floors, not merely documented. The handshake fences a PEER; the one residual it cannot
  fence — a node restarting from its OWN durable snapshot written by a pre-anchor binary (no tag 6) —
  is the same storage-preservation contract as `max_lease_window` above (fresh-cluster, no mid-life
  field-strip).
- `Entry` kind `SetReadMode` (`ENTRY_KIND_SET_READ_MODE` = 3) and `SnapshotMeta.read_only` (7) carry a
  cluster-wide READ-MODE migration. A committed `SetReadMode` entry flips the active read mode (Safe /
  LeaseBased / LeaseGuard) at APPLY-TIME on every node; its 1-byte payload is the target `ReadOnlyOption`
  discriminant (`0`/`1`/`2`). `SnapshotMeta.read_only` encodes the mode WITH PRESENCE as a `uint64` (`0` =
  absent/legacy, `n` = the discriminant + 1), carried so a restart / snapshot-install recovers the migrated
  mode from replicated state, not the static config — while an ABSENT (`0`) value falls back to config, so
  a pre-migration snapshot (the field absent ⇒ `0`) is NOT misread as an explicit migrate-to-Safe. `0` is
  absent on the wire (byte-identical to a pre-migration snapshot); an explicit mode is present. **A node
  predating the
  `SetReadMode` kind would poison on (or silently drop) a committed migration — diverging the replicated
  mode across the cluster — so a pre-migration peer is fenced by `LABEL_VERSION`:** the handshake fences it. (A
  node restarting from its OWN pre-migration durable log never sees a `SetReadMode`, so there is no
  divergence to fence — the same residual as the floors above.)
- `Entry` kind `Split` (`ENTRY_KIND_SPLIT` = 4) carries a committed GROUP SPLIT, applied by the core
  at apply-time (like `ConfChange`): the parent group's state machine partitions itself and the
  returned half seeds a new group. Its payload is one `sailing.v1.SplitPayload`:

  | field | type | meaning |
  |---|---|---|
  | `child` (1) | `bytes` | the child group id — the embedder `GroupId`'s `Data` encoding, **1..=1024 bytes** (the group-tag bound); an empty or over-bound field rejects at conversion, and the typed id is decoded (`decode_exact`) by the multi container, never the core |
  | `child_gen` (2) | `uint64` | the child id's incarnation under the single-incarnation contract (`0` unless the embedder reshapes ids; admission floors fence it) |
  | `parent_gen_after` (3) | `uint64` | the parent id's lineage counter AFTER this split — one unified monotone per-id counter for incarnation and shape; the replay-guard / idempotence anchor |
  | `instruction` (4) | `bytes` | the embedder's OPAQUE partition rule, handed to `StateMachine::split` on every replica — sailing never reads it; bounded by the ordinary append frame sizer |

  The payload is G-FREE by design — the child id rides as raw bytes — so the group-unaware consensus
  core can decode and fold the entry at the deterministic apply point; and the forked state itself
  NEVER rides the entry (every replica derives it locally at apply), so a split's wire cost is
  independent of state size. **A node predating the `Split` kind would reject a committed split's
  frame (an unknown kind), black-holing replication to it, so a pre-split peer is fenced by
  `LABEL_VERSION`:** the handshake fences it.

  The three MERGE entry kinds (`ENTRY_KIND_PREPARE_MERGE` = 5, `ENTRY_KIND_COMMIT_MERGE` = 6,
  `ENTRY_KIND_ROLLBACK_MERGE` = 7) were RESERVED in the same `LABEL_VERSION` bump (their pb
  values pinned), and the merge milestone MAPPED them without any wire change. Deliberately NO
  lease/clock field rides any of them — the freeze kills lease serving at APPEND observation of
  the `PrepareMerge` entry, so the merge choreography is clock-free end to end. Payloads:

  `sailing.v1.PrepareMergePayload` (on the SOURCE group's log — freeze so `target` can absorb):

  | field | type | meaning |
  |---|---|---|
  | `target` (1) | `bytes` | the absorbing group id's `Data` encoding, **1..=1024 bytes** (the group-tag bound) — retained at apply as the freeze's CLAIM: only this target may absorb or abort the frozen generation |
  | `source_gen_after` (2) | `uint64` | the source's lineage counter AFTER the freeze applies (the unified per-id counter) |

  `sailing.v1.CommitMergePayload` (on the TARGET group's log — absorb the frozen `source` at
  exactly `freeze_index`; the absorbed state itself NEVER rides the entry, every replica
  extracts its LOCAL source replica at the boundary, so the wire cost is independent of FSM
  size):

  | field | type | meaning |
  |---|---|---|
  | `source` (1) | `bytes` | the absorbed group id's `Data` encoding, **1..=1024 bytes** |
  | `freeze_index` (2) | `uint64` | the source's `PrepareMerge` index — the boundary the local source must be frozen-applied at before the parked apply can resolve |
  | `source_gen_after` (3) | `uint64` | the generation the source's freeze set (the park's log-determined comparator) |
  | `target_gen_after` (4) | `uint64` | the target's own lineage mint: the commit applies only at exactly this generation — a stale mint (an abort or competing reshape won the base) no-ops deterministically on every replica |
  | `freeze_term` (5) | `uint64` | the `PrepareMerge` entry's term — with `freeze_index`, the freeze's LOG IDENTITY: a parked host whose local source log contains the pair may advance that source's commit to the boundary (log matching carries the committed prefix), so a source follower stranded below the boundary by a lost final heartbeat cannot wedge the park after the absorb consumed the source's quorum (0 = no identity; the park only waits) |

  `sailing.v1.RollbackMergePayload` — the merge's explicit abort, in one of two log roles told
  apart by `source`:

  | field | type | meaning |
  |---|---|---|
  | `source_gen_after` (1) | `uint64` | target role: the freeze generation being abandoned; source role: the source's lineage counter AFTER the thaw applies |
  | `source` (2) | `bytes` | PRESENT (**1..=1024 bytes**): the TARGET-side abort, riding the TARGET's log so it is totally ordered against `CommitMerge` there; ABSENT/empty: the SOURCE-side thaw the container relays onto the source's own log (byte-identical to the pre-abort encoding of this payload) |
  | `target_gen_after` (3) | `uint64` | target role only (0/absent in the source role): the target's lineage mint — the abort applies only at exactly this generation, so aborts and commits racing from one base resolve to one log-ordered winner |
- `SnapshotMeta.shape_gen` (8) carries the snapshotted group's LINEAGE counter at the boundary (the
  same unified per-id counter `parent_gen_after` bumps), so a node restoring a post-split snapshot
  knows its lineage without replaying the compacted split entries, and the multi container seeds its
  replay guard from it. `0` (an unreshaped id) is absent on the wire — byte-identical to a pre-P6
  meta.
- `InstallSnapshot.offset` (5) and `total_len` (6), and `SnapshotResponse.acked_through` (5), carry
  CHUNKED snapshot transfer. `total_len == 0` is the legacy single-shot encoding (`data` is the whole
  blob — byte-identical to a pre-chunking message); `total_len != 0` means `data` is the chunk at
  `offset` within a `total_len`-byte blob, and `acked_through` is the receiver's highest contiguous
  staged offset (driving the leader's per-chunk pacing + resume). A `0` for any of the three is absent
  on the wire. **A node predating chunking would mis-stage a partial chunk as a whole blob (a decode
  failure or corrupt install), so a pre-chunking peer is fenced by `LABEL_VERSION`:** the handshake fences it.
- An enum field must carry a KNOWN value; the `Message.body` oneof must be present. Either
  failure rejects the message (parity with the old codec's unknown-tag reject).
- A rejected message closes the connection (transport) — the endpoint is never poisoned by
  wire input.

**Zero-copy contract:** `wire::decode_message` decodes over the frame's shared `Bytes`; every
`bytes` field (entry payloads, snapshot blobs, contexts, encoded ids) aliases the frame
allocation (O(1) refcount slices). A retained field pins its frame's allocation — by design,
one burst's frames at most.

## 2. The embedder seam (the `Data` codec)

`Data` is the embedder-generic encoding seam: `NodeId` (inside the envelope's id fields and the
hello), `StateMachine::Command` (inside `Entry.data` for Normal entries), and
`StateMachine::Snapshot` (the `InstallSnapshot.data` blob). The library ships impls for the id
building blocks:

| type | encoding |
|---|---|
| `u64` | 8 bytes, **little-endian** |
| `u128` | 16 bytes, **little-endian** |
| `bool` | 1 byte: `0` = false, `1` = true; any other value rejects |
| `()` | zero bytes |
| `bytes::Bytes` | `u64` length prefix, then exactly that many raw bytes |
| `Vec<T>` | `u64` count prefix, then each element's encoding back-to-back |
| `BTreeSet<T>` | `u64` count prefix, then each element in **strictly ascending** order |

Decoding rules (binding on every `Data` decoder): length/count prefixes are bounds-checked
against the remaining input before any allocation; collection elements must consume at least
one byte; a `BTreeSet` rejects duplicate or non-ascending elements; self-contained payloads
decode with `decode_exact` (trailing bytes reject); truncated input errors, never panics.

## 3. The stream-transport frame (`tcp`/`tls` features)

Each `Message` rides one frame:

```text
[ u32 payload length, BIG-endian ][ payload = [ u16 group length, BE ][ group id bytes ][ one encoded sailing.v1.Message ] ]
```

- The length prefix is big-endian (conventional for network framing) and covers the WHOLE payload:
  the multi-Raft group-demux header plus the protobuf envelope.
- The group-demux header tags each consensus frame (on BOTH the stream and QUIC transports) with its
  Raft group, so a multi-group host routes the frame to the right endpoint by reading the group id at a
  fixed offset — WITHOUT decoding the `Message`. A single-group host sends an empty tag
  (`group length == 0`); the group id is bounded 0..=1024 bytes.
- Maximum payload: **64 MiB** (`MAX_FRAME_LEN`), covering the group header and the envelope. A receiver
  rejects a larger declared length at the header, before buffering any payload byte; a sender refuses to
  emit one (closing the connection at the source rather than flap-looping against the receiver's bound).
- After the hello/preface (§4), a frame's payload must be a valid group header followed by **one**
  `Message` envelope with a present body (a malformed payload closes the connection). The QUIC
  preface frame predates authentication and carries a hello instead (§4), so this rule scopes to
  post-authentication consensus frames.
- All nodes of one cluster must agree on the deployment shape — single-group (empty tags) vs
  multi-group — and on the group-id type/encoding. A tag a host cannot accept (one that does not
  decode as its group-id type, or ANY non-empty tag on a single-group host) closes the connection
  as integrity-suspect. Only a well-formed tag for a group the host does not carry is dropped
  frame-by-frame: the shared connection survives for co-located groups, and the sender's retries
  cover the gap (a group being created/removed).

### 3.1 The coalesced control frame (multi-group hosts)

A multi-group sender may batch several groups' control messages to the same peer into ONE frame
(one syscall, one length prefix). A coalesced frame reuses the `[u32 length]` transport framing;
its payload opens with the `u16` big-endian marker `0xFFFF` followed by one or more entries:

```text
[ 0xFF 0xFF ][ entry ]+    entry = [ u8 flags ][ u16 group length, BE (1..=1024) ][ group id bytes ][ u32 message length, BE ][ one encoded sailing.v1.Message ]
```

- **The marker cannot alias a group length.** A single-message payload opens with its group length,
  bounded 0..=1024 — `0xFFFF` is outside that range, so the two payload forms are disjoint at the
  first two bytes: a pre-coalescing parser handed a coalesced frame errors (closing the connection)
  rather than mis-reading it, and §4's `LABEL_VERSION` fence rejects such a peer at the hello before
  any frame flows. Version 2 of the hello is the coalescing baseline, and version 3 the reshaping
  baseline (the `Split` entry kind + `SplitPayload`, the reserved merge kinds, and
  `SnapshotMeta.shape_gen`); ALL nodes of a cluster must be upgraded together (the hello fences a
  mixed deployment into refusing connections, never mis-decoding).
- `flags` bit 0 is QUIESCE: the sender stops exchanging this group's heartbeats after this beat, and
  the receiver's driver may stop arming the group's timers until traffic or a connection loss wakes
  it. All other bits must be zero on encode and are ignored on decode (forward room).
- Every entry carries a NON-empty group tag (`1..=1024` bytes): coalescing is a multi-group feature,
  so a single-group host closes on any coalesced frame — the same policy as any non-empty tag. A
  well-formed entry for a group the host does not carry is dropped ENTRY-by-entry; the frame's other
  entries still deliver and the connection survives.
- A malformed coalesced payload closes the connection: a truncated entry, a group length of zero or
  over the bound, a message length overrunning the frame, an empty entry list, or trailing bytes
  after the last complete entry.
- **Policy: only `Heartbeat` and `HeartbeatResponse` ride coalesced frames.** The frame layer is
  payload-agnostic, but the built-in senders coalesce exactly the heartbeat pair — every other
  message (AppendEntries, votes, snapshots, reads) keeps its own frame. Senders flush a coalesced
  frame before its payload would exceed 64 KiB (`COALESCED_FRAME_BUDGET` — thousands of heartbeats,
  never anywhere near the 64 MiB frame bound).

## 4. The `Labeled` hello (`tcp`/`tls`/`quic` features)

One-time, before any application frame, in each direction:

```text
[ magic 0xCA ][ version 0x03 ][ cluster id: 16 raw bytes ][ peer id length: u16 BIG-endian ][ peer id bytes ]
```

The ENCODING is shared by both transports — one format, one parser family, one version byte
(the `LABEL_VERSION` bump rule governs both). The ordering and local-id validation differ by
transport:

- The peer id is the `NodeId`'s `Data` encoding; it must be 1..=1024 bytes and must decode
  consuming exactly its length. A received id outside the bound terminally rejects the
  stream/connection on EITHER transport.
- A magic, version, or cluster mismatch — or a malformed id — terminally rejects the
  stream/connection on either transport.
- **Stream transport (`tcp`/`tls`)**: the dialer sends its hello eagerly; the acceptor emits its
  own only AFTER validating the dialer's, and before any application plaintext. A local id
  outside the bound is refused at construction (it could not be represented faithfully through
  the u16 length field). The hello may arrive as an incremental byte-stream prefix (a short
  prefix waits for more bytes). Over `Labeled<TlsRecords>` it is ordinary plaintext, i.e.
  encrypted inside the TLS session.
- **QUIC transport**: the hello is the identity preface — the FIRST frame (§3) on each side's
  consensus stream, written EAGERLY by BOTH sides the moment the QUIC handshake completes
  (mutual TLS has already authenticated the peer's cluster certificate; the hello binds the
  node id within it). It is delivered as one complete frame, so the parse is TOTAL: a short,
  truncated, or trailing-bytes frame is a hard reject, never a deferral. A misconfigured local
  id surfaces as a connection-level failure (an oversized preface closes the connection before
  any byte is sent; a malformed one is rejected by the peer), not a construction error.

## 5. Durable state

`HardState` persistence goes through the `StableStore` trait as a typed value — the codec above is
not (yet) used for disk. A store that serializes `HardState` itself must version its own format;
see the decoder obligations documented on `src/hard_state.rs` — including `lineage`, the fork
token the restart reconciliation compares against the durable snapshot slot (absent decodes to
`None`: exact, since no pre-`lineage` writer could have forked or adopted). Note that `ConfChange`
entries IN THE LOG carry the §1 envelope encoding (`sailing.v1.ConfChangeV2`): a log written before
the envelope migration does not replay against this version (pre-release; no migration path is
provided).

Snapshot metadata is durable state too: the meta-fidelity contract on `StableStore::submit_snapshot`
requires every meta the store hands back — the visible slot, the durable slot, and chunk staging —
to be the submitted value VERBATIM, `shape_gen` and `fork_id` included. A store that persists only
the coordinate triple breaks adoption completion and chunked-transfer resume silently.

The multi engine's PER-GROUP LINEAGE RECORDS — the incarnation generation and the admission floor —
are durable state that OUTLIVES group removal, written under the same flush barrier as the stores:
the floor is raised at REMOVAL (the removal ceiling), at MERGE resolution (the terminal
`MERGED_FLOOR` that refuses any successor incarnation), and consulted at every ADMISSION
(create/restore/fork walk the floor-first gate). A disk engine mirrors `multi::engine`'s reference
semantics; losing a floor record re-admits a retired incarnation below its fence.

## 6. Reserved: the group-header incarnation stamp (the generation fence)

The next `LABEL_VERSION` bump grows the §3 group-demux header by one field:

```
[u16 group_len][group bytes][varint generation]
```

`generation` is the SENDER's incarnation counter for that gid (the unified lineage/shape counter;
`0` for an unreshaped group). It gives the receiver a demux-time fence for retired incarnations —
`floor_admits(floor(gid), generation)` fails ⇒ drop the frame, exactly as a tombstoned gid's frame
is dropped today — the durable, generation-exact form of the volatile removal tombstone, and the
append/vote-plane counterpart of the snapshot path's lineage gate (which token-discriminates
snapshot traffic alone).

Reserved rather than landed: the field's ENFORCEMENT semantics — the comparator, tolerance for
same-lineage generation skew (a mid-split replica legitimately trails by one), and per-message-class
policy — are settled by the enforcement design that lands the bump, and a field whose semantics later
change would burn a version byte for nothing. The hello's version fence (§4) makes the eventual bump safe: mixed-version
peers reject at the handshake, never mis-parse the header.
