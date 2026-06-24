# sailing wire format (normative)

This document pins the byte-level encoding of everything `sailing-proto` puts on a wire or a disk:
the consensus envelope, the embedder-id (`Data`) seam, the stream-transport frame, and the
`Labeled` hello. **Any change to anything below MUST bump `LABEL_VERSION`**
(`src/transport/labeled.rs`) so mixed-version nodes reject each other at the handshake instead of
mis-decoding consensus traffic. The golden byte vectors in `src/wire/tests.rs`
(`golden_byte_vectors`) pin representative encodings; a deliberate format change updates this
document, the schema, the vectors, and the version byte in the same commit.

## 1. The consensus envelope (`Message` and entry payloads)

The envelope is **protobuf (proto3)**, defined normatively by
[`proto/sailing/v1/messages.proto`](proto/sailing/v1/messages.proto) and generated into the crate
at build time (via `buffa`). One transport frame carries exactly one `sailing.v1.Message`; a
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

**Sailing's validation (enforced at the wire→programming conversion, `src/wire.rs`):**

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
  release floors, so `LABEL_VERSION` was bumped to 3:** a peer predating the consumed floors would feed a
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
  mode across the cluster — so `LABEL_VERSION` is now 4:** the handshake fences a pre-migration peer. (A
  node restarting from its OWN pre-migration durable log never sees a `SetReadMode`, so there is no
  divergence to fence — the same residual as the floors above.)
- `InstallSnapshot.offset` (5) and `total_len` (6), and `SnapshotResponse.acked_through` (5), carry
  CHUNKED snapshot transfer. `total_len == 0` is the legacy single-shot encoding (`data` is the whole
  blob — byte-identical to a pre-chunking message); `total_len != 0` means `data` is the chunk at
  `offset` within a `total_len`-byte blob, and `acked_through` is the receiver's highest contiguous
  staged offset (driving the leader's per-chunk pacing + resume). A `0` for any of the three is absent
  on the wire. **A node predating chunking would mis-stage a partial chunk as a whole blob (a decode
  failure or corrupt install), so `LABEL_VERSION` is now 5:** the handshake fences a pre-chunking peer.
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
[ u32 payload length, BIG-endian ][ payload = one encoded sailing.v1.Message ]
```

- The length prefix is big-endian (conventional for network framing); the payload is the
  protobuf envelope.
- Maximum payload: **64 MiB** (`MAX_FRAME_LEN`). A receiver rejects a larger declared length at
  the header, before buffering any payload byte; a sender refuses to emit one (closing the
  connection at the source rather than flap-looping against the receiver's bound).
- A frame's payload must decode as **one** `Message` envelope with a present body (a malformed
  payload closes the connection).

## 4. The `Labeled` hello (`tcp`/`tls`/`quic` features)

One-time, before any application frame, in each direction:

```text
[ magic 0xCA ][ version 0x05 ][ cluster id: 16 raw bytes ][ peer id length: u16 BIG-endian ][ peer id bytes ]
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
see the decoder obligations documented on `src/hard_state.rs`. Note that `ConfChange` entries IN
THE LOG carry the §1 envelope encoding (`sailing.v1.ConfChangeV2`): a log written before the
envelope migration does not replay against this version (pre-release; no migration path is
provided).
