# sailing wire format (normative)

This document pins the byte-level encoding of everything `sailing-proto` puts on a wire or a disk:
the `Data` codec primitives, the consensus `Message` set, the stream-transport frame, and the
`Labeled` hello. **Any change to anything below MUST bump `LABEL_VERSION`**
(`src/transport/labeled.rs`) so mixed-version nodes reject each other at the handshake instead of
mis-decoding consensus traffic. The golden byte vectors in `src/message/tests.rs`
(`codec_golden_byte_vectors`) pin representative encodings; a deliberate format change updates this
document, the vectors, and the version byte in the same commit.

## 1. Primitives (the `Data` codec)

| type | encoding |
|---|---|
| `u64` | 8 bytes, **little-endian** |
| `bool` | 1 byte: `0` = false, `1` = true; any other value rejects |
| `()` | zero bytes |
| `Term`, `Index` | their inner `u64` |
| `OpId` is **not** wire-encoded (in-memory only) | — |
| `bytes::Bytes` | `u64` length prefix, then exactly that many raw bytes |
| `Vec<T>` | `u64` count prefix, then each element's encoding back-to-back |
| `BTreeSet<T>` | `u64` count prefix, then each element in **strictly ascending** order |
| `Duration` (in `HeartbeatResp`) | `u64` whole seconds, then `u64` nanoseconds; nanoseconds **must be < 1 000 000 000** |

Decoding rules (binding on every decoder):

- Every length/count prefix is bounds-checked against the remaining input **before** any
  allocation; a count can never reserve more memory than the input that backs it.
- Every collection element must consume at least one byte (zero-width elements reject), and a
  `BTreeSet` rejects duplicate or non-ascending elements — **one value has exactly one encoding**
  (canonicality), so two distinct byte strings can never decode to the same value.
- Self-contained payloads (an entry's command, a snapshot blob, a conf-change record) are decoded
  with `decode_exact`: trailing bytes reject.
- Truncated input always errors (`UnexpectedEof`); it never panics and never decodes as a
  different, shorter value.

## 2. Compound types

Fields are encoded in declaration order, back-to-back, with no padding or framing between them.

- `EntryKind`: 1 byte — `0` Normal, `1` ConfChange, `2` Empty.
- `Entry`: `term`, `index`, `kind`, `data` (Bytes).
- `ConfState`: `voters`, `learners`, `voters_outgoing`, `learners_next` (each a `BTreeSet<I>`),
  `auto_leave` (bool).
- `SnapshotMeta`: `last_index`, `last_term`, `conf` (ConfState).
- `ConfChangeSingle` / `ConfChange` / `ConfChangeV2`: see `src/conf.rs` (tag bytes for the
  discriminants, then fields in declaration order; the `changes` vec uses the generic `Vec<T>`
  codec).

## 3. `Message<I>` — the consensus RPC set

A leading **tag byte** selects the variant, followed by the payload struct's fields in declaration
order. An unknown tag rejects.

| tag | variant | fields (in order) |
|---|---|---|
| 0 | `AppendEntries` | term, leader (I), prev_log_index, prev_log_term, entries (`Vec<Entry>`), leader_commit |
| 1 | `AppendResp` | term, from (I), reject (bool), reject_hint_index, reject_hint_term, match_index |
| 2 | `RequestVote` | term, candidate (I), last_log_index, last_log_term, pre_vote (bool), leader_transfer (bool) |
| 3 | `VoteResp` | term, from (I), pre_vote (bool), reject (bool) |
| 4 | `Heartbeat` | term, leader (I), commit, context (Bytes), lease_round (u64) |
| 5 | `HeartbeatResp` | term, from (I), context (Bytes), lease_round (u64), lease_support (Duration) |
| 6 | `InstallSnapshot` | term, leader (I), snapshot (SnapshotMeta), data (Bytes) |
| 7 | `SnapshotResp` | term, from (I), reject (bool), match_index |
| 8 | `TimeoutNow` | term, leader (I) |
| 9 | `ReadIndex` | term, from (I), context (Bytes) |
| 10 | `ReadIndexResp` | term, from (I), index, context (Bytes), reject (bool) |

`I` is the application's `NodeId` type, encoded by its own `Data` impl (a `u64` id is 8 LE bytes).

## 4. The stream-transport frame (`tcp`/`tls` features)

Each `Message` rides one frame:

```text
[ u32 payload length, BIG-endian ][ payload = one encoded Message ]
```

- The length prefix is the only big-endian field in the protocol (conventional for network
  framing); everything inside the payload is the little-endian `Data` codec.
- Maximum payload: **64 MiB** (`MAX_FRAME_LEN`). A receiver rejects a larger declared length at
  the header, before buffering any payload byte; a sender refuses to emit one (closing the
  connection at the source rather than flap-looping against the receiver's bound).
- A frame's payload must decode as **exactly one** `Message` (trailing bytes close the
  connection).

## 5. The `Labeled` hello (`tcp`/`tls`/`quic` features)

One-time, before any application frame, in each direction:

```text
[ magic 0xCA ][ version 0x01 ][ cluster id: 16 raw bytes ][ peer id length: u16 BIG-endian ][ peer id bytes ]
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
- **QUIC transport**: the hello is the identity preface — the FIRST frame (§4) on each side's
  consensus stream, written EAGERLY by BOTH sides the moment the QUIC handshake completes
  (mutual TLS has already authenticated the peer's cluster certificate; the hello binds the
  node id within it). It is delivered as one complete frame, so the parse is TOTAL: a short,
  truncated, or trailing-bytes frame is a hard reject, never a deferral. A misconfigured local
  id surfaces as a connection-level failure (an oversized preface closes the connection before
  any byte is sent; a malformed one is rejected by the peer), not a construction error.

## 6. Durable state

`HardState` persistence goes through the `StableStore` trait as a typed value — the codec above is
not (yet) used for disk. A store that serializes `HardState` itself must version its own format;
see the decoder obligations documented on `src/hard_state.rs`.
