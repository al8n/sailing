# sailing-proto fuzz targets

[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) targets for the wire codec — the decode
surface that faces adversarial bytes off the network. The VOPR's `wire` feature only ever
round-trips *valid* messages; these targets feed the decoders arbitrary input.

```sh
cargo install cargo-fuzz          # once
cargo +nightly fuzz run wire_message      # from the sailing-proto/ directory
cargo +nightly fuzz build         # just compile all targets
```

## Targets and their oracles

Every target's baseline property is *no panic / no OOM / no hang* on any input — libfuzzer
enforces that for free. The value beyond that is the round-trip oracle, which differs by layer
because the two codecs have different canonicality:

| target | decodes | oracle |
|---|---|---|
| `wire_message` | `wire::decode_message` (the consensus envelope) | decode **idempotence**: `decode(encode(decode(x))) == decode(x)` — the envelope is protobuf, non-canonical, so byte identity is the wrong oracle |
| `wire_conf_change` | the v2 conf-change entry payload (the apply-poison path) | decode idempotence (same reason) |
| `data_codec` | the `Data` seam (`u64` / `Bytes` / `Vec` / `BTreeSet`) | **canonical** byte identity: `encode(decode(x)) == x` — the `Data` codec is one-value-one-encoding |
| `frame_decoder` | the stream `FrameDecoder` over arbitrarily-chunked input | every yielded frame is within `MAX_FRAME_LEN`; an oversized header is a terminal error, never an allocation |

Both oracle families are mutation-validated (a planted non-idempotent encode and a planted
non-canonical encode each crash their target); see the commit that introduced them.

## How it reaches internals

`wire_conf_change` and `frame_decoder` decode `pub(crate)` paths. The crate's `fuzzing`
feature (off by default, `#[doc(hidden)]`, not semver-stable) exposes thin `u64`-monomorphized
wrappers through `sailing_proto::fuzz_internals`. No production code path changes.

A crash writes a reproducer to `artifacts/<target>/`; replay it with
`cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>`. The corpus and artifacts
are git-ignored.
