#![no_main]
//! Fuzz the consensus envelope decode: arbitrary bytes through `wire::decode_message`.
//!
//! Oracle (the protobuf envelope is NON-canonical — field reordering, absent-vs-zero, and
//! conformant-encoder variation all decode to the same value): decode IDEMPOTENCE after the
//! first decode. A successful decode whose re-encode decodes to a DIFFERENT value is a
//! mis-decode (a field dropped on re-encode, a set re-encoded out of order, …). Byte identity
//! would be the WRONG oracle here — that is the canonical-codec property, checked in
//! `data_codec`.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use sailing_proto::wire;

fuzz_target!(|data: &[u8]| {
  let frame = Bytes::copy_from_slice(data);
  if let Ok(msg) = wire::decode_message::<u64>(frame) {
    let mut buf = std::vec::Vec::new();
    wire::encode_message(&msg, &mut buf);
    let redecoded = wire::decode_message::<u64>(Bytes::from(buf))
      .expect("re-encoding a decoded message must itself decode");
    assert_eq!(
      msg, redecoded,
      "decode is not idempotent on the envelope: a decoded value re-encoded to one that \
       decodes differently"
    );
  }
});
