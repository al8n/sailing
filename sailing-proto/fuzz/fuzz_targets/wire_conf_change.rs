#![no_main]
//! Fuzz the v2 conf-change entry-payload decode: arbitrary bytes through
//! `decode_conf_change_v2`. This is the apply-time decode path — a committed entry that
//! decodes wrong poisons every applier — so its robustness matters as much as the envelope's.
//!
//! Oracle: decode IDEMPOTENCE (the payload is a protobuf message, non-canonical). See
//! `wire_message` for why byte identity is the wrong oracle for the protobuf layer.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use sailing_proto::fuzz_internals;

fuzz_target!(|data: &[u8]| {
  let payload = Bytes::copy_from_slice(data);
  if let Ok(cc) = fuzz_internals::decode_conf_change_v2(payload) {
    let mut buf = std::vec::Vec::new();
    fuzz_internals::encode_conf_change_v2(&cc, &mut buf);
    let redecoded = fuzz_internals::decode_conf_change_v2(Bytes::from(buf))
      .expect("re-encoding a decoded conf-change must itself decode");
    assert_eq!(
      cc, redecoded,
      "decode is not idempotent on the conf-change payload"
    );
  }
});
