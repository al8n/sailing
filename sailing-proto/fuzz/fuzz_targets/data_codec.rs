#![no_main]
//! Fuzz the `Data` codec — the embedder-id seam (`u64`, `Bytes`, `Vec<T>`, `BTreeSet<T>`).
//!
//! Oracle: the `Data` codec is CANONICAL — one value has exactly one encoding (sets reject
//! non-ascending, lengths are exact, `decode_exact` rejects trailing bytes). So for any input
//! that decodes whole-buffer, re-encoding must reproduce the INPUT byte-for-byte. A failure
//! means two distinct byte strings decode to the same value — a canonicality break, the class
//! the old codec's tests guarded and on which the membership-set order discipline depends.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use sailing_proto::Data;
use std::collections::BTreeSet;

fn check<T: Data + PartialEq + core::fmt::Debug>(input: &[u8]) {
  if let Ok(v) = T::decode_exact(Bytes::copy_from_slice(input)) {
    let mut buf = std::vec::Vec::new();
    v.encode(&mut buf);
    assert_eq!(
      buf.as_slice(),
      input,
      "canonical re-encode must reproduce the input bytes exactly (decoded value: {v:?})"
    );
  }
}

fuzz_target!(|data: &[u8]| {
  // First byte selects the type; the remainder is the whole-buffer payload for `decode_exact`.
  let Some((&disc, rest)) = data.split_first() else {
    return;
  };
  match disc % 4 {
    0 => check::<u64>(rest),
    1 => check::<Bytes>(rest),
    2 => check::<std::vec::Vec<u64>>(rest),
    _ => check::<BTreeSet<u64>>(rest),
  }
});
