#![no_main]
//! Fuzz the stream frame reassembler: an arbitrary byte stream split into arbitrary chunks
//! (modelling socket reads of any size) fed to `FrameDecoder::push`/`poll`.
//!
//! Property: no panic, and every yielded frame is within `MAX_FRAME_LEN` — an oversized
//! declared length must surface as the terminal error, never as an allocation or a panic
//! (the header is validated before a payload byte is buffered). An `Err` is a legitimate
//! terminal outcome, not a failure.

use libfuzzer_sys::fuzz_target;
use sailing_proto::fuzz_internals::{drive_frame_decoder, MAX_FRAME_LEN};

fuzz_target!(|chunks: std::vec::Vec<std::vec::Vec<u8>>| {
  if let Ok(frames) = drive_frame_decoder(&chunks) {
    for f in frames {
      assert!(
        f.len() <= MAX_FRAME_LEN,
        "a yielded frame must be within the bound"
      );
    }
  }
});
