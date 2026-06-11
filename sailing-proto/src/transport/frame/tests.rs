use super::*;
use std::vec::Vec;

#[test]
fn encodes_and_decodes_one_frame() {
  let mut wire = Vec::new();
  encode_frame(b"hello", &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let mut out = Vec::new();
  assert!(dec.poll(&mut out).unwrap());
  assert_eq!(out, b"hello");
  assert!(!dec.poll(&mut out).unwrap(), "nothing more buffered");
}

#[test]
fn reassembles_across_partial_pushes() {
  let mut wire = Vec::new();
  encode_frame(b"abcd", &mut wire);
  let mut dec = FrameDecoder::new();
  let mut out = Vec::new();
  for byte in &wire {
    assert!(
      !dec.poll(&mut out).unwrap(),
      "no frame until every byte has arrived"
    );
    dec.push(&[*byte]);
  }
  assert!(dec.poll(&mut out).unwrap());
  assert_eq!(out, b"abcd");
}

#[test]
fn decodes_two_concatenated_frames() {
  let mut wire = Vec::new();
  encode_frame(b"one", &mut wire);
  encode_frame(b"two", &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let mut out = Vec::new();
  assert!(dec.poll(&mut out).unwrap());
  assert_eq!(out, b"one");
  assert!(dec.poll(&mut out).unwrap());
  assert_eq!(out, b"two");
  assert!(!dec.poll(&mut out).unwrap());
}

#[test]
fn empty_payload_round_trips() {
  let mut wire = Vec::new();
  encode_frame(b"", &mut wire);
  assert_eq!(wire.len(), 4, "just the length prefix");
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let mut out = Vec::new();
  assert!(dec.poll(&mut out).unwrap());
  assert!(out.is_empty());
}

#[test]
fn rejects_oversize_length() {
  let mut dec = FrameDecoder::new();
  dec.push(&u32::MAX.to_be_bytes()); // claims a ~4 GiB frame
  let mut out = Vec::new();
  assert_eq!(dec.poll(&mut out), Err(TransportError::FrameTooLarge));
}

#[test]
fn oversize_length_latches_failed_at_push_and_frees_buffer() {
  let mut dec = FrameDecoder::new();
  // A hostile read: an oversized length prefix followed by some payload bytes.
  let mut hostile = (MAX_FRAME_LEN as u32 + 1).to_be_bytes().to_vec();
  hostile.extend_from_slice(&[0u8; 1024]);
  dec.push(&hostile);
  // The decoder latched failed at push time and dropped the buffered bytes (no retention).
  assert!(dec.is_failed_for_test());
  assert_eq!(
    dec.buffered_for_test(),
    0,
    "no hostile payload byte is retained"
  );
  // A subsequent push is ignored, and poll keeps reporting the terminal error.
  dec.push(b"more bytes that must be ignored");
  assert_eq!(dec.buffered_for_test(), 0);
  let mut out = Vec::new();
  assert_eq!(dec.poll(&mut out), Err(TransportError::FrameTooLarge));
}

#[test]
fn oversize_payload_is_never_buffered_even_mid_stream() {
  // A valid frame followed by an oversized frame inside the SAME push: the oversized frame's
  // header is validated the moment it arrives, before any of its payload is copied.
  let mut chunk = Vec::new();
  encode_frame(b"ok", &mut chunk);
  chunk.extend_from_slice(&(MAX_FRAME_LEN as u32 + 1).to_be_bytes());
  chunk.extend_from_slice(&[0u8; 4096]); // hostile payload that must never land in the buffer
  let mut dec = FrameDecoder::new();
  dec.push(&chunk);
  assert!(dec.is_failed_for_test());
  assert_eq!(
    dec.buffered_for_test(),
    0,
    "the latch releases everything; the hostile payload was never accumulated"
  );
  let mut out = Vec::new();
  assert_eq!(dec.poll(&mut out), Err(TransportError::FrameTooLarge));
}

#[test]
fn split_header_still_validates_before_payload() {
  // Deliver the oversized header ONE BYTE at a time: the decoder must latch on the 4th header byte,
  // before any payload byte arrives.
  let header = (MAX_FRAME_LEN as u32 + 1).to_be_bytes();
  let mut dec = FrameDecoder::new();
  for b in &header {
    assert!(!dec.is_failed_for_test());
    dec.push(&[*b]);
  }
  assert!(
    dec.is_failed_for_test(),
    "latched exactly at the completed header"
  );
  assert_eq!(dec.buffered_for_test(), 0);
}
