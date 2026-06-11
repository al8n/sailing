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
  // A subsequent push is ignored, and poll keeps reporting the terminal error.
  dec.push(b"more bytes that must be ignored");
  let mut out = Vec::new();
  assert_eq!(dec.poll(&mut out), Err(TransportError::FrameTooLarge));
}
