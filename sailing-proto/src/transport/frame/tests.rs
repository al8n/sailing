use super::*;
use std::vec::Vec;

#[test]
fn encodes_and_decodes_one_frame() {
  let mut wire = Vec::new();
  encode_frame(b"hello", &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  assert_eq!(dec.poll().unwrap().as_deref(), Some(&b"hello"[..]));
  assert!(dec.poll().unwrap().is_none(), "nothing more buffered");
}

#[test]
fn reassembles_across_partial_pushes() {
  let mut wire = Vec::new();
  encode_frame(b"abcd", &mut wire);
  let mut dec = FrameDecoder::new();
  for byte in &wire {
    assert!(
      dec.poll().unwrap().is_none(),
      "no frame until every byte has arrived"
    );
    dec.push(&[*byte]);
  }
  assert_eq!(dec.poll().unwrap().as_deref(), Some(&b"abcd"[..]));
}

#[test]
fn decodes_two_concatenated_frames() {
  let mut wire = Vec::new();
  encode_frame(b"one", &mut wire);
  encode_frame(b"two", &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  assert_eq!(dec.poll().unwrap().as_deref(), Some(&b"one"[..]));
  assert_eq!(dec.poll().unwrap().as_deref(), Some(&b"two"[..]));
  assert!(dec.poll().unwrap().is_none());
}

#[test]
fn empty_payload_round_trips() {
  let mut wire = Vec::new();
  encode_frame(b"", &mut wire);
  assert_eq!(wire.len(), 4, "just the length prefix");
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let frame = dec.poll().unwrap().expect("zero-length frame surfaces");
  assert!(frame.is_empty());
}

#[test]
fn rejects_oversize_length() {
  let mut dec = FrameDecoder::new();
  dec.push(&u32::MAX.to_be_bytes()); // claims a ~4 GiB frame
  assert!(matches!(dec.poll(), Err(TransportError::FrameTooLarge)));
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
  assert!(matches!(dec.poll(), Err(TransportError::FrameTooLarge)));
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
  assert!(matches!(dec.poll(), Err(TransportError::FrameTooLarge)));
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

/// Frames are ZERO-COPY slices of the accumulation buffer; popping a burst of frames must yield
/// each payload exactly, with the consumed prefix reclaimed O(1) (split_to, no memmove).
#[test]
fn burst_of_frames_pops_zero_copy_and_drains() {
  let mut dec = FrameDecoder::new();
  let payload = std::vec![0xAB_u8; 48 * 1024];
  let mut wire = Vec::new();
  encode_frame(&payload, &mut wire);

  for round in 0..8 {
    // Push one frame split in two arbitrary chunks, then pop it.
    let cut = 5 + round * 1000;
    dec.push(&wire[..cut]);
    assert!(
      dec.poll().unwrap().is_none(),
      "incomplete frame yields nothing"
    );
    dec.push(&wire[cut..]);
    let frame = dec.poll().unwrap().expect("frame");
    assert_eq!(&frame[..], &payload[..], "frame {round} intact");
    assert!(dec.poll().unwrap().is_none());
    assert_eq!(
      dec.buffered_for_test(),
      0,
      "fully drained after round {round}"
    );
  }
}

/// EXHAUSTIVE split matrix: a three-frame stream (empty / small / 300-byte payloads) split
/// into two pushes at EVERY byte boundary must yield exactly the three payloads, regardless of
/// where the cut lands (header straddles, payload straddles, frame joins).
#[test]
fn every_two_chunk_split_reassembles_three_frames() {
  let payloads: [&[u8]; 3] = [b"", b"hello", &[0x5A; 300]];
  let mut wire = Vec::new();
  for p in payloads {
    encode_frame(p, &mut wire);
  }
  for cut in 0..=wire.len() {
    let mut dec = FrameDecoder::new();
    dec.push(&wire[..cut]);
    dec.push(&wire[cut..]);
    for (i, expected) in payloads.iter().enumerate() {
      let frame = dec
        .poll()
        .unwrap()
        .unwrap_or_else(|| panic!("cut {cut}: frame {i} must be produced"));
      assert_eq!(&frame[..], *expected, "cut {cut}: frame {i} intact");
    }
    assert!(dec.poll().unwrap().is_none(), "cut {cut}: nothing extra");
    assert_eq!(dec.buffered_for_test(), 0, "cut {cut}: fully drained");
  }
}
