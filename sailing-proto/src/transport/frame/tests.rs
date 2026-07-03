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

#[test]
fn group_header_round_trips() {
  let mut payload = Vec::new();
  write_group_header(b"grp-id", &mut payload);
  // Golden: the header is exactly `[u16 BE group_len][group bytes]`.
  assert_eq!(&payload[..2], &[0x00, 0x06], "u16 BE length prefix");
  assert_eq!(&payload[2..], b"grp-id");
  payload.extend_from_slice(b"the-message-bytes");

  // Full wire cycle: frame it, reassemble it, then split the group header back off.
  let mut wire = Vec::new();
  encode_frame(&payload, &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let frame = dec.poll().unwrap().expect("one complete frame");
  let (group, message) = split_group_header(frame).expect("well-formed header");
  assert_eq!(&group[..], b"grp-id", "the group tag is byte-exact");
  assert_eq!(
    &message[..],
    b"the-message-bytes",
    "the message remainder is byte-exact"
  );
}

#[test]
fn group_header_empty_tag_round_trips() {
  let mut payload = Vec::new();
  write_group_header(&[], &mut payload);
  assert_eq!(
    &payload[..],
    &[0x00, 0x00],
    "the single-group tag is a zero length"
  );
  payload.extend_from_slice(b"msg");
  let (group, message) = split_group_header(Bytes::from(payload)).expect("well-formed header");
  assert!(group.is_empty(), "an empty tag splits to an empty group");
  assert_eq!(&message[..], b"msg");
}

#[test]
fn group_header_rejects_truncation() {
  // One byte cannot even hold the u16 length prefix.
  assert!(matches!(
    split_group_header(Bytes::from_static(&[0x00])),
    Err(TransportError::Decode)
  ));
}

#[test]
fn group_header_rejects_oversized_group() {
  // A declared group length of MAX_GROUP_ID_LEN + 1 (1025) is rejected even when every declared
  // byte is present — the bound, not truncation, trips.
  let over = crate::wire::MAX_GROUP_ID_LEN + 1;
  let mut buf = std::vec![0xAB_u8; 2 + over + 3];
  buf[..2].copy_from_slice(&(over as u16).to_be_bytes());
  assert!(matches!(
    split_group_header(Bytes::from(buf)),
    Err(TransportError::Decode)
  ));

  // The bound is inclusive: exactly MAX_GROUP_ID_LEN splits cleanly.
  let mut ok = std::vec![0xAB_u8; 2 + crate::wire::MAX_GROUP_ID_LEN + 1];
  ok[..2].copy_from_slice(&(crate::wire::MAX_GROUP_ID_LEN as u16).to_be_bytes());
  let (group, message) = split_group_header(Bytes::from(ok)).expect("at the bound");
  assert_eq!(group.len(), crate::wire::MAX_GROUP_ID_LEN);
  assert_eq!(message.len(), 1);
}

#[test]
fn group_header_rejects_length_past_end() {
  // The header declares a 10-byte group but only 4 bytes remain in the frame.
  assert!(matches!(
    split_group_header(Bytes::from_static(&[0x00, 0x0A, 1, 2, 3, 4])),
    Err(TransportError::Decode)
  ));
}

/// A payload with the marker and `entries` coalesced records, as a sender builds it.
fn coalesced_payload(entries: &[(u8, &[u8], &[u8])]) -> Vec<u8> {
  let mut payload = Vec::new();
  write_coalesced_marker(&mut payload);
  for (flags, group, msg) in entries {
    write_coalesced_entry(*flags, group, msg, &mut payload);
  }
  payload
}

#[test]
fn coalesced_one_entry_round_trips() {
  let payload = coalesced_payload(&[(COALESCED_FLAG_QUIESCE, b"grp-id", b"the-message")]);
  // Full wire cycle: frame it, reassemble it, split the entries back out.
  let mut wire = Vec::new();
  encode_frame(&payload, &mut wire);
  let mut dec = FrameDecoder::new();
  dec.push(&wire);
  let frame = dec.poll().unwrap().expect("one complete frame");
  assert!(is_coalesced_frame(&frame));
  let entries = split_coalesced(frame).expect("well-formed");
  assert_eq!(entries.len(), 1);
  assert_eq!(entries[0].0, COALESCED_FLAG_QUIESCE);
  assert_eq!(&entries[0].1[..], b"grp-id");
  assert_eq!(&entries[0].2[..], b"the-message");
}

#[test]
fn coalesced_two_entry_frame_matches_golden_bytes() {
  let payload = coalesced_payload(&[
    (0x01, &[0xAA], &[0x01, 0x02]),
    (0x00, &[0xBB, 0xCC], &[0x03]),
  ]);
  // Golden: [marker FF FF] then per entry [flags][u16 BE group_len][group][u32 BE msg_len][msg].
  #[rustfmt::skip]
  let golden: &[u8] = &[
    0xFF, 0xFF,
    0x01, 0x00, 0x01, 0xAA, 0x00, 0x00, 0x00, 0x02, 0x01, 0x02,
    0x00, 0x00, 0x02, 0xBB, 0xCC, 0x00, 0x00, 0x00, 0x01, 0x03,
  ];
  assert_eq!(&payload[..], golden, "the coalesced layout is byte-exact");
  let entries = split_coalesced(Bytes::from(payload)).expect("well-formed");
  assert_eq!(entries.len(), 2);
  assert_eq!(
    (entries[0].0, &entries[0].1[..], &entries[0].2[..]),
    (0x01, &[0xAA][..], &[0x01, 0x02][..])
  );
  assert_eq!(
    (entries[1].0, &entries[1].1[..], &entries[1].2[..]),
    (0x00, &[0xBB, 0xCC][..], &[0x03][..])
  );
}

#[test]
fn coalesced_many_entries_round_trip() {
  let groups: Vec<Vec<u8>> = (0u16..40).map(|i| i.to_be_bytes().to_vec()).collect();
  let msgs: Vec<Vec<u8>> = (0u8..40).map(|i| std::vec![i; 1 + i as usize]).collect();
  let mut payload = Vec::new();
  write_coalesced_marker(&mut payload);
  for i in 0..40 {
    write_coalesced_entry((i % 2) as u8, &groups[i], &msgs[i], &mut payload);
  }
  let entries = split_coalesced(Bytes::from(payload)).expect("well-formed");
  assert_eq!(entries.len(), 40);
  for (i, (flags, group, msg)) in entries.iter().enumerate() {
    assert_eq!(*flags, (i % 2) as u8, "entry {i} flags");
    assert_eq!(&group[..], &groups[i][..], "entry {i} group");
    assert_eq!(&msg[..], &msgs[i][..], "entry {i} message");
  }
}

#[test]
fn coalesced_rejects_missing_marker_and_empty_list() {
  // A single-message payload (group header first) is not a coalesced frame.
  let mut normal = Vec::new();
  write_group_header(b"grp", &mut normal);
  normal.extend_from_slice(b"msg");
  assert!(!is_coalesced_frame(&normal));
  assert!(matches!(
    split_coalesced(Bytes::from(normal)),
    Err(TransportError::Decode)
  ));
  // A bare marker with no entries is an empty list, not a valid frame.
  assert!(matches!(
    split_coalesced(Bytes::from_static(&[0xFF, 0xFF])),
    Err(TransportError::Decode)
  ));
  // As is anything shorter than the marker itself.
  assert!(matches!(
    split_coalesced(Bytes::from_static(&[0xFF])),
    Err(TransportError::Decode)
  ));
}

#[test]
fn coalesced_rejects_truncated_entries() {
  let whole = coalesced_payload(&[(0x00, b"gg", b"mm"), (0x01, b"hh", b"nn")]);
  // Every strict prefix that still carries the marker must reject: a cut anywhere inside an entry
  // is a truncated entry, and a cut at an entry boundary is caught by the second entry vanishing
  // only when the cut also removed it entirely (those prefixes are themselves valid ONE-entry
  // frames — the boundary cut after entry 1 is the single valid shorter form).
  let entry1_end = 2 + (1 + 2 + 2 + 4 + 2);
  for cut in 2..whole.len() {
    let prefix = Bytes::copy_from_slice(&whole[..cut]);
    if cut == entry1_end {
      assert_eq!(split_coalesced(prefix).expect("entry boundary").len(), 1);
    } else {
      assert!(
        matches!(split_coalesced(prefix), Err(TransportError::Decode)),
        "cut {cut} must reject as truncated"
      );
    }
  }
}

#[test]
fn coalesced_rejects_zero_and_oversized_group_lengths() {
  // group_len == 0: the empty single-group tag never rides a coalesced frame.
  let mut zero = std::vec![0xFF, 0xFF, 0x00];
  zero.extend_from_slice(&0u16.to_be_bytes());
  zero.extend_from_slice(&0u32.to_be_bytes());
  assert!(matches!(
    split_coalesced(Bytes::from(zero)),
    Err(TransportError::Decode)
  ));
  // group_len == MAX_GROUP_ID_LEN + 1, with every declared byte present — the bound trips.
  let over = crate::wire::MAX_GROUP_ID_LEN + 1;
  let mut big = std::vec![0xFF, 0xFF, 0x00];
  big.extend_from_slice(&(over as u16).to_be_bytes());
  big.extend_from_slice(&std::vec![0xAB; over]);
  big.extend_from_slice(&0u32.to_be_bytes());
  assert!(matches!(
    split_coalesced(Bytes::from(big)),
    Err(TransportError::Decode)
  ));
  // The bound is inclusive: exactly MAX_GROUP_ID_LEN splits cleanly.
  let mut ok = std::vec![0xFF, 0xFF, 0x00];
  ok.extend_from_slice(&(crate::wire::MAX_GROUP_ID_LEN as u16).to_be_bytes());
  ok.extend_from_slice(&std::vec![0xAB; crate::wire::MAX_GROUP_ID_LEN]);
  ok.extend_from_slice(&1u32.to_be_bytes());
  ok.push(0x77);
  let entries = split_coalesced(Bytes::from(ok)).expect("at the bound");
  assert_eq!(entries[0].1.len(), crate::wire::MAX_GROUP_ID_LEN);
  assert_eq!(&entries[0].2[..], &[0x77]);
}

#[test]
fn coalesced_rejects_msg_len_overrunning_the_frame() {
  // The entry declares a 100-byte message but only 2 bytes follow.
  let mut bad = std::vec![0xFF, 0xFF, 0x00, 0x00, 0x01, 0xAA];
  bad.extend_from_slice(&100u32.to_be_bytes());
  bad.extend_from_slice(&[1, 2]);
  assert!(matches!(
    split_coalesced(Bytes::from(bad)),
    Err(TransportError::Decode)
  ));
}

#[test]
fn coalesced_rejects_trailing_garbage() {
  // One complete entry, then a remainder too short to be an entry prefix.
  let mut tail = coalesced_payload(&[(0x00, b"gg", b"mm")]);
  tail.extend_from_slice(&[0x00, 0x00]);
  assert!(matches!(
    split_coalesced(Bytes::from(tail)),
    Err(TransportError::Decode)
  ));
}

/// The marker and a group length are DISJOINT: handing a coalesced frame to the single-message
/// parser must error (0xFFFF is over the group-length bound), never alias as a group tag — and the
/// reverse hand-off errors too.
#[test]
fn coalesced_marker_and_group_length_are_disjoint() {
  let coalesced = coalesced_payload(&[(0x00, b"grp", b"msg")]);
  assert!(matches!(
    split_group_header(Bytes::from(coalesced)),
    Err(TransportError::Decode)
  ));
}
