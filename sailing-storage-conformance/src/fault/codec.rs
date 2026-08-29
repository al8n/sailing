//! A reference byte codec for the durable shapes, and the tagged-field framing it is built on.
//!
//! An in-tree store keeps `HardState`/`SnapshotMeta`/`Entry` BY VALUE and so preserves every field
//! for free. A disk store cannot, and the fields it is most likely to drop — a snapshot meta's
//! `shape_gen` and `fork_id`, a hard state's `lineage`, the three-valued `lease_support` — are
//! exactly the ones whose loss is SILENT. This codec is what a conforming one looks like, and the
//! subject the serialization suite runs against itself.

use bytes::Bytes;
use core::time::Duration;
use sailing_proto::{
  ConfState, Entry, EntryKind, ForkId, HardState, Index, LeaseSupport, ReadOnlyOption,
  SnapshotMeta, Term,
};
use std::{vec, vec::Vec};

/// A CRC-32 (IEEE 802.3, reflected) over `bytes`.
///
/// Bit-at-a-time and table-free: a barrier record is written once per flush, so the cost is
/// irrelevant beside keeping the crate dependency-free.
#[must_use]
pub fn crc32(bytes: &[u8]) -> u32 {
  let mut crc = 0xFFFF_FFFFu32;
  for &b in bytes {
    crc ^= u32::from(b);
    for _ in 0..8 {
      let mask = (crc & 1).wrapping_neg();
      crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
    }
  }
  !crc
}

/// Why a decode refused its input.
///
/// The distinction that matters is CLEAN EOF versus MALFORMED. A reader that answers the same for
/// both cannot tell "this record ends here" from "this record was cut here", so a truncated blob
/// decodes to a value built from the fields that happened to survive — and every field that did not
/// comes back as its default. A default is a claim: `lease_support` defaults to a legacy record,
/// `shape_gen` to generation zero, a lineage token to absent. None of those are safe to invent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeFault {
  /// The record's length-and-checksum envelope is shorter than its own header.
  TruncatedEnvelope,
  /// The envelope declares more bytes than the input holds.
  TruncatedRecord,
  /// The record's checksum does not match its bytes.
  ChecksumMismatch,
  /// A field header (tag plus length) is cut short.
  TruncatedFieldHeader,
  /// A field's declared length runs past the end of the record.
  TruncatedFieldBody,
  /// A field's payload is shorter than the fixed width its tag requires.
  ShortField,
  /// A declared count or length would read beyond the bytes backing it — refused BEFORE any
  /// allocation is sized from it.
  ImplausibleLength,
  /// A field the shape cannot be reconstructed without is absent.
  MissingField,
  /// A discriminant byte names no variant this codec mints.
  UnknownDiscriminant,
  /// An operation decoded without consuming its whole field payload. The bytes are the ones the
  /// writer wrote — the checksum agrees — but the decode did not read them all, which is what a
  /// wrapped count looks like from the inside: a complete-looking operation with acknowledged data
  /// sitting behind it, ignored.
  TrailingBytes,
  /// A field carries a value no encoder mints — a sub-second component at or past a whole second,
  /// say. Refused BEFORE it is handed to a constructor, because several of those PANIC on input
  /// outside their domain, and a decoder that panics turns a malformed record into a crash loop
  /// rather than a fail-stop.
  NonCanonicalValue,
}

impl core::fmt::Display for DecodeFault {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str(match self {
      Self::TruncatedEnvelope => "the record envelope is cut short",
      Self::TruncatedRecord => "the record is shorter than its envelope declares",
      Self::ChecksumMismatch => "the record's checksum does not match its bytes",
      Self::TruncatedFieldHeader => "a field header is cut short",
      Self::TruncatedFieldBody => "a field body runs past the end of the record",
      Self::ShortField => "a field is shorter than its tag's fixed width",
      Self::ImplausibleLength => "a declared length or count exceeds the bytes backing it",
      Self::MissingField => "a required field is absent",
      Self::UnknownDiscriminant => "a discriminant names no known variant",
      Self::TrailingBytes => "an operation left part of its field payload unread",
      Self::NonCanonicalValue => "a field carries a value no encoder mints",
    })
  }
}

impl core::error::Error for DecodeFault {}

/// One step of a field walk: a field, or the clean end of the record.
#[derive(Debug)]
pub(crate) enum FieldStep<'a> {
  Field(u8, &'a [u8]),
  Done,
}

/// A write cursor for the tagged-field framing: `[tag u8][len u32 LE][len bytes]` per field,
/// repeated to the end of the record.
///
/// The framing is what makes an ABSENT field distinguishable from a present-but-default one — the
/// distinction `lease_support`'s legacy decode turns on.
#[derive(Debug, Default)]
pub(crate) struct FieldWriter {
  buf: Vec<u8>,
}

impl FieldWriter {
  pub(crate) fn field(&mut self, tag: u8, payload: &[u8]) -> &mut Self {
    self.buf.push(tag);
    self
      .buf
      .extend_from_slice(&declared(payload.len()).to_le_bytes());
    self.buf.extend_from_slice(payload);
    self
  }

  pub(crate) fn u64_field(&mut self, tag: u8, v: u64) -> &mut Self {
    self.field(tag, &v.to_le_bytes())
  }

  pub(crate) fn finish(self) -> Vec<u8> {
    self.buf
  }

  /// Seal the fields into a self-delimiting record: `[u64 len][fields][u32 crc]`.
  ///
  /// The envelope is what makes "cut short" DETECTABLE at all. Without it a tagged record has no
  /// end of its own — any prefix that happens to land on a field boundary reads as a complete
  /// record with fewer fields, and the reader cannot tell that from a writer who wrote fewer.
  /// Inside the envelope an absent field still means absent, which is what keeps the legacy
  /// `lease_support` reading intact.
  pub(crate) fn seal(self) -> Vec<u8> {
    let body = self.buf;
    let mut out = declared(body.len()).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out.extend_from_slice(&crc32(&body).to_le_bytes());
    out
  }
}

/// Open a sealed record, returning its field bytes.
pub(crate) fn unseal(bytes: &[u8]) -> Result<&[u8], DecodeFault> {
  let len_bytes = bytes.get(..8).ok_or(DecodeFault::TruncatedEnvelope)?;
  let len = as_offset(u64::from_le_bytes(
    len_bytes
      .try_into()
      .map_err(|_| DecodeFault::TruncatedEnvelope)?,
  ))?;
  let end = 8usize
    .checked_add(len)
    .ok_or(DecodeFault::ImplausibleLength)?;
  // The declared length is checked against the bytes that actually back it BEFORE anything is
  // sized from it.
  let body = bytes.get(8..end).ok_or(DecodeFault::TruncatedRecord)?;
  let crc_end = end.checked_add(4).ok_or(DecodeFault::ImplausibleLength)?;
  let crc_bytes = bytes
    .get(end..crc_end)
    .ok_or(DecodeFault::TruncatedEnvelope)?;
  let stored = u32::from_le_bytes(
    crc_bytes
      .try_into()
      .map_err(|_| DecodeFault::TruncatedEnvelope)?,
  );
  if crc32(body) != stored {
    return Err(DecodeFault::ChecksumMismatch);
  }
  if crc_end != bytes.len() {
    return Err(DecodeFault::TruncatedRecord);
  }
  Ok(body)
}

/// A read cursor over the tagged-field framing. Unknown tags are skipped, so a decoder built on it
/// is forward-compatible; a truncated field ends the record (the caller sees the fields before it).
#[derive(Debug)]
pub(crate) struct FieldReader<'a> {
  buf: &'a [u8],
  pos: usize,
}

impl<'a> FieldReader<'a> {
  pub(crate) const fn new(buf: &'a [u8]) -> Self {
    Self { buf, pos: 0 }
  }

  /// The next field, the clean end of the record, or a typed fault — the three outcomes a reader
  /// must keep apart.
  pub(crate) fn next_field(&mut self) -> Result<FieldStep<'a>, DecodeFault> {
    if self.pos == self.buf.len() {
      return Ok(FieldStep::Done);
    }
    let tag = *self
      .buf
      .get(self.pos)
      .ok_or(DecodeFault::TruncatedFieldHeader)?;
    let len_at = self
      .pos
      .checked_add(1)
      .ok_or(DecodeFault::ImplausibleLength)?;
    let body_at = len_at
      .checked_add(8)
      .ok_or(DecodeFault::ImplausibleLength)?;
    let len_bytes = self
      .buf
      .get(len_at..body_at)
      .ok_or(DecodeFault::TruncatedFieldHeader)?;
    let len = as_offset(u64::from_le_bytes(
      len_bytes
        .try_into()
        .map_err(|_| DecodeFault::TruncatedFieldHeader)?,
    ))?;
    let end = body_at
      .checked_add(len)
      .ok_or(DecodeFault::ImplausibleLength)?;
    let payload = self
      .buf
      .get(body_at..end)
      .ok_or(DecodeFault::TruncatedFieldBody)?;
    self.pos = end;
    Ok(FieldStep::Field(tag, payload))
  }
}

/// A [`Duration`] from a decoded `(seconds, nanoseconds)` pair.
///
/// `Duration::new` PANICS when the nanosecond carry overflows the second count, so `u64::MAX`
/// seconds beside a nanosecond value at or past a whole second aborts the decode instead of
/// refusing it — and a journal replaying a checksum-valid but malformed record would crash-loop on
/// every open rather than fail closed. Refusing a non-canonical sub-second component here makes the
/// construction TOTAL: a canonical encoder writes `subsec_nanos()`, which is always below a whole
/// second, so no carry exists to overflow.
fn duration_from(secs: u64, nanos: u32) -> Result<Duration, DecodeFault> {
  if u64::from(nanos) >= NANOS_PER_SECOND {
    return Err(DecodeFault::NonCanonicalValue);
  }
  Ok(Duration::new(secs, nanos))
}

/// The carry boundary `Duration` normalises on.
const NANOS_PER_SECOND: u64 = 1_000_000_000;

fn u64_at(payload: &[u8]) -> Result<u64, DecodeFault> {
  let bytes = payload.get(..8).ok_or(DecodeFault::ShortField)?;
  Ok(u64::from_le_bytes(
    bytes.try_into().map_err(|_| DecodeFault::ShortField)?,
  ))
}

/// A length as it goes on the medium: SIXTY-FOUR BITS, so a declaration can never be narrower than
/// the bytes it describes. `usize` is at most 64 bits on every target this crate builds for, so the
/// conversion is total — the point is the TYPE. A `u32` prefix wrapped silently past four
/// gibibytes, and the record then declared a length far below its own body: the write succeeded,
/// the completion released, and the reopen followed the truncated boundary and cut away
/// acknowledged state.
pub(crate) const fn declared(len: usize) -> u64 {
  len as u64
}

/// The inverse, on the decode side, where it is NOT total: a declaration off the medium can name
/// more bytes than this address space holds, and truncating it into one that fits is how a corrupt
/// length becomes a plausible record.
pub(crate) fn as_offset(declared: u64) -> Result<usize, DecodeFault> {
  usize::try_from(declared).map_err(|_| DecodeFault::ImplausibleLength)
}

fn u64_len_at(payload: &[u8], at: usize) -> Result<u64, DecodeFault> {
  let end = at.checked_add(8).ok_or(DecodeFault::ImplausibleLength)?;
  let bytes = payload.get(at..end).ok_or(DecodeFault::ShortField)?;
  Ok(u64::from_le_bytes(
    bytes.try_into().map_err(|_| DecodeFault::ShortField)?,
  ))
}

fn u32_at(payload: &[u8], at: usize) -> Result<u32, DecodeFault> {
  let end = at.checked_add(4).ok_or(DecodeFault::ImplausibleLength)?;
  let bytes = payload.get(at..end).ok_or(DecodeFault::ShortField)?;
  Ok(u32::from_le_bytes(
    bytes.try_into().map_err(|_| DecodeFault::ShortField)?,
  ))
}

/// A length-prefixed `u64` list: `[count u64][count * u64]`.
fn put_u64_set<'a>(buf: &mut Vec<u8>, ids: impl ExactSizeIterator<Item = &'a u64>) {
  buf.extend_from_slice(&declared(ids.len()).to_le_bytes());
  for id in ids {
    buf.extend_from_slice(&id.to_le_bytes());
  }
}

fn take_u64_set(payload: &[u8], at: &mut usize) -> Result<Vec<u64>, DecodeFault> {
  let count = as_offset(u64_len_at(payload, *at)?)?;
  *at += 8;
  // BEFORE the allocation: a declared count is only as good as the bytes behind it. Sizing a
  // Vec from an unchecked count is how a corrupt length becomes an out-of-memory abort.
  let needed = count.checked_mul(8).ok_or(DecodeFault::ImplausibleLength)?;
  let end = at
    .checked_add(needed)
    .ok_or(DecodeFault::ImplausibleLength)?;
  if end > payload.len() {
    return Err(DecodeFault::ImplausibleLength);
  }
  let mut out = Vec::with_capacity(count);
  for _ in 0..count {
    out.push(u64_at(payload.get(*at..).ok_or(DecodeFault::ShortField)?)?);
    *at += 8;
  }
  Ok(out)
}

/// The stable byte tag of an [`EntryKind`]. The mapping is explicit rather than derived from the
/// declaration order: a variant added in the middle must not renumber the ones already on disk.
const fn kind_tag(kind: EntryKind) -> u8 {
  match kind {
    EntryKind::Normal => 0,
    EntryKind::ConfChange => 1,
    EntryKind::Empty => 2,
    EntryKind::SetReadMode => 3,
    EntryKind::Split => 4,
    EntryKind::PrepareMerge => 5,
    EntryKind::CommitMerge => 6,
    EntryKind::RollbackMerge => 7,
    EntryKind::ThawDischarged => 8,
  }
}

const fn kind_of_tag(tag: u8) -> Result<EntryKind, DecodeFault> {
  Ok(match tag {
    0 => EntryKind::Normal,
    1 => EntryKind::ConfChange,
    2 => EntryKind::Empty,
    3 => EntryKind::SetReadMode,
    4 => EntryKind::Split,
    5 => EntryKind::PrepareMerge,
    6 => EntryKind::CommitMerge,
    7 => EntryKind::RollbackMerge,
    8 => EntryKind::ThawDischarged,
    _ => return Err(DecodeFault::UnknownDiscriminant),
  })
}

const fn read_only_tag(read_only: ReadOnlyOption) -> u8 {
  match read_only {
    ReadOnlyOption::Safe => 0,
    ReadOnlyOption::LeaseBased => 1,
    ReadOnlyOption::LeaseGuard => 2,
  }
}

const fn read_only_of_tag(tag: u8) -> Result<ReadOnlyOption, DecodeFault> {
  Ok(match tag {
    0 => ReadOnlyOption::Safe,
    1 => ReadOnlyOption::LeaseBased,
    2 => ReadOnlyOption::LeaseGuard,
    _ => return Err(DecodeFault::UnknownDiscriminant),
  })
}

// Hard-state field tags.
const HS_TERM: u8 = 1;
const HS_COMMIT: u8 = 2;
const HS_VOTE: u8 = 3;
const HS_LEASE_SUPPORT: u8 = 4;
const HS_LINEAGE: u8 = 5;
const HS_FOUNDING_GEN: u8 = 6;

// Snapshot-meta field tags.
const SM_LAST_INDEX: u8 = 1;
const SM_LAST_TERM: u8 = 2;
const SM_CONF: u8 = 3;
const SM_MAX_LEASE_WINDOW: u8 = 4;
const SM_MAX_WALL_PLUS_WINDOW: u8 = 5;
const SM_MAX_UNWALLED_LEASE_WINDOW: u8 = 6;
const SM_READ_ONLY: u8 = 7;
const SM_SHAPE_GEN: u8 = 8;
const SM_FORK_ID: u8 = 9;

// Entry field tags.
const EN_TERM: u8 = 1;
const EN_INDEX: u8 = 2;
const EN_KIND: u8 = 3;
const EN_DATA: u8 = 4;
const EN_TIMESTAMP: u8 = 5;
const EN_LEASE_WINDOW: u8 = 6;
const EN_WALL_TIMESTAMP: u8 = 7;

fn encode_fork_id(fork: &ForkId) -> Vec<u8> {
  let mut buf = Vec::new();
  let parent = fork.parent();
  buf.extend_from_slice(&declared(parent.len()).to_le_bytes());
  buf.extend_from_slice(parent);
  buf.extend_from_slice(&fork.parent_incarnation().to_le_bytes());
  buf.extend_from_slice(&fork.split_index().get().to_le_bytes());
  buf.extend_from_slice(&fork.split_term().get().to_le_bytes());
  let child = fork.child();
  buf.extend_from_slice(&declared(child.len()).to_le_bytes());
  buf.extend_from_slice(child);
  buf.extend_from_slice(&fork.child_gen().to_le_bytes());
  buf
}

fn take_len_prefixed(payload: &[u8], at: &mut usize) -> Result<Bytes, DecodeFault> {
  let len = as_offset(u64_len_at(payload, *at)?)?;
  *at += 8;
  let end = at.checked_add(len).ok_or(DecodeFault::ImplausibleLength)?;
  // Length-before-allocation, as on the set decoder above.
  let slice = payload
    .get(*at..end)
    .ok_or(DecodeFault::ImplausibleLength)?;
  *at = end;
  Ok(Bytes::copy_from_slice(slice))
}

fn decode_fork_id(payload: &[u8]) -> Result<ForkId, DecodeFault> {
  let mut at = 0usize;
  let parent = take_len_prefixed(payload, &mut at)?;
  let parent_incarnation = u64_at(payload.get(at..).ok_or(DecodeFault::ShortField)?)?;
  at += 8;
  let split_index = Index::new(u64_at(payload.get(at..).ok_or(DecodeFault::ShortField)?)?);
  at += 8;
  let split_term = Term::new(u64_at(payload.get(at..).ok_or(DecodeFault::ShortField)?)?);
  at += 8;
  let child = take_len_prefixed(payload, &mut at)?;
  let child_gen = u64_at(payload.get(at..).ok_or(DecodeFault::ShortField)?)?;
  Ok(ForkId::new(
    parent,
    parent_incarnation,
    split_index,
    split_term,
    child,
    child_gen,
  ))
}

fn encode_conf(conf: &ConfState<u64>) -> Vec<u8> {
  let mut buf = Vec::new();
  put_u64_set(&mut buf, conf.voters().iter());
  put_u64_set(&mut buf, conf.learners().iter());
  put_u64_set(&mut buf, conf.voters_outgoing().iter());
  put_u64_set(&mut buf, conf.learners_next().iter());
  buf.push(u8::from(conf.auto_leave()));
  buf
}

fn decode_conf(payload: &[u8]) -> Result<ConfState<u64>, DecodeFault> {
  let mut at = 0usize;
  let voters = take_u64_set(payload, &mut at)?;
  let learners = take_u64_set(payload, &mut at)?;
  let voters_outgoing = take_u64_set(payload, &mut at)?;
  let learners_next = take_u64_set(payload, &mut at)?;
  let auto_leave = *payload.get(at).ok_or(DecodeFault::ShortField)? != 0;
  Ok(ConfState::new(
    voters,
    learners,
    voters_outgoing,
    learners_next,
    auto_leave,
  ))
}

/// The reference [`Codec`](crate::check::Codec): a conforming byte encoding of every durable shape,
/// over `u64` node ids.
///
/// The point of the encoding is the FIELD SET, not the bytes: any framing works, but a codec that
/// omits `shape_gen`, `fork_id`, `lineage`, or the three-valued `lease_support` breaks the core in
/// ways nothing else reports. See the serialization suite for the checks this satisfies.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReferenceCodec;

impl ReferenceCodec {
  /// Encode a hard state.
  #[must_use]
  pub fn encode_hard_state(hs: &HardState<u64>) -> Vec<u8> {
    let mut w = FieldWriter::default();
    w.u64_field(HS_TERM, hs.term().get());
    w.u64_field(HS_COMMIT, hs.commit().get());
    if let Some(vote) = hs.vote() {
      w.u64_field(HS_VOTE, vote);
    }
    // PRESENT-but-none and ABSENT are different states: only an absent field means "no
    // current-format writer ever recorded this", which is what decodes back as `Unrecorded`.
    match hs.lease_support() {
      LeaseSupport::Unrecorded => {}
      LeaseSupport::Recorded(None) => {
        w.field(HS_LEASE_SUPPORT, &[0u8]);
      }
      LeaseSupport::Recorded(Some(d)) => {
        let mut payload = vec![1u8];
        payload.extend_from_slice(&d.as_secs().to_le_bytes());
        payload.extend_from_slice(&d.subsec_nanos().to_le_bytes());
        w.field(HS_LEASE_SUPPORT, &payload);
      }
    }
    if let Some(fork) = hs.lineage() {
      w.field(HS_LINEAGE, &encode_fork_id(fork));
    }
    // Written only when nonzero, because ABSENT is exactly what zero means here: no writer that
    // predates the field could have founded above zero, that being the only generation the
    // storeless create door admits. Encoding a zero would be equally correct and strictly noisier.
    if hs.founding_gen() != 0 {
      w.u64_field(HS_FOUNDING_GEN, hs.founding_gen());
    }
    w.seal()
  }

  /// Decode a hard state. `None` on malformed input.
  ///
  /// An ABSENT `lease_support` field decodes to [`LeaseSupport::Unrecorded`] — the conservative
  /// legacy verdict — because that is the only reading a pre-format blob can honestly get.
  ///
  /// # Errors
  /// A [`DecodeFault`] naming what was wrong — never a value assembled from the fields that
  /// happened to survive a truncation.
  pub fn decode_hard_state(bytes: &[u8]) -> Result<HardState<u64>, DecodeFault> {
    let body = unseal(bytes)?;
    let mut hs = HardState::<u64>::initial().with_lease_support(LeaseSupport::Unrecorded);
    let mut term = None;
    let mut commit = None;
    let mut reader = FieldReader::new(body);
    loop {
      match reader.next_field()? {
        FieldStep::Done => break,
        FieldStep::Field(tag, payload) => match tag {
          HS_TERM => {
            term = Some(Term::new(u64_at(payload)?));
          }
          HS_COMMIT => {
            commit = Some(Index::new(u64_at(payload)?));
          }
          HS_VOTE => hs = hs.with_vote(Some(u64_at(payload)?)),
          HS_LEASE_SUPPORT => {
            let support = match *payload.first().ok_or(DecodeFault::ShortField)? {
              0 => LeaseSupport::Recorded(None),
              1 => {
                let secs = u64_at(payload.get(1..).ok_or(DecodeFault::ShortField)?)?;
                let nanos = u32_at(payload, 9)?;
                LeaseSupport::Recorded(Some(duration_from(secs, nanos)?))
              }
              _ => return Err(DecodeFault::UnknownDiscriminant),
            };
            hs = hs.with_lease_support(support);
          }
          HS_LINEAGE => hs = hs.with_lineage(Some(decode_fork_id(payload)?)),
          HS_FOUNDING_GEN => hs = hs.with_founding_gen(u64_at(payload)?),
          _ => {}
        },
      }
    }
    // `lease_support` and `founding_gen` are deliberately NOT required: each one's absence IS its
    // legacy reading. Term and commit are, because every current-format writer emits them and
    // inventing either is a claim.
    Ok(
      hs.with_term(term.ok_or(DecodeFault::MissingField)?)
        .with_commit(commit.ok_or(DecodeFault::MissingField)?),
    )
  }

  /// Encode a hard state the way a PRE-`lease_support` writer would have: every other field, and
  /// neither `lease_support` nor `founding_gen` present at all. The input a legacy decode must be
  /// tested against.
  ///
  /// Both omissions are the genuine old shape rather than a zeroed new one: a pre-format writer
  /// emitted no such field, and each field's ABSENCE is the reading its contract assigns — a legacy
  /// promise is `Unrecorded`, a legacy founding generation is `0`.
  #[must_use]
  pub fn encode_legacy_hard_state(hs: &HardState<u64>) -> Vec<u8> {
    Self::encode_hard_state(
      &hs
        .clone()
        .with_lease_support(LeaseSupport::Unrecorded)
        .with_founding_gen(0),
    )
  }

  /// Encode a snapshot meta.
  #[must_use]
  pub fn encode_snapshot_meta(meta: &SnapshotMeta<u64>) -> Vec<u8> {
    let mut w = FieldWriter::default();
    w.u64_field(SM_LAST_INDEX, meta.last_index().get());
    w.u64_field(SM_LAST_TERM, meta.last_term().get());
    w.field(SM_CONF, &encode_conf(meta.conf()));
    w.u64_field(SM_MAX_LEASE_WINDOW, meta.max_lease_window());
    w.u64_field(SM_MAX_WALL_PLUS_WINDOW, meta.max_wall_plus_window());
    w.u64_field(
      SM_MAX_UNWALLED_LEASE_WINDOW,
      meta.max_unwalled_lease_window(),
    );
    if let Some(read_only) = meta.read_only() {
      w.field(SM_READ_ONLY, &[read_only_tag(read_only)]);
    }
    w.u64_field(SM_SHAPE_GEN, meta.shape_gen());
    if let Some(fork) = meta.fork_id() {
      w.field(SM_FORK_ID, &encode_fork_id(fork));
    }
    w.seal()
  }

  /// Decode a snapshot meta. `None` on malformed input or a missing boundary.
  ///
  /// # Errors
  /// A [`DecodeFault`] naming what was wrong.
  pub fn decode_snapshot_meta(bytes: &[u8]) -> Result<SnapshotMeta<u64>, DecodeFault> {
    let mut last_index = None;
    let mut last_term = None;
    let mut conf = None;
    let mut max_lease_window = 0u64;
    let mut max_wall_plus_window = 0u64;
    let mut max_unwalled_lease_window = 0u64;
    let mut read_only = None;
    let mut shape_gen = 0u64;
    let mut fork_id = None;
    let body = unseal(bytes)?;
    let mut reader = FieldReader::new(body);
    loop {
      match reader.next_field()? {
        FieldStep::Done => break,
        FieldStep::Field(tag, payload) => match tag {
          SM_LAST_INDEX => last_index = Some(Index::new(u64_at(payload)?)),
          SM_LAST_TERM => last_term = Some(Term::new(u64_at(payload)?)),
          SM_CONF => conf = Some(decode_conf(payload)?),
          SM_MAX_LEASE_WINDOW => max_lease_window = u64_at(payload)?,
          SM_MAX_WALL_PLUS_WINDOW => max_wall_plus_window = u64_at(payload)?,
          SM_MAX_UNWALLED_LEASE_WINDOW => max_unwalled_lease_window = u64_at(payload)?,
          SM_READ_ONLY => {
            read_only = Some(read_only_of_tag(
              *payload.first().ok_or(DecodeFault::ShortField)?,
            )?);
          }
          SM_SHAPE_GEN => shape_gen = u64_at(payload)?,
          SM_FORK_ID => fork_id = Some(decode_fork_id(payload)?),
          _ => {}
        },
      }
    }
    let mut meta = SnapshotMeta::new(
      last_index.ok_or(DecodeFault::MissingField)?,
      last_term.ok_or(DecodeFault::MissingField)?,
      conf.ok_or(DecodeFault::MissingField)?,
    )
    .with_max_lease_window(max_lease_window)
    .with_max_wall_plus_window(max_wall_plus_window)
    .with_max_unwalled_lease_window(max_unwalled_lease_window)
    .with_shape_gen(shape_gen);
    if let Some(read_only) = read_only {
      meta = meta.with_read_only(read_only);
    }
    if let Some(fork) = fork_id {
      meta = meta.with_fork_id(fork);
    }
    Ok(meta)
  }

  /// Encode a log entry, self-describing fields included: a store that drops `lease_window` or
  /// `wall_timestamp` under-sizes a successor's commit-wait after a restart.
  #[must_use]
  pub fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut w = FieldWriter::default();
    w.u64_field(EN_TERM, entry.term().get());
    w.u64_field(EN_INDEX, entry.index().get());
    w.field(EN_KIND, &[kind_tag(entry.kind())]);
    w.field(EN_DATA, entry.data());
    w.u64_field(EN_TIMESTAMP, entry.timestamp());
    w.u64_field(EN_LEASE_WINDOW, entry.lease_window());
    w.u64_field(EN_WALL_TIMESTAMP, entry.wall_timestamp());
    w.seal()
  }

  /// Decode a log entry. `None` on malformed input.
  ///
  /// # Errors
  /// A [`DecodeFault`] naming what was wrong.
  pub fn decode_entry(bytes: &[u8]) -> Result<Entry, DecodeFault> {
    let body = unseal(bytes)?;
    let mut term = None;
    let mut index = None;
    let mut kind = None;
    let mut data = None;
    let mut timestamp = 0u64;
    let mut lease_window = 0u64;
    let mut wall_timestamp = 0u64;
    let mut reader = FieldReader::new(body);
    loop {
      match reader.next_field()? {
        FieldStep::Done => break,
        FieldStep::Field(tag, payload) => match tag {
          EN_TERM => term = Some(Term::new(u64_at(payload)?)),
          EN_INDEX => index = Some(Index::new(u64_at(payload)?)),
          EN_KIND => {
            kind = Some(kind_of_tag(
              *payload.first().ok_or(DecodeFault::ShortField)?,
            )?)
          }
          EN_DATA => data = Some(Bytes::copy_from_slice(payload)),
          EN_TIMESTAMP => timestamp = u64_at(payload)?,
          EN_LEASE_WINDOW => lease_window = u64_at(payload)?,
          EN_WALL_TIMESTAMP => wall_timestamp = u64_at(payload)?,
          _ => {}
        },
      }
    }
    // Every one of these is written by every encode, so an absent one is a truncation rather than
    // an older writer — and a defaulted term, index, or kind is a fabricated entry.
    Ok(
      Entry::new(
        term.ok_or(DecodeFault::MissingField)?,
        index.ok_or(DecodeFault::MissingField)?,
        kind.ok_or(DecodeFault::MissingField)?,
        data.ok_or(DecodeFault::MissingField)?,
      )
      .with_timestamp(timestamp)
      .with_lease_window(lease_window)
      .with_wall_timestamp(wall_timestamp),
    )
  }
}

impl crate::check::Codec for ReferenceCodec {
  type NodeId = u64;

  fn encode_hard_state(&self, hs: &HardState<u64>) -> Vec<u8> {
    Self::encode_hard_state(hs)
  }

  fn decode_hard_state(&self, bytes: &[u8]) -> Option<HardState<u64>> {
    Self::decode_hard_state(bytes).ok()
  }

  fn encode_snapshot_meta(&self, meta: &SnapshotMeta<u64>) -> Vec<u8> {
    Self::encode_snapshot_meta(meta)
  }

  fn decode_snapshot_meta(&self, bytes: &[u8]) -> Option<SnapshotMeta<u64>> {
    Self::decode_snapshot_meta(bytes).ok()
  }

  fn encode_legacy_hard_state(&self, hs: &HardState<u64>) -> Option<Vec<u8>> {
    Some(Self::encode_legacy_hard_state(hs))
  }

  fn node_id(&self, n: u64) -> u64 {
    n
  }
}

#[cfg(test)]
mod tests;
