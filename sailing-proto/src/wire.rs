//! The wire envelope: buffa-generated protobuf types and their conversions to and from
//! the programming-level structs.
//!
//! `proto/sailing/v1/messages.proto` is the NORMATIVE schema (WIRE.md references it);
//! `build.rs` generates the types here via buffa. The programming-level types
//! ([`Message`], the payload structs, [`Entry`], [`ConfState`], …) never change shape
//! for the wire's sake — encoding converts INTO the generated types and decoding
//! converts OUT of them, at exactly the transport's existing choke points.
//!
//! # Zero-copy
//!
//! [`decode_message`] drives buffa's owned decode over the frame's shared [`Bytes`]:
//! every `bytes` field (entry payloads, snapshot blobs, contexts, encoded ids) comes
//! out as an O(1) refcount slice of the frame allocation — never a byte copy. The
//! conversions then MOVE those slices into the programming types. Encoding clones
//! [`Bytes`] handles (refcount bumps) into the generated structs and writes once into
//! the caller's buffer.
//!
//! # What the envelope accepts vs what the conversion enforces
//!
//! The envelope is protobuf (proto3): absent scalars decode as zero/empty (identical
//! in meaning to an explicit zero), duplicate singular fields are last-wins, unknown
//! fields are skipped (forward compatibility), and buffa enforces the structural
//! bounds (length-before-allocation, overlong-varint rejection, recursion depth,
//! bounded skips). Sailing's stricter, semantic validation lives in the conversions
//! here, where the old codec's guarantees are preserved:
//!
//! - an id field must be 1..=1024 bytes (the hello's bound) and must decode consuming
//!   EXACTLY its length ([`Data::decode_exact`]);
//! - a membership set must be STRICTLY ASCENDING by decoded value — duplicates and
//!   disorder reject, so one set still has exactly one accepted encoding;
//! - `lease_support_nanos` must be < 1_000_000_000;
//! - an enum must carry a KNOWN value; the `Message.body` oneof must be present
//!   (parity with the old unknown-tag reject).

use crate::{
  ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Entry,
  EntryKind, Index, Message, SnapshotMeta, Term,
  data::{Data, DecodeError},
};
use buffa::{EnumValue, Message as _};
use bytes::Bytes;
use std::vec::Vec;

mod generated {
  // The view accessors for the protocol's `from_id` fields generate `fn from_id(&self)`,
  // which trips the from_-convention lint; the field name (raft's conventional `from`)
  // is right, the lint is noise on generated code. (buffa-build's own allow list could
  // grow this — upstream candidate.)
  #![allow(clippy::wrong_self_convention)]
  include!(concat!(env!("OUT_DIR"), "/sailing_wire_generated.rs"));
}
use generated::sailing::v1 as pb;

/// The id-field bound, shared with the `Labeled` hello: an encoded `NodeId` is
/// 1..=1024 bytes everywhere it appears on the wire.
const MAX_ID_LEN: usize = 1024;

/// Whether `id`'s `Data` encoding satisfies the wire bound (1..=1024 bytes). The
/// propose-side gate for ids that enter the LOG: a conf-change id outside the bound
/// would append and replicate fine, then `decode_id` would reject it at APPLY on every
/// node — a committed entry poisoning the whole cluster. Reject it before append.
pub(crate) fn id_within_wire_bound<I: Data>(id: &I) -> bool {
  let mut v = Vec::new();
  id.encode(&mut v);
  !v.is_empty() && v.len() <= MAX_ID_LEN
}

// ─── Entry points ──────────────────────────────────────────────────────────────────

/// Encode one consensus message into `buf` as the protobuf envelope — the payload of one
/// transport frame. The public seam for custom transports (the built-in stream/QUIC
/// transports and the simulation harness all route through here).
pub fn encode_message<I: crate::NodeId>(msg: &Message<I>, buf: &mut Vec<u8>) {
  pb_message(msg).encode(buf);
}

/// Decode one frame (the COMPLETE payload of a transport frame) into a consensus
/// message. The frame's `Bytes` is the backing allocation: decoded payloads alias it
/// (O(1) refcount slices — the frame stays alive as long as any decoded field does).
pub fn decode_message<I: crate::NodeId>(mut frame: Bytes) -> Result<Message<I>, DecodeError> {
  let wire = pb::Message::decode(&mut frame).map_err(map_err)?;
  message_from(wire)
}

/// Encode a v2 conf change as an entry payload.
pub(crate) fn encode_conf_change_v2<I: crate::NodeId>(cc: &ConfChangeV2<I>, buf: &mut Vec<u8>) {
  pb::ConfChangeV2 {
    transition: EnumValue::Known(pb_transition(cc.transition())),
    changes: cc
      .changes()
      .iter()
      .map(|c| pb::ConfChangeSingle {
        change_type: pb_cc_type(c.ty()),
        node_id: encode_id(&c.node()),
        ..Default::default()
      })
      .collect(),
    context: cc.context().clone(),
    ..Default::default()
  }
  .encode(buf);
}

/// Decode a v2 conf change from an entry payload.
pub(crate) fn decode_conf_change_v2<I: crate::NodeId>(
  mut data: Bytes,
) -> Result<ConfChangeV2<I>, DecodeError> {
  let w = pb::ConfChangeV2::decode(&mut data).map_err(map_err)?;
  let transition = match w.transition {
    EnumValue::Known(pb::ConfChangeTransition::Auto) => ConfChangeTransition::Auto,
    EnumValue::Known(pb::ConfChangeTransition::Implicit) => ConfChangeTransition::Implicit,
    EnumValue::Known(pb::ConfChangeTransition::Explicit) => ConfChangeTransition::Explicit,
    EnumValue::Unknown(_) => return Err(DecodeError::Invalid("ConfChangeTransition")),
  };
  let changes = w
    .changes
    .into_iter()
    .map(|c| {
      Ok(ConfChangeSingle::new(
        cc_type_from(c.change_type)?,
        decode_id(&c.node_id)?,
      ))
    })
    .collect::<Result<Vec<_>, DecodeError>>()?;
  Ok(ConfChangeV2::new(transition, changes, w.context))
}

// ─── Error + id + set helpers ──────────────────────────────────────────────────────

/// Map buffa's structural decode errors onto the crate's error surface. The envelope
/// rejects-and-closes at the transport either way; the distinction that matters to
/// callers is truncation vs malformation.
fn map_err(e: buffa::DecodeError) -> DecodeError {
  match e {
    buffa::DecodeError::UnexpectedEof => DecodeError::UnexpectedEof,
    _ => DecodeError::Invalid("wire envelope"),
  }
}

/// Encode an id into its `bytes` field. Infallible: the 1..=1024 bound is enforced at
/// the hello for the LOCAL id, and a peer id was validated on its way in.
fn encode_id<I: Data>(id: &I) -> Bytes {
  let mut v = Vec::new();
  id.encode(&mut v);
  Bytes::from(v)
}

/// Decode an id from its `bytes` field: present (non-empty), bounded, exact-consume.
fn decode_id<I: Data>(b: &Bytes) -> Result<I, DecodeError> {
  if b.is_empty() || b.len() > MAX_ID_LEN {
    return Err(DecodeError::Invalid("node id length"));
  }
  I::decode_exact(b.clone())
}

/// Decode a membership set: each element an id, the sequence STRICTLY ASCENDING by
/// decoded value. Duplicates and disorder reject — one set, one accepted encoding.
fn decode_set<I: crate::NodeId>(
  elems: &[Bytes],
) -> Result<std::collections::BTreeSet<I>, DecodeError> {
  let mut out = std::collections::BTreeSet::new();
  let mut prev: Option<I> = None;
  for b in elems {
    let id = decode_id::<I>(b)?;
    if let Some(p) = prev
      && p >= id
    {
      return Err(DecodeError::Invalid("set order"));
    }
    prev = Some(id);
    out.insert(id);
  }
  Ok(out)
}

/// Encode a membership set: `BTreeSet` iteration is ascending by `Ord`, which IS the
/// canonical wire order.
fn encode_set<I: crate::NodeId>(set: &std::collections::BTreeSet<I>) -> Vec<Bytes> {
  set.iter().map(encode_id).collect()
}

fn pb_cc_type(t: ConfChangeType) -> EnumValue<pb::ConfChangeType> {
  EnumValue::Known(match t {
    ConfChangeType::AddNode => pb::ConfChangeType::AddNode,
    ConfChangeType::RemoveNode => pb::ConfChangeType::RemoveNode,
    ConfChangeType::AddLearnerNode => pb::ConfChangeType::AddLearnerNode,
  })
}

fn cc_type_from(v: EnumValue<pb::ConfChangeType>) -> Result<ConfChangeType, DecodeError> {
  match v {
    EnumValue::Known(pb::ConfChangeType::AddNode) => Ok(ConfChangeType::AddNode),
    EnumValue::Known(pb::ConfChangeType::RemoveNode) => Ok(ConfChangeType::RemoveNode),
    EnumValue::Known(pb::ConfChangeType::AddLearnerNode) => Ok(ConfChangeType::AddLearnerNode),
    EnumValue::Unknown(_) => Err(DecodeError::Invalid("ConfChangeType")),
  }
}

fn pb_transition(t: ConfChangeTransition) -> pb::ConfChangeTransition {
  match t {
    ConfChangeTransition::Auto => pb::ConfChangeTransition::Auto,
    ConfChangeTransition::Implicit => pb::ConfChangeTransition::Implicit,
    ConfChangeTransition::Explicit => pb::ConfChangeTransition::Explicit,
  }
}

// ─── Entry / ConfState / SnapshotMeta ──────────────────────────────────────────────

fn pb_entry(e: &Entry) -> pb::Entry {
  pb::Entry {
    term: e.term().get(),
    index: e.index().get(),
    kind: EnumValue::Known(match e.kind() {
      EntryKind::Normal => pb::EntryKind::Normal,
      EntryKind::ConfChange => pb::EntryKind::ConfChange,
      EntryKind::Empty => pb::EntryKind::Empty,
      EntryKind::SetReadMode => pb::EntryKind::SetReadMode,
    }),
    data: e.data_bytes(),
    timestamp: e.timestamp(),
    lease_window: e.lease_window(),
    wall_timestamp: e.wall_timestamp(),
    ..Default::default()
  }
}

fn entry_from(w: pb::Entry) -> Result<Entry, DecodeError> {
  let kind = match w.kind {
    EnumValue::Known(pb::EntryKind::Normal) => EntryKind::Normal,
    EnumValue::Known(pb::EntryKind::ConfChange) => EntryKind::ConfChange,
    EnumValue::Known(pb::EntryKind::Empty) => EntryKind::Empty,
    EnumValue::Known(pb::EntryKind::SetReadMode) => EntryKind::SetReadMode,
    EnumValue::Unknown(_) => return Err(DecodeError::Invalid("EntryKind")),
  };
  Ok(
    Entry::new(Term::new(w.term), Index::new(w.index), kind, w.data)
      .with_timestamp(w.timestamp)
      .with_lease_window(w.lease_window)
      .with_wall_timestamp(w.wall_timestamp),
  )
}

fn pb_conf_state<I: crate::NodeId>(c: &ConfState<I>) -> pb::ConfState {
  pb::ConfState {
    voters: encode_set(c.voters()),
    learners: encode_set(c.learners()),
    voters_outgoing: encode_set(c.voters_outgoing()),
    learners_next: encode_set(c.learners_next()),
    auto_leave: c.auto_leave(),
    ..Default::default()
  }
}

fn conf_state_from<I: crate::NodeId>(w: &pb::ConfState) -> Result<ConfState<I>, DecodeError> {
  Ok(ConfState::new(
    decode_set::<I>(&w.voters)?,
    decode_set::<I>(&w.learners)?,
    decode_set::<I>(&w.voters_outgoing)?,
    decode_set::<I>(&w.learners_next)?,
    w.auto_leave,
  ))
}

fn pb_snapshot_meta<I: crate::NodeId>(m: &SnapshotMeta<I>) -> pb::SnapshotMeta {
  pb::SnapshotMeta {
    last_index: m.last_index().get(),
    last_term: m.last_term().get(),
    conf: buffa::MessageField::some(pb_conf_state(m.conf())),
    max_lease_window: m.max_lease_window(),
    max_wall_plus_window: m.max_wall_plus_window(),
    max_unwalled_lease_window: m.max_unwalled_lease_window(),
    read_only: m.read_only().map_or(0, |o| u64::from(o.as_u8()) + 1),
    ..Default::default()
  }
}

fn snapshot_meta_from<I: crate::NodeId>(
  w: &pb::SnapshotMeta,
) -> Result<SnapshotMeta<I>, DecodeError> {
  let conf = w
    .conf
    .as_option()
    .ok_or(DecodeError::Invalid("SnapshotMeta.conf"))?;
  // 0 = legacy/absent (a pre-migration snapshot, or one that never set the mode) → leave the meta's
  // read_only as None so restart/install falls back to the static config; 1.. = an explicit migrated mode
  // (the ReadOnlyOption discriminant + 1, so an explicit Safe is DISTINCT from absent).
  let read_only = match w.read_only {
    0 => None,
    n => Some(
      u8::try_from(n - 1)
        .ok()
        .and_then(crate::ReadOnlyOption::from_u8)
        .ok_or(DecodeError::Invalid("SnapshotMeta.read_only"))?,
    ),
  };
  let meta = SnapshotMeta::new(
    Index::new(w.last_index),
    Term::new(w.last_term),
    conf_state_from(conf)?,
  )
  .with_max_lease_window(w.max_lease_window)
  .with_max_wall_plus_window(w.max_wall_plus_window)
  .with_max_unwalled_lease_window(w.max_unwalled_lease_window);
  Ok(match read_only {
    Some(mode) => meta.with_read_only(mode),
    None => meta,
  })
}

// ─── Message ───────────────────────────────────────────────────────────────────────

fn pb_message<I: crate::NodeId>(msg: &Message<I>) -> pb::Message {
  use pb::message::Body;
  let body = match msg {
    Message::AppendEntries(m) => Body::from(pb::AppendEntries {
      term: m.term().get(),
      leader_id: encode_id(&m.leader()),
      prev_log_index: m.prev_log_index().get(),
      prev_log_term: m.prev_log_term().get(),
      entries: m.entries().iter().map(pb_entry).collect(),
      leader_commit: m.leader_commit().get(),
      ..Default::default()
    }),
    Message::AppendResp(m) => Body::from(pb::AppendResp {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      reject: m.reject(),
      reject_hint_index: m.reject_hint_index().get(),
      reject_hint_term: m.reject_hint_term().get(),
      match_index: m.match_index().get(),
      ..Default::default()
    }),
    Message::RequestVote(m) => Body::from(pb::RequestVote {
      term: m.term().get(),
      candidate_id: encode_id(&m.candidate()),
      last_log_index: m.last_log_index().get(),
      last_log_term: m.last_log_term().get(),
      pre_vote: m.pre_vote(),
      leader_transfer: m.leader_transfer(),
      ..Default::default()
    }),
    Message::VoteResp(m) => Body::from(pb::VoteResp {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      pre_vote: m.pre_vote(),
      reject: m.reject(),
      ..Default::default()
    }),
    Message::Heartbeat(m) => Body::from(pb::Heartbeat {
      term: m.term().get(),
      leader_id: encode_id(&m.leader()),
      commit: m.commit().get(),
      context: m.context_bytes(),
      lease_round: m.lease_round(),
      ..Default::default()
    }),
    Message::HeartbeatResp(m) => Body::from(pb::HeartbeatResp {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      context: m.context_bytes(),
      lease_round: m.lease_round(),
      lease_support_secs: m.lease_support().as_secs(),
      lease_support_nanos: u64::from(m.lease_support().subsec_nanos()),
      ..Default::default()
    }),
    Message::InstallSnapshot(m) => Body::from(pb::InstallSnapshot {
      term: m.term().get(),
      leader_id: encode_id(&m.leader()),
      snapshot: buffa::MessageField::some(pb_snapshot_meta(m.snapshot())),
      data: m.data().clone(),
      ..Default::default()
    }),
    Message::SnapshotResp(m) => Body::from(pb::SnapshotResp {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      reject: m.reject(),
      match_index: m.match_index().get(),
      ..Default::default()
    }),
    Message::TimeoutNow(m) => Body::from(pb::TimeoutNow {
      term: m.term().get(),
      leader_id: encode_id(&m.leader()),
      ..Default::default()
    }),
    Message::ReadIndex(m) => Body::from(pb::ReadIndex {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      context: m.context_bytes(),
      ..Default::default()
    }),
    Message::ReadIndexResp(m) => Body::from(pb::ReadIndexResp {
      term: m.term().get(),
      from_id: encode_id(&m.from()),
      index: m.index().get(),
      context: m.context_bytes(),
      reject: m.reject(),
      ..Default::default()
    }),
  };
  pb::Message {
    body: Some(body),
    ..Default::default()
  }
}

fn message_from<I: crate::NodeId>(wire: pb::Message) -> Result<Message<I>, DecodeError> {
  use pb::message::Body;
  let body = wire.body.ok_or(DecodeError::Invalid("Message.body"))?;
  Ok(match body {
    Body::AppendEntries(m) => Message::AppendEntries(crate::AppendEntries::new(
      Term::new(m.term),
      decode_id(&m.leader_id)?,
      Index::new(m.prev_log_index),
      Term::new(m.prev_log_term),
      m.entries
        .into_iter()
        .map(entry_from)
        .collect::<Result<Vec<_>, _>>()?,
      Index::new(m.leader_commit),
    )),
    Body::AppendResp(m) => Message::AppendResp(crate::AppendResp::new(
      Term::new(m.term),
      decode_id(&m.from_id)?,
      m.reject,
      Index::new(m.reject_hint_index),
      Term::new(m.reject_hint_term),
      Index::new(m.match_index),
    )),
    Body::RequestVote(m) => Message::RequestVote(crate::RequestVote::new(
      Term::new(m.term),
      decode_id(&m.candidate_id)?,
      Index::new(m.last_log_index),
      Term::new(m.last_log_term),
      m.pre_vote,
      m.leader_transfer,
    )),
    Body::VoteResp(m) => Message::VoteResp(crate::VoteResp::new(
      Term::new(m.term),
      decode_id(&m.from_id)?,
      m.pre_vote,
      m.reject,
    )),
    Body::Heartbeat(m) => Message::Heartbeat(
      crate::Heartbeat::new(
        Term::new(m.term),
        decode_id(&m.leader_id)?,
        Index::new(m.commit),
        m.context,
      )
      .with_lease_round(m.lease_round),
    ),
    Body::HeartbeatResp(m) => {
      // Validate on the FULL u64 before narrowing: the schema deliberately carries nanos
      // as uint64 because protobuf uint32 decoding truncates oversized varints by spec,
      // which would let 2^32 + k slip past a post-truncation bound as k.
      if m.lease_support_nanos >= 1_000_000_000 {
        return Err(DecodeError::Invalid("lease_support nanos"));
      }
      Message::HeartbeatResp(
        crate::HeartbeatResp::new(Term::new(m.term), decode_id(&m.from_id)?, m.context)
          .with_lease_round(m.lease_round)
          .with_lease_support(core::time::Duration::new(
            m.lease_support_secs,
            m.lease_support_nanos as u32,
          )),
      )
    }
    Body::InstallSnapshot(m) => {
      let meta = m
        .snapshot
        .as_option()
        .ok_or(DecodeError::Invalid("InstallSnapshot.snapshot"))?;
      Message::InstallSnapshot(crate::InstallSnapshot::new(
        Term::new(m.term),
        decode_id(&m.leader_id)?,
        snapshot_meta_from(meta)?,
        m.data,
      ))
    }
    Body::SnapshotResp(m) => Message::SnapshotResp(crate::SnapshotResp::new(
      Term::new(m.term),
      decode_id(&m.from_id)?,
      m.reject,
      Index::new(m.match_index),
    )),
    Body::TimeoutNow(m) => Message::TimeoutNow(crate::TimeoutNow::new(
      Term::new(m.term),
      decode_id(&m.leader_id)?,
    )),
    Body::ReadIndex(m) => Message::ReadIndex(crate::ReadIndex::new(
      Term::new(m.term),
      decode_id(&m.from_id)?,
      m.context,
    )),
    Body::ReadIndexResp(m) => Message::ReadIndexResp(crate::ReadIndexResp::new(
      Term::new(m.term),
      decode_id(&m.from_id)?,
      Index::new(m.index),
      m.context,
      m.reject,
    )),
  })
}

#[cfg(test)]
mod tests;
