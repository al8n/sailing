//! A replicated-log entry and its kind.
use crate::{Data, Index, Term};
use bytes::Bytes;

/// What a log entry carries. Only `Normal` entries reach `StateMachine::apply`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant)]
#[display("{}", self.as_str())]
pub enum EntryKind {
  /// An application command (decoded to `StateMachine::Command` at apply).
  Normal,
  /// A membership change (applied by the core, not the state machine).
  ConfChange,
  /// A leader's no-op entry, appended on election.
  Empty,
}

impl EntryKind {
  /// The stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Normal => "normal",
      Self::ConfChange => "conf_change",
      Self::Empty => "empty",
    }
  }
}

/// A single replicated-log entry. Payload is opaque bytes (O(1) clone via `Bytes`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
  term: Term,
  index: Index,
  kind: EntryKind,
  data: Bytes,
}

impl Entry {
  /// Construct an entry.
  #[inline(always)]
  pub const fn new(term: Term, index: Index, kind: EntryKind, data: Bytes) -> Self {
    Self {
      term,
      index,
      kind,
      data,
    }
  }

  /// The entry's term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The entry's index.
  #[inline(always)]
  pub const fn index(&self) -> Index {
    self.index
  }

  /// The entry's kind.
  #[inline(always)]
  pub const fn kind(&self) -> EntryKind {
    self.kind
  }

  /// The payload bytes.
  #[inline(always)]
  pub fn data(&self) -> &[u8] {
    &self.data
  }

  /// The payload as a cheap-clone `Bytes`.
  #[inline(always)]
  pub fn data_bytes(&self) -> Bytes {
    self.data.clone()
  }
}

impl Data for EntryKind {
  fn encode(&self, buf: &mut std::vec::Vec<u8>) {
    buf.push(match self {
      Self::Normal => 0,
      Self::ConfChange => 1,
      Self::Empty => 2,
    });
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, crate::DecodeError> {
    match cur.take_u8()? {
      0 => Ok(Self::Normal),
      1 => Ok(Self::ConfChange),
      2 => Ok(Self::Empty),
      _ => Err(crate::DecodeError::Invalid("EntryKind")),
    }
  }
}

impl Data for Entry {
  fn encode(&self, buf: &mut std::vec::Vec<u8>) {
    self.term.encode(buf);
    self.index.encode(buf);
    self.kind.encode(buf);
    self.data.encode(buf);
  }

  fn decode(cur: &mut crate::data::ByteCursor) -> Result<Self, crate::DecodeError> {
    let term = Term::decode(cur)?;
    let index = Index::decode(cur)?;
    let kind = EntryKind::decode(cur)?;
    // Zero-copy: the payload is an O(1) shared slice of the frame's allocation.
    let data = Bytes::decode(cur)?;
    Ok(Self::new(term, index, kind, data))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;

  #[test]
  fn entry_accessors_and_kind() {
    let e = Entry::new(
      Term::new(2),
      Index::new(7),
      EntryKind::Normal,
      Bytes::from_static(b"x"),
    );
    assert_eq!(e.term(), Term::new(2));
    assert_eq!(e.index(), Index::new(7));
    assert!(e.kind().is_normal());
    assert_eq!(e.data(), b"x");
    assert_eq!(EntryKind::ConfChange.as_str(), "conf_change");
    assert_eq!(std::format!("{}", EntryKind::Empty), "empty");
  }
}
