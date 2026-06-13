//! A replicated-log entry and its kind.
use crate::{Index, Term};
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
  /// The leader's append-time clock (nanos since its monotonic ORIGIN), used ONLY by the
  /// LeaseGuard read mode to age the entry across a leader change. `0` (and ignored) in every
  /// other mode; it then encodes absent on the wire, so a non-LeaseGuard entry is byte-identical
  /// to before this field existed.
  timestamp: u64,
}

impl Entry {
  /// Construct an entry (`timestamp` defaults to `0`; set it with
  /// [`with_timestamp`](Self::with_timestamp) in LeaseGuard mode).
  #[inline(always)]
  pub const fn new(term: Term, index: Index, kind: EntryKind, data: Bytes) -> Self {
    Self {
      term,
      index,
      kind,
      data,
      timestamp: 0,
    }
  }

  /// Set the LeaseGuard append-timestamp (nanos since the leader's ORIGIN). A builder, so the
  /// common 4-arg [`new`](Self::new) stays untouched in non-LeaseGuard paths.
  #[inline(always)]
  #[must_use]
  pub const fn with_timestamp(mut self, timestamp: u64) -> Self {
    self.timestamp = timestamp;
    self
  }

  /// The LeaseGuard append-timestamp (nanos since the leader's ORIGIN), or `0` if unset.
  #[inline(always)]
  pub const fn timestamp(&self) -> u64 {
    self.timestamp
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
