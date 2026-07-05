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
  /// A committed read-mode migration, applied by the core at apply-time (like `ConfChange`). The
  /// payload is the target [`crate::ReadOnlyOption`] as a canonical byte.
  SetReadMode,
  /// A committed group split, applied by the core at apply-time (like `ConfChange`): the parent's
  /// state machine partitions itself and the returned half seeds a NEW group. The payload is a
  /// [`SplitPayload`] — deliberately G-FREE (the child group id rides as raw bytes) so the
  /// group-unaware consensus core can decode and fold it; the multi container decodes the typed
  /// child id when it relays the staged fork. The forked state itself NEVER rides the entry —
  /// every replica derives it locally at apply — so a split's wire cost is independent of state
  /// size.
  Split,
}

impl EntryKind {
  /// The stable snake_case name.
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Normal => "normal",
      Self::ConfChange => "conf_change",
      Self::Empty => "empty",
      Self::SetReadMode => "set_read_mode",
      Self::Split => "split",
    }
  }
}

/// The payload of an [`EntryKind::Split`] entry: which child to fork, under which lineage
/// numbers, and the opaque partition instruction the state machine's `split` receives.
///
/// G-free by design — `child_bytes` is the canonical `Data` encoding of the embedder's group id —
/// so the ENDPOINT (which knows no group-id type) can decode and apply the entry at the
/// deterministic point. Sailing never reads `instruction`: it is the embedder's partition rule
/// (e.g. a key boundary), bounded only by the ordinary append frame sizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitPayload {
  /// The child group id, `Data`-encoded (1..=1024 bytes on the wire, like every group tag).
  child_bytes: Bytes,
  /// The child's incarnation under the single-incarnation contract (normally 0; a reshaping
  /// embedder's catalog supplies it, and admission floors fence it).
  child_gen: u64,
  /// The parent's lineage counter AFTER this split — the monotone per-id value the container's
  /// replay guard and the engine's lineage records key on (one unified counter for incarnation
  /// and shape).
  parent_gen_after: u64,
  /// The opaque partition instruction handed to `StateMachine::split` on every replica.
  instruction: Bytes,
}

impl SplitPayload {
  /// Construct.
  pub const fn new(
    child_bytes: Bytes,
    child_gen: u64,
    parent_gen_after: u64,
    instruction: Bytes,
  ) -> Self {
    Self {
      child_bytes,
      child_gen,
      parent_gen_after,
      instruction,
    }
  }

  /// The child group id's canonical `Data` encoding.
  #[inline(always)]
  pub fn child_bytes(&self) -> Bytes {
    self.child_bytes.clone()
  }

  /// The child's incarnation.
  #[inline(always)]
  pub const fn child_gen(&self) -> u64 {
    self.child_gen
  }

  /// The parent's lineage counter after this split.
  #[inline(always)]
  pub const fn parent_gen_after(&self) -> u64 {
    self.parent_gen_after
  }

  /// The opaque partition instruction.
  #[inline(always)]
  pub fn instruction(&self) -> &[u8] {
    &self.instruction
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
  /// The LeaseGuard commit-wait window (nanos; the exact `Δ·(Δ+ε)/(Δ−ε)`, see `Config::clock_drift_bound`) of the leader
  /// that appended this entry — i.e. how long a SUCCESSOR must wait, from a lower bound on this
  /// entry's creation, before that leader's read-lease on it has provably expired (the lease Δ plus
  /// the drift slack ε). Self-describing, so a successor sizes its commit-wait by the MAX window
  /// over inherited entries and needs NO assumption about other nodes' config. `0` (and ignored) in
  /// every other mode — absent on the wire, byte-identical to before this field existed.
  lease_window: u64,
  /// The appending leader's SYNCHRONIZED WALL-CLOCK reading (nanos since a cluster-wide epoch),
  /// used ONLY by the LeaseGuard FAILOVER tier for cross-node stamp comparison (which needs bounded
  /// skew). Distinct from `timestamp` (per-node monotonic, never compared cross-node). `0` (and
  /// absent on the wire) outside the failover tier. NEVER back-fill this from `timestamp` /
  /// `since_origin()` — those assume per-node monotonic origins, not a synchronized epoch.
  wall_timestamp: u64,
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
      lease_window: 0,
      wall_timestamp: 0,
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

  /// Set the LeaseGuard commit-wait window (nanos; the exact `Δ·(Δ+ε)/(Δ−ε)`, see `Config::clock_drift_bound`) of the
  /// appending leader. A builder, so the common 4-arg [`new`](Self::new) stays untouched.
  #[inline(always)]
  #[must_use]
  pub const fn with_lease_window(mut self, lease_window: u64) -> Self {
    self.lease_window = lease_window;
    self
  }

  /// The LeaseGuard commit-wait window (nanos; the exact `Δ·(Δ+ε)/(Δ−ε)`, see `Config::clock_drift_bound`) of the
  /// appending leader, or `0` if unset (non-LeaseGuard).
  #[inline(always)]
  pub const fn lease_window(&self) -> u64 {
    self.lease_window
  }

  /// Set the LeaseGuard failover wall-clock stamp (nanos since the cluster epoch). A builder, so the
  /// common 4-arg [`new`](Self::new) stays untouched outside the failover tier.
  #[inline(always)]
  #[must_use]
  pub const fn with_wall_timestamp(mut self, wall_timestamp: u64) -> Self {
    self.wall_timestamp = wall_timestamp;
    self
  }

  /// The LeaseGuard failover wall-clock stamp (nanos since the cluster epoch), or `0` if unset.
  #[inline(always)]
  pub const fn wall_timestamp(&self) -> u64 {
    self.wall_timestamp
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

  #[test]
  fn entry_kind_as_str_all_variants() {
    // The stable snake_case names every variant maps to (the Display impl routes through `as_str`).
    for (kind, name) in [
      (EntryKind::Normal, "normal"),
      (EntryKind::ConfChange, "conf_change"),
      (EntryKind::Empty, "empty"),
      (EntryKind::SetReadMode, "set_read_mode"),
      (EntryKind::Split, "split"),
    ] {
      assert_eq!(kind.as_str(), name);
      assert_eq!(std::format!("{kind}"), name);
    }
  }
}
