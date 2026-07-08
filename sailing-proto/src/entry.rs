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
  /// A committed merge FREEZE on the SOURCE group's log, applied by the core at apply-time: the
  /// source stops accepting proposals/reads/transfers so the payload's `target` group can absorb
  /// it. The lease SAFETY gate moves even earlier — to APPEND observation of this kind (see the
  /// endpoint's `freeze_pending`) — which is what makes the merge clock-free. The payload is a
  /// [`PrepareMergePayload`], G-free like [`Split`](Self::Split).
  PrepareMerge,
  /// A committed merge ABSORB on the TARGET group's log: fold the frozen source group's state
  /// machine into this one at exactly the payload's `freeze_index`. Applies only at its minted
  /// target generation (a stale mint — an abort or competing reshape won the base — no-ops
  /// deterministically); a live mint PARKS until the container resolves it from local facts —
  /// the endpoint alone cannot apply it, because the absorbed half lives in another group. The
  /// payload is a [`CommitMergePayload`]; the absorbed state never rides the entry.
  CommitMerge,
  /// A committed merge ABORT, in one of two roles told apart by the payload (a
  /// [`RollbackMergePayload`]): on the TARGET's log — source named — it abandons the merge,
  /// totally ordered against [`CommitMerge`](Self::CommitMerge) on that one log; on the
  /// SOURCE's log — source empty — it is the container-relayed unfreeze that follows.
  RollbackMerge,
  /// A committed WITNESS on a merge TARGET's log that some leader OBSERVED one of its outstanding
  /// merge-abort thaw obligations DISCHARGED (the named source thawed past its abandoned freeze, or
  /// was terminally merged away). Every replica's apply clears the obligation for exactly the named
  /// `(source, generation)` — so a replica that can NEVER locally observe the source (unhosted, its
  /// floor 0 or non-terminal, its lineage unknown here) still clears, instead of fencing compaction
  /// and blocking teardown forever. FSM-non-mutating: the apply is a pure gen-exact map clear — no
  /// lineage move, no park/fence interaction. The payload is a [`ThawDischargedPayload`], G-free like
  /// the other merge kinds. Minted ONLY from a GLOBALLY-valid discharge proof (never a host-local
  /// non-terminal floor), so it can never clear a still-live obligation on a co-hosting holder.
  ThawDischarged,
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
      Self::PrepareMerge => "prepare_merge",
      Self::CommitMerge => "commit_merge",
      Self::RollbackMerge => "rollback_merge",
      Self::ThawDischarged => "thaw_discharged",
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

/// The payload of an [`EntryKind::PrepareMerge`] entry: which group will absorb this one, and
/// the source's lineage counter after the freeze applies. G-free by design, like
/// [`SplitPayload`] — `target_bytes` is the canonical `Data` encoding of the embedder's group id.
/// Deliberately NO lease/clock field: the lease kill is observed at APPEND of the carrying
/// entry, so the merge choreography needs no cross-node clock anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareMergePayload {
  /// The absorbing (target) group id, `Data`-encoded (1..=1024 bytes, the group-tag bound).
  target_bytes: Bytes,
  /// The source's lineage counter AFTER this freeze applies — the monotone per-id value a parked
  /// `CommitMerge` compares against (one unified counter for incarnation and shape).
  source_gen_after: u64,
}

impl PrepareMergePayload {
  /// Construct.
  pub const fn new(target_bytes: Bytes, source_gen_after: u64) -> Self {
    Self {
      target_bytes,
      source_gen_after,
    }
  }

  /// The target group id's canonical `Data` encoding.
  #[inline(always)]
  pub fn target_bytes(&self) -> Bytes {
    self.target_bytes.clone()
  }

  /// The source's lineage counter after this freeze applies.
  #[inline(always)]
  pub const fn source_gen_after(&self) -> u64 {
    self.source_gen_after
  }
}

/// The payload of an [`EntryKind::CommitMerge`] entry: which frozen group to absorb, at which
/// freeze boundary, and the lineage counters that settle every race log-side. The absorbed state
/// itself NEVER rides the entry — each replica extracts its LOCAL source replica at the boundary
/// — so a merge's wire cost is independent of state size, exactly like a split's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMergePayload {
  /// The absorbed (source) group id, `Data`-encoded (1..=1024 bytes, the group-tag bound).
  source_bytes: Bytes,
  /// The source's `PrepareMerge` index — the boundary the local source replica must be
  /// frozen-applied at before this entry's parked apply can resolve.
  freeze_index: Index,
  /// The `PrepareMerge` entry's TERM: with `freeze_index` it is the freeze's LOG IDENTITY. A
  /// parked host whose local source log contains `(freeze_index, freeze_term)` holds — by log
  /// matching — the exact committed freeze and its whole prefix, and may advance that source's
  /// commit to the boundary with no live source leader left to tell it (`Term::ZERO` = no
  /// identity carried; the park then only ever waits).
  freeze_term: Term,
  /// The gen the source's freeze set: the parked apply's comparator. A source whose live counter
  /// moved PAST it was rolled back — the parked apply aborts deterministically.
  source_gen_after: u64,
  /// The target's lineage counter AFTER the absorb — the replay-idempotence anchor.
  target_gen_after: u64,
}

impl CommitMergePayload {
  /// Construct.
  pub const fn new(
    source_bytes: Bytes,
    freeze_index: Index,
    freeze_term: Term,
    source_gen_after: u64,
    target_gen_after: u64,
  ) -> Self {
    Self {
      source_bytes,
      freeze_index,
      freeze_term,
      source_gen_after,
      target_gen_after,
    }
  }

  /// The source group id's canonical `Data` encoding.
  #[inline(always)]
  pub fn source_bytes(&self) -> Bytes {
    self.source_bytes.clone()
  }

  /// The source's `PrepareMerge` index (the absorb boundary).
  #[inline(always)]
  pub const fn freeze_index(&self) -> Index {
    self.freeze_index
  }

  /// The source's `PrepareMerge` term (with the index, the freeze's log identity).
  #[inline(always)]
  pub const fn freeze_term(&self) -> Term {
    self.freeze_term
  }

  /// The gen the source's freeze set (the abort comparator).
  #[inline(always)]
  pub const fn source_gen_after(&self) -> u64 {
    self.source_gen_after
  }

  /// The target's lineage counter after the absorb.
  #[inline(always)]
  pub const fn target_gen_after(&self) -> u64 {
    self.target_gen_after
  }
}

/// The payload of an [`EntryKind::RollbackMerge`] entry — the merge's explicit abort, in one of
/// two log roles told apart by `source_bytes`:
///
/// - **Non-empty — the TARGET-side abort**, proposed on the TARGET's log so it is totally
///   ordered against [`CommitMerge`](EntryKind::CommitMerge) on ONE log (a source-side abort
///   has no cross-log order against the target's commit, so observation timing would decide
///   the race per host). `source_gen_after` names the freeze generation being abandoned;
///   `target_gen_after` is the target's own lineage mint — the same optimistic guard a split
///   carries: the abort applies only at exactly its minted generation, so aborts and commits
///   racing from one base resolve to one log-ordered winner on every replica.
/// - **Empty — the SOURCE-side unfreeze**, proposed on the source's own log as the
///   container-relayed consequence of the target-side abort applying (log-borne so a restart
///   re-derives the thaw). `source_gen_after` moves the source's lineage past the freeze
///   generation; `target_gen_after` is 0. Byte-identical on the wire to the pre-abort shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackMergePayload {
  /// The absorbed-to-be (source) group id's canonical `Data` encoding for the target-side
  /// abort role, or EMPTY for the source-side unfreeze role (the entry rides the source's own
  /// log there — naming itself would be redundant and the empty form keeps the unfreeze
  /// byte-identical to the pre-abort encoding).
  source_bytes: Bytes,
  /// The freeze generation this abort abandons (target role), or the source's lineage counter
  /// AFTER the thaw applies (source role).
  source_gen_after: u64,
  /// The target's lineage mint (target role only; 0 in the source role): the abort applies
  /// only when the target's live counter is exactly `target_gen_after - 1`.
  target_gen_after: u64,
}

impl RollbackMergePayload {
  /// Construct the SOURCE-side unfreeze role (empty source, no target mint).
  pub const fn unfreeze(source_gen_after: u64) -> Self {
    Self {
      source_bytes: Bytes::new(),
      source_gen_after,
      target_gen_after: 0,
    }
  }

  /// Construct the TARGET-side abort role.
  pub const fn abort(source_bytes: Bytes, source_gen_after: u64, target_gen_after: u64) -> Self {
    Self {
      source_bytes,
      source_gen_after,
      target_gen_after,
    }
  }

  /// Whether this is the SOURCE-side unfreeze role (empty source encoding).
  #[inline(always)]
  pub fn is_unfreeze(&self) -> bool {
    self.source_bytes.is_empty()
  }

  /// The source group id's canonical `Data` encoding (target role), or empty (source role).
  #[inline(always)]
  pub fn source_bytes(&self) -> Bytes {
    self.source_bytes.clone()
  }

  /// The freeze generation abandoned (target role) / the post-thaw lineage (source role).
  #[inline(always)]
  pub const fn source_gen_after(&self) -> u64 {
    self.source_gen_after
  }

  /// The target's lineage mint (target role only; 0 in the source role).
  #[inline(always)]
  pub const fn target_gen_after(&self) -> u64 {
    self.target_gen_after
  }
}

/// The payload of an [`EntryKind::ThawDischarged`] entry (rides `Entry.data`), proposed on a merge
/// TARGET's log: a committed witness that some leader OBSERVED this target's outstanding thaw
/// obligation for `source` at freeze generation `generation` discharged. G-free like the other merge
/// payloads — `source_bytes` is the canonical `Data` encoding of the embedder's group id (1..=1024
/// bytes, the group-tag bound). The apply clears the obligation iff it is still held at EXACTLY
/// `generation`, so a stale witness (the source re-froze at a higher generation for a fresh merge) or
/// a replayed duplicate no-ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThawDischargedPayload {
  /// The discharged source group id's canonical `Data` encoding (1..=1024 bytes, the group-tag
  /// bound). Always present — a witness names exactly one source.
  source_bytes: Bytes,
  /// The abandoned freeze generation whose obligation this witness discharges — the value the
  /// target's `abandoned[source]` record must equal for the apply's gen-exact clear to fire.
  generation: u64,
}

impl ThawDischargedPayload {
  /// Construct.
  pub const fn new(source_bytes: Bytes, generation: u64) -> Self {
    Self {
      source_bytes,
      generation,
    }
  }

  /// The discharged source group id's canonical `Data` encoding.
  #[inline(always)]
  pub fn source_bytes(&self) -> Bytes {
    self.source_bytes.clone()
  }

  /// The abandoned freeze generation this witness discharges (the gen-exact clear comparator).
  #[inline(always)]
  pub const fn generation(&self) -> u64 {
    self.generation
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
      (EntryKind::PrepareMerge, "prepare_merge"),
      (EntryKind::CommitMerge, "commit_merge"),
      (EntryKind::RollbackMerge, "rollback_merge"),
      (EntryKind::ThawDischarged, "thaw_discharged"),
    ] {
      assert_eq!(kind.as_str(), name);
      assert_eq!(std::format!("{kind}"), name);
    }
  }
}
