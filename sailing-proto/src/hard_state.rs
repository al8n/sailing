//! The durable Raft metadata: `(term, vote, commit, lease_support, lineage)`, persisted before acting.
use crate::{CheapClone, ForkId, Index, Term};
use core::time::Duration;
use std::boxed::Box;

/// The durable provenance + magnitude of this node's LeaseBased read-lease promise.
///
/// The post-restart vote fence must size itself by the largest lease window this node may have advertised
/// before the crash. The subtlety the bare `Option<Duration>` could not express: the ABSENCE of a value is
/// ambiguous — it could mean "a current-format node recorded that it promised nothing" OR "a pre-format
/// (legacy) record that never had this field, whose prior promise is UNKNOWN". Conflating them lets a
/// legacy upgrade under weaker config under-fence. The three-valued type makes the
/// distinction durable and impossible to lose: an in-tree by-value store holds the variant exactly, and
/// (the library being genesis-at-this-format) only a genuine pre-format disk decode can ever be
/// [`Unrecorded`](Self::Unrecorded).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseSupport {
  /// No current-format writer ever recorded this field: a pre-format (legacy) durable record. The
  /// post-restart fence must treat the prior promise as UNKNOWN (conservative), NEVER as "promised nothing".
  Unrecorded,
  /// A current-format node recorded its enforcing promise. `None` = it promised nothing (a fresh,
  /// non-enforcing, or never-yet-enforcing node); `Some(d)` = it will uphold a `d` lease window across a
  /// restart and the fence must cover it.
  Recorded(Option<Duration>),
}

impl Default for LeaseSupport {
  /// Genesis is the current format: a freshly constructed record promised nothing, but it is RECORDED
  /// (never `Unrecorded`) so a native node is unambiguously distinguishable from a legacy decode.
  #[inline(always)]
  fn default() -> Self {
    Self::Recorded(None)
  }
}

impl LeaseSupport {
  /// The promised lease-window MAGNITUDE: `Some(d)` for a recorded promise, `None` for a recorded
  /// no-promise OR a legacy record (callers that need to DISTINGUISH legacy use [`is_unrecorded`](Self::is_unrecorded)).
  #[inline(always)]
  pub const fn promised(self) -> Option<Duration> {
    match self {
      Self::Recorded(d) => d,
      Self::Unrecorded => None,
    }
  }

  /// Whether this is a pre-format (legacy) record whose prior promise is unknown.
  #[inline(always)]
  pub const fn is_unrecorded(self) -> bool {
    matches!(self, Self::Unrecorded)
  }

  /// The monotone JOIN used only by the durable write choke-point (`stamp_floors`): raise a recorded floor
  /// to at least `floor`, AND upgrade `Unrecorded -> Recorded` — any current-format write re-stamps
  /// provenance, so a legacy record self-heals on the first write this incarnation makes. Never lowers a
  /// recorded magnitude (`max`).
  #[inline]
  pub fn raise(self, floor: Option<Duration>) -> Self {
    Self::Recorded(self.promised().max(floor))
  }
}

/// Durable Raft metadata. `vote` keeps `Option` (the documented `Copy`-scalar exception: `Some(_)` ≠
/// `None`); `lease_support` is a three-valued [`LeaseSupport`] (provenance + magnitude). Generic params
/// carry no bounds (bounds live on methods).
///
/// `lease_support` is the durable shadow of `HeartbeatResponse.lease_support` — the lease window this node has
/// advertised it will uphold — persisted so a restarted node keeps the promise its prior incarnation made
/// to the network, sizing the post-restart vote fence by the PROMISE rather than by the (possibly weaker)
/// post-restart config, with the provenance needed to handle a legacy upgrade safely. It is
/// the lease analogue of persisting `vote` (persist-before-advertise, the sibling of persist-before-ack).
/// An out-of-tree disk decoder MUST map a genuine pre-`lease_support` blob to [`LeaseSupport::Unrecorded`]
/// (never `Recorded(None)`): `Unrecorded` triggers the conservative restart fence, so a freshly-upgraded
/// node is never less safe than before.
///
/// `lineage` is the durable record of which lineage this node's LOG belongs to — the fork token the node
/// was manufactured under or adopted at a snapshot install, `None` for a node that never did either. The
/// log itself carries no token, so without this record a restart cannot tell whether a durable snapshot
/// and the surviving log suffix are the SAME lineage's artifacts — and `(index, term)` coordinate proofs
/// cannot answer that across a fork boundary (Log Matching holds only within one lineage). Restart
/// reconciliation therefore compares this record against the durable snapshot's token BEFORE any
/// coordinate arm. Written at the durable choke-point on every hard-state write (the node's current
/// lineage), so an adoption makes its lineage durable before the destructive re-baseline acts on it.
/// An out-of-tree disk decoder maps an absent field to `None` — exact, not merely conservative: no
/// pre-`lineage` writer could ever have forked or adopted, so its log is unconditionally the token-less
/// lineage's.
///
/// `founding_gen` is the generation the group's admission ceremony FOUNDED this incarnation at — the
/// only reading of that value which outlives the process. Two facts make it safe to recover a lineage
/// counter from, and both must stay true of any writer:
///
/// - It is a per-incarnation CONSTANT, set once at the ceremony and never moved. It does not track the
///   group's current shape; a shape move writes nothing here.
/// - It is therefore TEMPORALLY PRIOR to every entry of the incarnation — no committed entry can be
///   below it, so recovering it can never place a retained entry behind a counter it has yet to reach.
///
/// A value that tracked the CURRENT shape would satisfy neither, and recovering one would be unsound:
/// the durable record pairs monotone in-memory floors with a possibly-stale `commit`, so a record can
/// carry a shape a restart's replay does not reach, and seeding from it would leave a re-committing
/// shape entry stale on this replica while its peers applied it.
///
/// An out-of-tree disk decoder maps an absent field to `0`. That reading is exact for every record
/// this crate can produce — the storeless create door refuses a nonzero founding generation, so a
/// writer of this format founds above zero only through the door that stamps the field — but it is
/// exact for THAT REASON and no other, and the reason is a property of the writer, not of the
/// encoding. A blob written before the field existed is indistinguishable from a recorded zero, and
/// such a blob could name a nonzero incarnation: an absent field there means UNKNOWN, not zero, and
/// a decoder that can encounter one must refuse rather than assume.
///
/// No published version can produce such a blob — the field ships in the first one — so the
/// ambiguity is closed by construction rather than by a provenance flag. A future format change
/// that reintroduces it would need one, and would need the restore door to discriminate: the device
/// is recorded (a store with no shape evidence can only hold a ceremony-granted founding value, so
/// the record may seed it; with shape evidence it cannot be reconstructed and must refuse).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState<I> {
  term: Term,
  vote: Option<I>,
  commit: Index,
  lease_support: LeaseSupport,
  lineage: Option<Box<ForkId>>,
  founding_gen: u64,
}

impl<I> HardState<I> {
  /// The initial durable state of a fresh node.
  #[inline(always)]
  pub const fn initial() -> Self {
    Self {
      term: Term::ZERO,
      vote: None,
      commit: Index::ZERO,
      lease_support: LeaseSupport::Recorded(None),
      lineage: None,
      founding_gen: 0,
    }
  }

  /// The current term.
  #[inline(always)]
  pub const fn term(&self) -> Term {
    self.term
  }

  /// The committed index.
  #[inline(always)]
  pub const fn commit(&self) -> Index {
    self.commit
  }

  /// Replace the term (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_term(mut self, term: Term) -> Self {
    self.term = term;
    self
  }

  /// Replace the committed index (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_commit(mut self, commit: Index) -> Self {
    self.commit = commit;
    self
  }

  /// The durable lease-support record (provenance + magnitude). See [`LeaseSupport`].
  #[inline(always)]
  pub const fn lease_support(&self) -> LeaseSupport {
    self.lease_support
  }

  /// The promised lease-window MAGNITUDE (`self.lease_support().promised()`) — the value the ~majority of
  /// read sites want (fence math input, durability watermark comparison). Use [`lease_support`](Self::lease_support)
  /// when the legacy/native PROVENANCE matters.
  #[inline(always)]
  pub const fn promised_lease_support(&self) -> Option<Duration> {
    self.lease_support.promised()
  }

  /// Replace the lease-support record (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_lease_support(mut self, lease_support: LeaseSupport) -> Self {
    self.lease_support = lease_support;
    self
  }

  /// Replace the vote (consuming builder).
  #[inline(always)]
  #[must_use]
  pub fn with_vote(mut self, vote: Option<I>) -> Self {
    self.vote = vote;
    self
  }

  /// The lineage this node's log belongs to — its fork token, or `None` for a node that never forked
  /// nor adopted. See the type-level doc for why restart needs this durable.
  #[inline(always)]
  pub fn lineage(&self) -> Option<&ForkId> {
    self.lineage.as_deref()
  }

  /// Replace the lineage record (consuming builder).
  #[inline(always)]
  #[must_use]
  pub fn with_lineage(mut self, lineage: Option<ForkId>) -> Self {
    self.lineage = lineage.map(Box::new);
    self
  }

  /// The generation this incarnation was FOUNDED at — a per-incarnation constant, `0` for every
  /// group founded through the storeless create door. See the type-level doc for why a restart may
  /// recover a lineage counter from this and from no other durable per-replica value.
  #[inline(always)]
  pub const fn founding_gen(&self) -> u64 {
    self.founding_gen
  }

  /// Replace the founding generation (consuming builder). Written at the durable choke-point on
  /// every hard-state write, so no builder can persist a record that disowns its incarnation's
  /// founding value.
  #[inline(always)]
  #[must_use]
  pub const fn with_founding_gen(mut self, founding_gen: u64) -> Self {
    self.founding_gen = founding_gen;
    self
  }
}

impl<I: CheapClone> HardState<I> {
  /// Whom this node voted for in `term`, if anyone.
  #[inline(always)]
  pub fn vote(&self) -> Option<I> {
    self.vote.cheap_clone()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn hard_state_defaults_and_accessors() {
    let hs = HardState::<u64>::initial();
    assert_eq!(hs.term(), Term::ZERO);
    assert_eq!(hs.vote(), None);
    assert_eq!(hs.commit(), Index::ZERO);
    // Genesis is RECORDED (current-format), not Unrecorded; it promised nothing.
    assert_eq!(hs.lease_support(), LeaseSupport::Recorded(None));
    assert_eq!(hs.promised_lease_support(), None);
    assert!(!hs.lease_support().is_unrecorded());
    let hs = hs
      .with_term(Term::new(3))
      .with_vote(Some(7))
      .with_commit(Index::new(2))
      .with_lease_support(LeaseSupport::Recorded(Some(Duration::from_millis(500))));
    assert_eq!(hs.term(), Term::new(3));
    assert_eq!(hs.vote(), Some(7));
    assert_eq!(hs.commit(), Index::new(2));
    assert_eq!(
      hs.promised_lease_support(),
      Some(Duration::from_millis(500))
    );
    // `Unrecorded` (a legacy decode) is DISTINCT from `Recorded(None)` and reports no magnitude.
    let legacy = hs.with_lease_support(LeaseSupport::Unrecorded);
    assert_eq!(legacy.lease_support(), LeaseSupport::Unrecorded);
    assert_eq!(legacy.promised_lease_support(), None);
    assert!(legacy.lease_support().is_unrecorded());
    assert_ne!(LeaseSupport::Unrecorded, LeaseSupport::Recorded(None));
  }

  #[test]
  fn lease_support_default_and_promised_arms() {
    // Genesis default is RECORDED-nothing (never `Unrecorded`), distinguishing a native node from a legacy decode.
    assert_eq!(LeaseSupport::default(), LeaseSupport::Recorded(None));
    // `promised()` reports the magnitude on both arms: a recorded value, and `None` for a legacy (unrecorded) record.
    assert_eq!(
      LeaseSupport::Recorded(Some(Duration::from_millis(250))).promised(),
      Some(Duration::from_millis(250))
    );
    assert_eq!(LeaseSupport::Recorded(None).promised(), None);
    assert_eq!(LeaseSupport::Unrecorded.promised(), None);
  }

  #[test]
  fn lease_support_raise_upgrades_provenance_and_is_monotone() {
    use core::time::Duration;
    // raise() upgrades Unrecorded -> Recorded (self-heal) and never lowers a recorded magnitude.
    assert_eq!(
      LeaseSupport::Unrecorded.raise(Some(Duration::from_millis(100))),
      LeaseSupport::Recorded(Some(Duration::from_millis(100)))
    );
    assert_eq!(
      LeaseSupport::Unrecorded.raise(None),
      LeaseSupport::Recorded(None)
    );
    assert_eq!(
      LeaseSupport::Recorded(Some(Duration::from_millis(300)))
        .raise(Some(Duration::from_millis(100))),
      LeaseSupport::Recorded(Some(Duration::from_millis(300))) // never lowers
    );
    assert_eq!(
      LeaseSupport::Recorded(Some(Duration::from_millis(100)))
        .raise(Some(Duration::from_millis(300))),
      LeaseSupport::Recorded(Some(Duration::from_millis(300))) // raises
    );
  }
}
