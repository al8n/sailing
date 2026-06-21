//! The durable Raft metadata: `(term, vote, commit, lease_support)`, persisted before acting.
use crate::{Index, Term};
use core::time::Duration;

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
/// `lease_support` is the durable shadow of `HeartbeatResp.lease_support` — the lease window this node has
/// advertised it will uphold — persisted so a restarted node keeps the promise its prior incarnation made
/// to the network, sizing the post-restart vote fence by the PROMISE rather than by the (possibly weaker)
/// post-restart config, with the provenance needed to handle a legacy upgrade safely. It is
/// the lease analogue of persisting `vote` (persist-before-advertise, the sibling of persist-before-ack).
/// An out-of-tree disk decoder MUST map a genuine pre-`lease_support` blob to [`LeaseSupport::Unrecorded`]
/// (never `Recorded(None)`): `Unrecorded` triggers the conservative restart fence, so a freshly-upgraded
/// node is never less safe than before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardState<I> {
  term: Term,
  vote: Option<I>,
  commit: Index,
  lease_support: LeaseSupport,
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
}

impl<I: Copy> HardState<I> {
  /// Whom this node voted for in `term`, if anyone.
  #[inline(always)]
  pub const fn vote(&self) -> Option<I> {
    self.vote
  }

  /// Replace the vote (consuming builder).
  #[inline(always)]
  #[must_use]
  pub const fn with_vote(mut self, vote: Option<I>) -> Self {
    self.vote = vote;
    self
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
      .with_lease_support(LeaseSupport::Recorded(Some(
        core::time::Duration::from_millis(500),
      )));
    assert_eq!(hs.term(), Term::new(3));
    assert_eq!(hs.vote(), Some(7));
    assert_eq!(hs.commit(), Index::new(2));
    assert_eq!(
      hs.promised_lease_support(),
      Some(core::time::Duration::from_millis(500))
    );
    // `Unrecorded` (a legacy decode) is DISTINCT from `Recorded(None)` and reports no magnitude.
    let legacy = hs.with_lease_support(LeaseSupport::Unrecorded);
    assert_eq!(legacy.lease_support(), LeaseSupport::Unrecorded);
    assert_eq!(legacy.promised_lease_support(), None);
    assert!(legacy.lease_support().is_unrecorded());
    assert_ne!(LeaseSupport::Unrecorded, LeaseSupport::Recorded(None));
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
