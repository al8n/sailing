//! Runtime membership: [`Tracker`] holds the joint voter configuration, learner sets, and the
//! per-peer [`Progress`] map. The pure [`confchange`] submodule contains the [`confchange::Changer`]
//! that computes the next configuration from a sequence of [`ConfChangeSingle`] operations.
//!
//! Faithful port of etcd `tracker/tracker.go` and `confchange/confchange.go`.
use crate::{CheapClone, ConfState, Index, JointConfig, MajorityConfig, Progress, VoteResult};
use std::{
  collections::{BTreeMap, BTreeSet},
  vec::Vec,
};

/// A sorted-by-key association list standing in for the old `BTreeMap<I, Progress>`.
///
/// Cluster membership is small (typically 3-7 voters + learners), so a contiguous `Vec` kept in
/// ascending key order serves every access the tracker needs — a binary-search lookup and an
/// O(n) sorted insert/remove — without the per-node B-tree allocation churn the membership-hot
/// `progress_map()` fan-out otherwise pays. Iteration is in ascending key order, IDENTICAL to the
/// `BTreeMap` it replaces, so the replication fan-out's message order (and thus VOPR determinism
/// and the canonical wire order) is unchanged.
#[derive(Debug, Clone)]
struct ProgressMap<I> {
  /// Strictly ascending in the key `I`; every mutator preserves this invariant so iteration and
  /// `binary_search` stay correct.
  entries: Vec<(I, Progress)>,
}

impl<I> ProgressMap<I> {
  fn new() -> Self {
    Self {
      entries: Vec::new(),
    }
  }

  fn as_slice(&self) -> &[(I, Progress)] {
    &self.entries
  }

  fn keys(&self) -> impl Iterator<Item = &I> {
    self.entries.iter().map(|(id, _)| id)
  }

  fn values_mut(&mut self) -> impl Iterator<Item = &mut Progress> {
    self.entries.iter_mut().map(|(_, p)| p)
  }

  fn clear(&mut self) {
    self.entries.clear();
  }
}

impl<I: Ord> ProgressMap<I> {
  fn get(&self, id: &I) -> Option<&Progress> {
    self
      .entries
      .binary_search_by(|(k, _)| k.cmp(id))
      .ok()
      .map(|i| &self.entries[i].1)
  }

  fn get_mut(&mut self, id: &I) -> Option<&mut Progress> {
    match self.entries.binary_search_by(|(k, _)| k.cmp(id)) {
      Ok(i) => Some(&mut self.entries[i].1),
      Err(_) => None,
    }
  }

  fn contains_key(&self, id: &I) -> bool {
    self.entries.binary_search_by(|(k, _)| k.cmp(id)).is_ok()
  }

  /// Insert or replace the entry for `id`, preserving the ascending-key invariant.
  fn insert(&mut self, id: I, p: Progress) {
    match self.entries.binary_search_by(|(k, _)| k.cmp(&id)) {
      Ok(i) => self.entries[i].1 = p,
      Err(i) => self.entries.insert(i, (id, p)),
    }
  }

  fn remove(&mut self, id: &I) -> Option<Progress> {
    match self.entries.binary_search_by(|(k, _)| k.cmp(id)) {
      Ok(i) => Some(self.entries.remove(i).1),
      Err(_) => None,
    }
  }
}

/// Runtime membership state: joint voter configuration, learner sets, and per-peer [`Progress`].
///
/// Port of etcd `tracker.ProgressTracker`. The `voters` field is a [`JointConfig`]; during a
/// simple (non-joint) configuration its `outgoing` half is empty.
///
/// # Invariants (from etcd `checkInvariants`)
///
/// - `voters ∩ learners = ∅` (no node is simultaneously a voter in either joint half and a learner).
/// - `learners ∩ learners_next = ∅`.
/// - Every node in `voters` (both halves) ∪ `learners` ∪ `learners_next` has an entry in `progress`.
/// - When not in a joint configuration: `voters.outgoing` is empty, `learners_next` is empty,
///   `auto_leave` is `false`.
#[derive(Debug, Clone)]
pub struct Tracker<I> {
  voters: JointConfig<I>,
  learners: BTreeSet<I>,
  /// Learners that are staged for promotion: they are voters in the *outgoing* half and therefore
  /// cannot yet be added to `learners` (that would violate `voters ∩ learners = ∅`). They will
  /// be moved into `learners` by [`confchange::Changer::leave_joint`].
  learners_next: BTreeSet<I>,
  auto_leave: bool,
  progress: ProgressMap<I>,
}

impl<I> Default for Tracker<I> {
  /// An empty tracker (no voters, no learners, not in a joint transition).
  fn default() -> Self {
    Self::new()
  }
}

impl<I> Tracker<I> {
  /// Construct an empty tracker. Use [`Tracker::from_conf_state`] to bootstrap from a
  /// snapshot's [`ConfState`].
  pub fn new() -> Self {
    Self {
      voters: JointConfig::from_voters(BTreeSet::new()),
      learners: BTreeSet::new(),
      learners_next: BTreeSet::new(),
      auto_leave: false,
      progress: ProgressMap::new(),
    }
  }

  /// Whether the cluster is currently in a joint (two-phase) configuration transition.
  pub fn is_joint(&self) -> bool {
    !self.voters.outgoing().is_empty()
  }

  /// The full progress list (all voters + learners + learners_next), as `(id, progress)` pairs in
  /// ascending `id` order.
  ///
  /// The order is identical to [`ids`](Self::ids) (every member has a `Progress` entry per the type
  /// invariant), so a caller iterating peers from here is deterministic.
  pub fn progress_map(&self) -> &[(I, Progress)] {
    self.progress.as_slice()
  }

  /// The joint voter configuration.
  #[allow(dead_code, reason = "exercised by unit tests; membership accessor")]
  pub fn voters(&self) -> &JointConfig<I> {
    &self.voters
  }

  /// The current learner set (not staged).
  #[allow(dead_code, reason = "exercised by unit tests; membership accessor")]
  pub fn learners(&self) -> &BTreeSet<I> {
    &self.learners
  }

  /// The staged learner set (waiting for `leave_joint`).
  #[allow(
    dead_code,
    reason = "internal membership accessor; retained for completeness"
  )]
  pub fn learners_next(&self) -> &BTreeSet<I> {
    &self.learners_next
  }

  /// Whether the joint config should be left automatically after it is committed.
  pub fn auto_leave(&self) -> bool {
    self.auto_leave
  }
}

impl<I: Ord> Tracker<I> {
  /// Reset every tracked member's `recent_active` to `false`, then set `leader_id`'s back to
  /// `true` (the leader is always active to itself).
  ///
  /// Called at the start of each CheckQuorum window so that only peers heard from *in this
  /// window* count toward the next `quorum_active` check.
  pub fn reset_recent_active(&mut self, leader_id: I) {
    for pr in self.progress.values_mut() {
      pr.set_recent_active(false);
    }
    if let Some(pr) = self.progress.get_mut(&leader_id) {
      pr.set_recent_active(true);
    }
  }

  /// Whether `id` is a voter in either the incoming or outgoing joint-config half.
  pub fn is_voter(&self, id: &I) -> bool {
    self.voters.incoming().contains(id) || self.voters.outgoing().contains(id)
  }

  /// Whether `id` is a learner (not staged — see `is_learner_next`).
  #[allow(dead_code, reason = "exercised by unit tests; membership accessor")]
  pub fn is_learner(&self, id: &I) -> bool {
    self.learners.contains(id)
  }

  /// Whether `id` is staged in `learners_next` (will become a learner after `leave_joint`).
  #[allow(dead_code, reason = "exercised by unit tests; membership accessor")]
  pub fn is_learner_next(&self, id: &I) -> bool {
    self.learners_next.contains(id)
  }

  /// Shared reference to the progress entry for `id`, if any.
  pub fn progress(&self, id: &I) -> Option<&Progress> {
    self.progress.get(id)
  }

  /// Exclusive reference to the progress entry for `id`, if any.
  pub fn progress_mut(&mut self, id: &I) -> Option<&mut Progress> {
    self.progress.get_mut(id)
  }

  /// Insert or replace a progress entry.
  #[allow(
    dead_code,
    reason = "internal progress mutator; retained for completeness"
  )]
  pub fn insert_progress(&mut self, id: I, p: Progress) {
    self.progress.insert(id, p);
  }

  /// Remove a progress entry. Only use this when you have separately ensured the node is
  /// no longer in any membership set; the invariant checker will catch misuse in tests.
  #[allow(
    dead_code,
    reason = "internal progress mutator; retained for completeness"
  )]
  pub fn remove_progress(&mut self, id: &I) {
    self.progress.remove(id);
  }
}

impl<I: Ord + CheapClone> Tracker<I> {
  /// Bootstrap or restore a tracker from a [`ConfState`] (e.g., from a snapshot or initial
  /// cluster configuration). Creates a fresh [`Progress`] for every voter and learner.
  ///
  /// `last_index` is the last log index known at this point; new peers are probed starting
  /// from `last_index + 1` (their `next_index`).
  pub fn from_conf_state(
    cs: &ConfState<I>,
    last_index: Index,
    max_inflight_msgs: usize,
    max_inflight_bytes: u64,
  ) -> Self {
    let next = last_index.next();
    let mut p = ProgressMap::new();

    // Install a fresh Progress for every member that needs one, without duplicates.
    for id in cs
      .voters()
      .iter()
      .chain(cs.voters_outgoing().iter())
      .chain(cs.learners().iter())
      .chain(cs.learners_next().iter())
    {
      if !p.contains_key(id) {
        p.insert(
          id.cheap_clone(),
          Progress::new(next, max_inflight_msgs, max_inflight_bytes),
        );
      }
    }

    Self {
      voters: JointConfig::new(
        MajorityConfig::new(cs.voters().clone()),
        MajorityConfig::new(cs.voters_outgoing().clone()),
      ),
      learners: cs.learners().clone(),
      learners_next: cs.learners_next().clone(),
      auto_leave: cs.auto_leave(),
      progress: p,
    }
  }

  /// The largest log index jointly committed by both joint-config halves (or by the sole
  /// incoming half when not in a joint transition).
  ///
  /// Uses each voter's `Progress::match_index`; a voter absent from `progress` contributes
  /// [`Index::ZERO`].
  pub fn quorum_committed(&self) -> Index {
    self.voters.committed_index(|id| {
      self
        .progress
        .get(&id)
        .map_or(Index::ZERO, |p| p.match_index())
    })
  }

  /// Tally the votes in `votes` against the joint voter configuration.
  ///
  /// `votes` maps node id → `true` (grant) / `false` (reject); absent ids are treated as
  /// not-yet-voted (→ `None`).
  pub fn vote_result(&self, votes: &BTreeMap<I, bool>) -> VoteResult {
    self.voters.vote_result(|id| votes.get(&id).copied())
  }

  /// Like [`vote_result`](Self::vote_result) but driven by a closure `id → granted?` instead of a
  /// materialized map. The read/lease confirmation paths call this per heartbeat response, so the
  /// closure form avoids allocating a `BTreeMap` (and the `ids()` set) on every ack; only voter ids are
  /// ever queried, so a definite `bool` per id suffices.
  pub fn vote_result_by(&self, granted: impl Fn(I) -> bool) -> VoteResult {
    self.voters.vote_result(|id| Some(granted(id)))
  }

  /// Whether a voter quorum is currently active (i.e. `recent_active` is true for a quorum
  /// of voters in each joint-config half).
  ///
  /// Uses the same `JointConfig::vote_result` machinery as `vote_result`: a voter not in
  /// `progress` contributes `false`; the JOINT rule applies (both halves must have an active
  /// majority). Returns `true` iff the result is `VoteResult::Won`.
  pub fn quorum_active(&self) -> bool {
    self
      .voters
      .vote_result(|id| Some(self.progress.get(&id).is_some_and(|p| p.recent_active())))
      .is_won()
  }

  /// All node IDs tracked: voters (both halves) ∪ learners ∪ learners_next.
  pub fn ids(&self) -> BTreeSet<I> {
    let mut ids = self.voters.ids();
    ids.extend(self.learners.iter().map(CheapClone::cheap_clone));
    ids.extend(self.learners_next.iter().map(CheapClone::cheap_clone));
    ids
  }

  /// Produce a [`ConfState`] snapshot of the current configuration.
  pub fn conf_state(&self) -> ConfState<I> {
    ConfState::new(
      self
        .voters
        .incoming()
        .ids()
        .iter()
        .map(CheapClone::cheap_clone),
      self.learners.iter().map(CheapClone::cheap_clone),
      self
        .voters
        .outgoing()
        .ids()
        .iter()
        .map(CheapClone::cheap_clone),
      self.learners_next.iter().map(CheapClone::cheap_clone),
      self.auto_leave,
    )
  }

  /// Re-initialize a fresh [`Progress`] for **every** current member (voters both halves
  /// ∪ learners ∪ learners_next) starting at `next_index`. Existing progress entries are
  /// discarded and replaced, so calling this after `become_leader` guarantees no member is
  /// missing a `Progress` (a missing voter Progress would read `match_index = ZERO` and
  /// silently block commit advancement).
  ///
  /// `next_index` should be set to `last_log_index + 1` so new peers probe from the right
  /// place. The leader's own entry is left at `match = ZERO` (same as new peers); the
  /// caller is expected to call `progress_mut(self_id).maybe_update(last)` immediately
  /// after to bring self up to date.
  pub fn reset_progress(
    &mut self,
    next_index: Index,
    max_inflight_msgs: usize,
    max_inflight_bytes: u64,
  ) {
    self.progress.clear();
    for id in self.ids() {
      self.progress.insert(
        id,
        Progress::new(next_index, max_inflight_msgs, max_inflight_bytes),
      );
    }
  }
}

/// Pure configuration-change transforms. Port of etcd `confchange/confchange.go`.
///
/// The [`Changer`] carries only the parameters needed to mint new [`Progress`] entries; it takes
/// the current [`Tracker`] by shared reference and returns a new (cloned) [`Tracker`] with the
/// change applied — it never mutates its input.
pub mod confchange {
  use super::Tracker;
  use crate::{
    ConfChangeSingle, ConfChangeType, Index, JointConfig, MajorityConfig, NodeId, Progress,
  };
  use std::collections::BTreeSet;

  /// Why a configuration-change operation was rejected.
  ///
  /// All variants carry the node ID(s) involved when applicable, so callers can produce
  /// structured diagnostics without re-parsing strings.
  #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
  #[non_exhaustive]
  pub enum ConfChangeError {
    /// [`Changer::enter_joint`] was called while already in a joint configuration.
    #[error("config is already joint")]
    AlreadyJoint,

    /// [`Changer::leave_joint`] was called while not in a joint configuration.
    #[error("can't leave a non-joint config")]
    NotJoint,

    /// [`Changer::simple`] was called while already in a joint configuration.
    #[error("can't apply simple config change in joint config")]
    SimpleInJoint,

    /// A simple change would alter more than one voter (use [`Changer::enter_joint`] instead).
    #[error("more than one voter changed without entering joint config")]
    MultipleVoterChanges,

    /// All voters were removed.
    #[error("removed all voters")]
    EmptyVoterSet,

    /// A `LearnersNext` entry was created during a simple change (this is a logic error;
    /// `learners_next` is only valid inside a joint transition).
    #[error("learners_next must be empty after a simple config change")]
    LearnersNextInSimple,

    /// [`Changer::enter_joint`] would produce a joint config with an empty voter set.
    #[error("can't make a zero-voter config joint")]
    EmptyIncomingForJoint,

    /// Invariant violation detected (bug in Changer logic or caller) — a defensive catch-all that never
    /// fires in correct operation; the payload names the violated invariant.
    #[error("config invariant violated: {0}")]
    InvariantViolation(std::string::String),
  }

  /// Pure configuration-change transformer.
  ///
  /// Carries the parameters needed to initialize [`Progress`] for newly added nodes; the
  /// tracker under transformation is passed by reference to each method and is never mutated.
  ///
  /// Port of etcd `confchange.Changer`.
  #[derive(Debug, Clone, Copy)]
  pub struct Changer {
    /// The last log index at the time of the change. New peers probe from `last_index` (floored at 1),
    /// matching etcd `initProgress` — deliberately distinct from `become_leader` / `from_conf_state`,
    /// which start at `last_index + 1`; the probe/reject walk converges the true match either way.
    pub last_index: Index,
    /// Maximum number of in-flight messages per peer (passed to [`Progress::new`]).
    pub max_inflight_msgs: usize,
    /// Maximum total in-flight bytes per peer (passed to [`Progress::new`]).
    pub max_inflight_bytes: u64,
  }

  impl Changer {
    /// Construct a new [`Changer`].
    pub fn new(last_index: Index, max_inflight_msgs: usize, max_inflight_bytes: u64) -> Self {
      Self {
        last_index,
        max_inflight_msgs,
        max_inflight_bytes,
      }
    }

    /// Apply `changes` as a *simple* (non-joint) configuration change.
    ///
    /// Returns an error if:
    /// - `tr` is currently joint (`voters.outgoing` non-empty).
    /// - The change would mutate more than one voter in the incoming set (use
    ///   [`enter_joint`][Self::enter_joint] for that).
    /// - Any `learners_next` entry was created (never valid in a simple change).
    ///
    /// Port of etcd `Changer.Simple`.
    pub fn simple<I: NodeId>(
      &self,
      tr: &Tracker<I>,
      changes: &[ConfChangeSingle<I>],
    ) -> Result<Tracker<I>, ConfChangeError> {
      if tr.is_joint() {
        return Err(ConfChangeError::SimpleInJoint);
      }

      // Take a snapshot of the incoming voters BEFORE applying the change.
      let incoming_before: BTreeSet<I> = tr.voters.incoming().ids().clone();

      let mut next = self.clone_tracker(tr);
      self.apply(&mut next, changes)?;

      // symdiff: count nodes in (before - after) ∪ (after - before).
      let incoming_after = next.voters.incoming().ids();
      let diff = sym_diff(&incoming_before, incoming_after);
      if diff > 1 {
        return Err(ConfChangeError::MultipleVoterChanges);
      }

      if !next.learners_next.is_empty() {
        return Err(ConfChangeError::LearnersNextInSimple);
      }

      self.check_invariants(&next)?;
      Ok(next)
    }

    /// Transition into a joint configuration.
    ///
    /// The current incoming voters are copied to the outgoing half (preserving the old quorum),
    /// then `changes` are applied to the incoming half. `auto_leave` governs whether the leader
    /// will automatically write the `LeaveJoint` entry.
    ///
    /// Returns an error if `tr` is already joint or if the voter set would be empty.
    ///
    /// Port of etcd `Changer.EnterJoint`.
    pub fn enter_joint<I: NodeId>(
      &self,
      tr: &Tracker<I>,
      auto_leave: bool,
      changes: &[ConfChangeSingle<I>],
    ) -> Result<Tracker<I>, ConfChangeError> {
      if tr.is_joint() {
        return Err(ConfChangeError::AlreadyJoint);
      }
      if tr.voters.incoming().is_empty() {
        return Err(ConfChangeError::EmptyIncomingForJoint);
      }

      let mut next = self.clone_tracker(tr);

      // Copy incoming → outgoing (snapshot the old config).
      let outgoing_ids = next.voters.incoming().ids().clone();
      next.voters = JointConfig::new(
        next.voters.incoming().clone(),
        MajorityConfig::new(outgoing_ids),
      );

      self.apply(&mut next, changes)?;
      next.auto_leave = auto_leave;

      self.check_invariants(&next)?;
      Ok(next)
    }

    /// Leave the joint configuration, transitioning to the new simple config.
    ///
    /// - Moves `learners_next` into `learners` (staged demotions take effect).
    /// - Clears `voters_outgoing`.
    /// - Drops `Progress` for any node no longer in voters ∪ learners.
    /// - Clears `auto_leave`.
    ///
    /// Returns an error if `tr` is not currently joint.
    ///
    /// Port of etcd `Changer.LeaveJoint`.
    pub fn leave_joint<I: NodeId>(&self, tr: &Tracker<I>) -> Result<Tracker<I>, ConfChangeError> {
      if !tr.is_joint() {
        return Err(ConfChangeError::NotJoint);
      }

      let mut next = self.clone_tracker(tr);

      // Move staged learners into learners.
      let staged: BTreeSet<I> = next.learners_next.clone();
      for id in staged {
        next.learners.insert(id);
      }
      next.learners_next.clear();

      // Drop Progress for ids that were only in outgoing and are now gone.
      let outgoing_ids: BTreeSet<I> = next.voters.outgoing().ids().clone();
      // Clear the outgoing half.
      next.voters = JointConfig::new(
        next.voters.incoming().clone(),
        MajorityConfig::new(BTreeSet::new()),
      );
      next.auto_leave = false;

      // Remove progress for nodes that are no longer in any membership set.
      let still_needed: BTreeSet<I> = next.ids();
      for id in &outgoing_ids {
        if !still_needed.contains(id) {
          next.progress.remove(id);
        }
      }

      self.check_invariants(&next)?;
      Ok(next)
    }

    /// Deep-clone `tr` into a new [`Tracker`] (the Changer only mutates its copy).
    fn clone_tracker<I: NodeId>(&self, tr: &Tracker<I>) -> Tracker<I> {
      Tracker {
        voters: tr.voters.clone(),
        learners: tr.learners.clone(),
        learners_next: tr.learners_next.clone(),
        auto_leave: tr.auto_leave,
        progress: tr.progress.clone(),
      }
    }

    /// Apply each [`ConfChangeSingle`] to `tr` in order.
    ///
    /// Port of etcd `Changer.apply`.
    fn apply<I: NodeId>(
      &self,
      tr: &mut Tracker<I>,
      changes: &[ConfChangeSingle<I>],
    ) -> Result<(), ConfChangeError> {
      for cc in changes {
        match cc.ty() {
          ConfChangeType::AddNode => self.make_voter(tr, cc.node()),
          ConfChangeType::AddLearnerNode => self.make_learner(tr, cc.node()),
          ConfChangeType::RemoveNode => self.remove(tr, cc.node()),
        }
      }
      // After applying all changes, the incoming voter set must not be empty.
      if tr.voters.incoming().is_empty() {
        return Err(ConfChangeError::EmptyVoterSet);
      }
      Ok(())
    }

    /// Add or promote `id` to be a voter in the incoming majority config.
    ///
    /// - If no Progress exists yet, create one (the node is brand new).
    /// - Remove `id` from `learners` and `learners_next` (a node cannot be both).
    /// - Add `id` to the incoming voter set.
    ///
    /// Port of etcd `Changer.makeVoter`.
    fn make_voter<I: NodeId>(&self, tr: &mut Tracker<I>, id: I) {
      if !tr.progress.contains_key(&id) {
        self.init_progress(tr, id, false);
        return;
      }
      // Promote: remove from all learner sets, add to incoming voters.
      tr.learners.remove(&id);
      tr.learners_next.remove(&id);
      // Add to incoming voters via rebuild.
      let mut incoming_ids = tr.voters.incoming().ids().clone();
      incoming_ids.insert(id);
      tr.voters = JointConfig::new(
        MajorityConfig::new(incoming_ids),
        tr.voters.outgoing().clone(),
      );
    }

    /// Make `id` a learner or stage it for later demotion.
    ///
    /// **The key joint-consensus rule (etcd `makeLearner`):**
    /// If `id` is currently in the *outgoing* voters half, it is still required for the old
    /// quorum — we cannot mark it as a learner yet (that would violate `voters ∩ learners = ∅`).
    /// Instead, we stage it in `learners_next`; it becomes a real learner in
    /// [`leave_joint`][Self::leave_joint] once the joint config is committed and the outgoing
    /// half is cleared.
    ///
    /// If `id` is NOT in the outgoing half, it is demoted immediately: removed from incoming
    /// voters and added to `learners`.
    ///
    /// Port of etcd `Changer.makeLearner`.
    fn make_learner<I: NodeId>(&self, tr: &mut Tracker<I>, id: I) {
      if !tr.progress.contains_key(&id) {
        // Brand new node added directly as a learner.
        self.init_progress(tr, id, true);
        return;
      }

      // Already a learner → no-op (idempotent).
      // Check by seeing if it is in learners but NOT a voter.
      if tr.learners.contains(&id) && !tr.voters.incoming().contains(&id) {
        return;
      }

      // Save the existing Progress before remove() might delete it (remove() keeps Progress
      // only if the id is still in the outgoing half; we re-attach it after).
      let saved_pr = tr.progress.get(&id).cloned();

      // Remove from incoming voters / learners / learners_next.
      self.remove(tr, id.cheap_clone());

      // Restore Progress that remove() may have deleted (we still need it).
      if let Some(pr) = saved_pr
        && !tr.progress.contains_key(&id)
      {
        tr.progress.insert(id.cheap_clone(), pr);
      }

      // If id is in the outgoing voters half, it still participates in the old quorum.
      // We cannot add it to learners yet — stage it in learners_next instead.
      if tr.voters.outgoing().contains(&id) {
        tr.learners_next.insert(id);
        // Do NOT add to learners (would violate voters ∩ learners = ∅).
      } else {
        tr.learners.insert(id);
      }
    }

    /// Remove `id` from the incoming voters, learners, and learners_next sets.
    ///
    /// **The key joint-consensus rule (etcd `remove`):**
    /// The `Progress` entry is deleted **only if `id` is NOT in the outgoing voters half**.
    /// When `id` is still in the outgoing half, it continues to participate in the old quorum,
    /// so its `Progress` must be kept alive until [`leave_joint`][Self::leave_joint].
    ///
    /// Port of etcd `Changer.remove`.
    fn remove<I: NodeId>(&self, tr: &mut Tracker<I>, id: I) {
      if !tr.progress.contains_key(&id) {
        return;
      }

      // Remove from incoming voters.
      let mut incoming_ids = tr.voters.incoming().ids().clone();
      incoming_ids.remove(&id);
      tr.voters = JointConfig::new(
        MajorityConfig::new(incoming_ids),
        tr.voters.outgoing().clone(),
      );

      // Remove from learners and learners_next.
      tr.learners.remove(&id);
      tr.learners_next.remove(&id);

      // Drop Progress only if the node is NOT still needed by the outgoing config.
      if !tr.voters.outgoing().contains(&id) {
        tr.progress.remove(&id);
      }
    }

    /// Initialize a fresh [`Progress`] for `id` and add it to the appropriate membership set.
    ///
    /// Port of etcd `Changer.initProgress`.
    fn init_progress<I: NodeId>(&self, tr: &mut Tracker<I>, id: I, is_learner: bool) {
      let next = Index::new(self.last_index.get().max(1)); // invariant: match < next
      let pr = Progress::new(next, self.max_inflight_msgs, self.max_inflight_bytes);
      tr.progress.insert(id.cheap_clone(), pr);

      if is_learner {
        tr.learners.insert(id);
      } else {
        let mut incoming_ids = tr.voters.incoming().ids().clone();
        incoming_ids.insert(id);
        tr.voters = JointConfig::new(
          MajorityConfig::new(incoming_ids),
          tr.voters.outgoing().clone(),
        );
      }
    }

    /// Verify the core membership invariants.
    ///
    /// These are the same checks etcd's `checkInvariants` performs. Failures here indicate a
    /// bug in the Changer logic.
    fn check_invariants<I: NodeId>(&self, tr: &Tracker<I>) -> Result<(), ConfChangeError> {
      // 1. Every member in voters(both) ∪ learners ∪ learners_next must have a Progress.
      for id in tr.voters.incoming().ids() {
        if !tr.progress.contains_key(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "no progress for voter {id}"
          )));
        }
      }
      for id in tr.voters.outgoing().ids() {
        if !tr.progress.contains_key(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "no progress for outgoing voter {id}"
          )));
        }
      }
      for id in &tr.learners {
        if !tr.progress.contains_key(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "no progress for learner {id}"
          )));
        }
      }
      for id in &tr.learners_next {
        if !tr.progress.contains_key(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "no progress for learners_next {id}"
          )));
        }
      }

      // 2. learners_next nodes must be in the outgoing voters half.
      for id in &tr.learners_next {
        if !tr.voters.outgoing().contains(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "{id} is in learners_next but not in outgoing voters"
          )));
        }
      }

      // 3. learners must not intersect with either voter half.
      for id in &tr.learners {
        if tr.voters.outgoing().contains(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "{id} is in learners and outgoing voters"
          )));
        }
        if tr.voters.incoming().contains(id) {
          return Err(ConfChangeError::InvariantViolation(std::format!(
            "{id} is in learners and incoming voters"
          )));
        }
      }

      // 4. When not joint: outgoing must be empty, learners_next must be empty, auto_leave=false.
      if !tr.is_joint() {
        if !tr.learners_next.is_empty() {
          return Err(ConfChangeError::InvariantViolation(
            "learners_next must be empty when not joint".into(),
          ));
        }
        if tr.auto_leave {
          return Err(ConfChangeError::InvariantViolation(
            "auto_leave must be false when not joint".into(),
          ));
        }
      }

      // 5. Debug: no orphan Progress entries.
      debug_assert!(
        {
          let needed = tr.ids();
          tr.progress.keys().all(|id| needed.contains(id))
        },
        "orphan Progress entries detected"
      );

      Ok(())
    }
  }

  /// Count of the symmetric difference between two `BTreeSet`s:
  /// `|(a - b) ∪ (b - a)|`.
  ///
  /// Port of etcd `symdiff`.
  fn sym_diff<I: Ord>(a: &BTreeSet<I>, b: &BTreeSet<I>) -> usize {
    let only_a = a.iter().filter(|id| !b.contains(id)).count();
    let only_b = b.iter().filter(|id| !a.contains(id)).count();
    only_a + only_b
  }
}

#[cfg(test)]
mod tests;
