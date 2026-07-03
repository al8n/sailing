//! `MultiStreamCoordinator<G, I, F, R>`: the multi-group stream-transport super state machine.
//!
//! It composes a [`MultiRaft`] of single-group endpoints with a [`PeerRouter`]: inbound frames are
//! demuxed by their group tag and fed to the owning group's endpoint, and each group's outbound
//! messages are routed back stamped with that group's id. One connection per peer carries every
//! co-located group's traffic. Storage is per group: [`handle_conn_data`](MultiStreamCoordinator::handle_conn_data)
//! resolves each decoded frame's group store through a caller-supplied [`GroupStores`], while the
//! single-group driving methods take the target group's store directly.
use super::super::{
  CoalescedEntry, ConnId, TransportError, frame::COALESCED_FLAG_QUIESCE, router::PeerRouter,
  stream::RecordIo,
};
use crate::{
  Config, CreateGroupError, Data, Endpoint, Event, GroupId, Index, Instant, LogStore, Message,
  MultiRaft, NodeId, Now, ProposeError, StableStore, StateMachine, StorageProgress,
};
use std::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  vec::Vec,
};

/// Per-group storage a [`MultiStreamCoordinator`] uses to drive each group's endpoint when inbound
/// bytes span multiple groups. The caller implements it over its own per-group store table.
///
/// CONTRACT: resolution must be STABLE (the same group always resolves to the same stores) and
/// NON-ALIASING (two groups must never share a store — a shared log is a safety violation).
/// Returning `Some` for a group the `MultiRaft` does not host is a harmless per-message drop;
/// returning `None` for a hosted group starves it.
pub trait GroupStores<G, L, S> {
  /// The `(log, stable)` stores for `group`, or `None` if this host has no storage for it — an
  /// inbound message for an unknown group is then dropped (the sender retries on its own cadence).
  fn stores(&mut self, group: &G) -> Option<(&mut L, &mut S)>;
}

/// A group-scoped scheduling signal a multi-group coordinator surfaces to its driver (drained via
/// `poll_group_control`, like `poll_event`). The queue preserves DISPATCH ORDER, so a driver that
/// folds the signals left to right always lands on the group's latest state — e.g. a quiesce-flagged
/// beat followed by an append in the same read yields `Quiesce` then `Wake`, ending awake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupControl {
  /// The group's leader quiesced it after the beat that carried this flag: the peer promised
  /// heartbeat silence, so the driver may stop arming/sweeping the group's timers — safe because
  /// any traffic surfaces a [`Wake`](Self::Wake) and a connection loss is the driver's own signal
  /// to wake everything.
  Quiesce,
  /// WAKE-class inbound traffic was dispatched to the group. The absorbed (non-waking) complement
  /// is exactly one heartbeat round's response tail — `HeartbeatResponse`, the empty
  /// `AppendEntries` the leader pumps back at each responder, and that probe's `AppendResponse` —
  /// because a quiescing group's FINAL flagged round still exchanges precisely those, and waking
  /// on any of them would re-arm a timer and keep the round-trip alive forever. Absorbing them is
  /// safe: each can only be the echo of a round the quiesced side itself completed pre-quiesce
  /// (a quiesced leader sends no new beats or appends to respond to; quiesce eligibility required
  /// every voter matched and commit applied, so no straggler ack can carry new progress, and a
  /// commit can only advance behind a NON-empty append that already woke its follower).
  /// Everything else wakes: a `Heartbeat` tells a quiesced follower its leader is active again
  /// (restoring the silent-wedge detection its swept election timer provides), a non-empty
  /// `AppendEntries` is live replication, votes/transfers/reads/snapshots are live consensus —
  /// and a connection loss, the liveness oracle, is the driver's own wake signal.
  Wake,
}

/// A multi-group consensus node speaking over framed reliable connections (`R` is the record layer,
/// e.g. `Labeled<Passthrough>` for TCP or `Labeled<TlsRecords>` for TLS).
pub struct MultiStreamCoordinator<G, I, F, R>
where
  F: StateMachine,
{
  multi: MultiRaft<G, I, F>,
  router: PeerRouter<I, R>,
  next_conn_id: u64,
  /// Heartbeat/HeartbeatResponse batches diverted per peer by [`flush`](Self::flush), shipped as
  /// coalesced frames at the [`poll_transmit`](Self::poll_transmit) chokepoint. Batching across
  /// flushes (rather than per flush) is what coalesces a driver's PER-GROUP `handle_timeout` sweep
  /// — each call flushes separately, but all of a crank's beats leave in that crank's transmit
  /// drain as one frame per peer.
  hb_batches: BTreeMap<I, Vec<CoalescedEntry<I>>>,
  /// Groups whose NEXT heartbeat broadcast carries the QUIESCE flag (set by
  /// [`mark_quiescing`](Self::mark_quiescing), consumed by the flush that stamps the beats).
  quiesce_intents: BTreeSet<G>,
  /// Group-scoped scheduling signals for the driver, in dispatch order (see [`GroupControl`]).
  controls: VecDeque<(G, GroupControl)>,
}

impl<G, I, F, R> MultiStreamCoordinator<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: core::error::Error,
  R: RecordIo,
{
  /// A coordinator hosting no groups and no connections.
  #[must_use]
  pub fn new() -> Self {
    Self {
      multi: MultiRaft::new(),
      router: PeerRouter::new(),
      next_conn_id: 1,
      hb_batches: BTreeMap::new(),
      quiesce_intents: BTreeSet::new(),
      controls: VecDeque::new(),
    }
  }

  /// Create a fresh group (see [`MultiRaft::create_group`]).
  ///
  /// # Errors
  /// The admission checks of [`MultiRaft::create_group`] — see [`CreateGroupError`].
  pub fn create_group(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
  ) -> Result<(), CreateGroupError> {
    self.multi.create_group(gid, config, now, seed, fsm)
  }

  /// Recover a group from durable storage (see [`MultiRaft::restore_group`]).
  ///
  /// # Errors
  /// The admission checks of [`MultiRaft::restore_group`] — see [`CreateGroupError`].
  #[allow(clippy::too_many_arguments)]
  pub fn restore_group<L, S>(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
    boot_epoch: u64,
    log: &mut L,
    stable: &mut S,
  ) -> Result<(), CreateGroupError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    self
      .multi
      .restore_group(gid, config, now, seed, fsm, boot_epoch, log, stable)
  }

  /// Remove a group, returning its endpoint if present. Drops the group's pending quiesce intent
  /// and queued controls with it (its per-peer batched beats, if any, still ship — a removed
  /// group's in-flight heartbeat is indistinguishable from one that left just before removal, and
  /// the receiver's unhosted-entry drop absorbs it).
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F>> {
    self.quiesce_intents.remove(gid);
    self.controls.retain(|(g, _)| g != gid);
    self.multi.remove_group(gid)
  }

  /// Register a freshly opened connection, returning the coordinator-assigned [`ConnId`] the driver
  /// keys its socket by.
  ///
  /// # Panics
  /// Panics if the `u64` connection-id space is exhausted (unreachable in practice).
  pub fn on_conn_open(&mut self, record: R, now: Instant) -> ConnId {
    let id = ConnId(self.next_conn_id);
    self.next_conn_id = self
      .next_conn_id
      .checked_add(1)
      .expect("connection id space exhausted");
    self.router.register(id, record, now);
    id
  }

  /// Tear down a driver-closed connection.
  pub fn on_conn_close(&mut self, conn: ConnId) {
    self.router.remove(conn);
  }

  /// The next connection the transport closed on its own initiative, with the fault if any.
  pub fn poll_conn_closed(&mut self) -> Option<(ConnId, Option<TransportError>)> {
    self.router.poll_conn_closed()
  }

  /// Feed inbound bytes from `conn`: decode each frame, resolve its group's store through `stores`,
  /// feed the owning group's endpoint, then flush every group's resulting outbound messages. A frame
  /// whose group has no store is dropped (the sender retries on its own cadence); a group tag that
  /// does not decode as `G` closes the connection as integrity-suspect (reported via
  /// [`poll_conn_closed`](Self::poll_conn_closed)).
  pub fn handle_conn_data<L, S, St>(
    &mut self,
    conn: ConnId,
    bytes: &[u8],
    eof: bool,
    now: impl Into<Now>,
    stores: &mut St,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
    St: GroupStores<G, L, S>,
  {
    let now: Now = now.into();
    let mut decoded = Vec::new();
    let _ = self
      .router
      .handle_conn_data(conn, bytes, eof, now.mono(), &mut decoded);
    for (group_bytes, flags, from, msg) in decoded {
      let Ok(group) = G::decode_exact(group_bytes) else {
        // A well-framed tag that is not a valid `G` is a systematic peer fault (a different
        // group-id type, or a single-group node on a multi-group cluster): every frame reproduces
        // it, and dropping silently would black-hole consensus traffic on a healthy-looking
        // connection. Close as integrity-suspect — the connection's remaining frames are equally
        // suspect.
        self.router.close(conn, Some(TransportError::Decode));
        break;
      };
      // Receive-side gate on the quiesce bit: it is only ever STAMPED on a leader's own
      // Heartbeat broadcast, so a flagged anything-else is a protocol violation (a buggy or
      // stale-version peer) — and honoring it would freeze this group on a message class that
      // deliberately emits no Wake. Close as integrity-suspect, the uniform violation policy.
      if flags & COALESCED_FLAG_QUIESCE != 0 && !msg.is_heartbeat() {
        self.router.close(conn, Some(TransportError::Decode));
        break;
      }
      if let Some((log, stable)) = stores.stores(&group) {
        let wake = Self::is_wake_class(&msg);
        // `None` here means `stores` resolved a group `MultiRaft` does not host — the same
        // unhosted-group drop as a missing store; an unhosted entry's flags drop with it.
        if self
          .multi
          .handle_message(&group, now, log, stable, from, msg)
          .is_some()
        {
          self.push_dispatch_controls(&group, wake, flags);
        }
      }
    }
    self.flush();
  }

  /// Propose a command on `group`'s leader, replicating immediately. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn submit_propose<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.propose(group, now, log, stable, cmd)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Append a proposal on `group` WITHOUT fanning out its `AppendEntries` now; the caller drives
  /// replication afterward via [`flush_appends`](Self::flush_appends), once per crank, so a burst
  /// coalesces into one broadcast per peer. Direct callers should prefer
  /// [`submit_propose`](Self::submit_propose). `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn submit_propose_deferred<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.propose(group, now, log, stable, cmd)?;
    self.flush();
    Some(r)
  }

  /// Ship `group`'s coalesced replication batch and flush. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn flush_appends<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    self.multi.flush_appends(group, now, log, stable)?;
    self.flush();
    Some(())
  }

  /// Propose a membership change (single-step) on `group`, replicating immediately. `None` if no
  /// such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_conf_change<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChange<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_conf_change(group, now, log, stable, cc)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a membership change (joint-consensus capable) on `group`, replicating immediately.
  /// `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_conf_change_v2<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    cc: crate::ConfChangeV2<I>,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_conf_change_v2(group, now, log, stable, cc)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Propose a cluster-wide read-mode migration on `group`, replicating immediately. `None` if no
  /// such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn propose_read_mode_change<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &S,
    mode: crate::ReadOnlyOption,
  ) -> Option<Result<Index, ProposeError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self
      .multi
      .propose_read_mode_change(group, now, log, stable, mode)?;
    let _ = self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Initiate a linearizable read on `group`; the resulting `ReadState` surfaces via
  /// [`poll_event`](Self::poll_event) stamped with the group. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn read_index<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    context: bytes::Bytes,
  ) -> Option<Result<(), crate::ReadIndexError>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.read_index(group, now, log, stable, context)?;
    self.flush();
    Some(r)
  }

  /// Begin transferring `group`'s leadership to `to`. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn transfer_leader<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &L,
    stable: &S,
    to: I,
  ) -> Option<Result<(), crate::TransferError<I>>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let r = self.multi.transfer_leader(group, now, log, stable, to)?;
    self.flush();
    Some(r)
  }

  /// The bound connection for `peer`, if any — the driver's input for its redial policy.
  pub fn conn_of(&self, peer: &I) -> Option<ConnId> {
    self.router.conn_of(peer)
  }

  /// Fire `group`'s timers (and the transport's handshake reaping), then flush. `None` if no such
  /// group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_timeout<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<()>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    self.router.reap_handshakes(now.mono());
    self.multi.handle_timeout(group, now, log, stable)?;
    self.flush();
    Some(())
  }

  /// Drain `group`'s storage completions, then flush. `None` if no such group.
  #[must_use = "`None` means no group with this id is hosted — the call did nothing"]
  pub fn handle_storage<L, S>(
    &mut self,
    group: &G,
    now: impl Into<Now>,
    log: &mut L,
    stable: &mut S,
  ) -> Option<StorageProgress>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let now: Now = now.into();
    let progress = self.multi.handle_storage(group, now, log, stable)?;
    self.flush();
    Some(progress)
  }

  /// Drain queued outbound wire bytes as `(conn, bytes)` pairs for the driver to write. This is
  /// the drain-end chokepoint where the crank's batched heartbeats ship (one coalesced frame per
  /// peer — see [`flush`](Self::flush)), so every `handle_*` call's beats leave with that call's
  /// transmit drain.
  pub fn poll_transmit(&mut self) -> Vec<(ConnId, Vec<u8>)> {
    self.ship_heartbeats();
    self.router.poll_transmit()
  }

  /// Record the intent to QUIESCE `group`: its next heartbeat broadcast is stamped with the
  /// quiesce flag (every copy in that broadcast — all followers hear the promise), after which the
  /// intent clears and [`is_quiescing`](Self::is_quiescing) reports `false`. The driver then stops
  /// arming the group's timers; each follower surfaces [`GroupControl::Quiesce`] to its own driver.
  /// A no-op for an unhosted group.
  pub fn mark_quiescing(&mut self, group: &G) {
    if self.multi.contains_group(group) {
      self.quiesce_intents.insert(group.cheap_clone());
    }
  }

  /// Whether `group`'s quiesce intent is still pending (its flagged beat has not yet been stamped).
  /// The driver's cue to move the group into its quiesced set once this flips to `false`.
  #[must_use]
  pub fn is_quiescing(&self, group: &G) -> bool {
    self.quiesce_intents.contains(group)
  }

  /// Cancel a pending quiesce intent for `group` (a no-op if none). The driver calls this on
  /// EVERY un-quiesce trigger — a wake control, a local command, a connection loss, a leadership
  /// change — so an intent recorded before the wake can never be stamped onto a later beat: the
  /// eligibility that justified it no longer holds.
  pub fn cancel_quiescing(&mut self, group: &G) {
    self.quiesce_intents.remove(group);
  }

  /// Drain the next group-scoped scheduling signal, in dispatch order (see [`GroupControl`]).
  pub fn poll_group_control(&mut self) -> Option<(G, GroupControl)> {
    self.controls.pop_front()
  }

  /// The transport's OWN earliest deadline (handshake reaping), without any group's consensus
  /// deadline — the [`poll_timeout`](Self::poll_timeout) decomposition a quiescing driver needs:
  /// it folds this with the non-quiesced subset of [`deadlines`](Self::deadlines) instead of the
  /// all-groups aggregate.
  #[must_use]
  pub fn transport_timeout(&self) -> Option<Instant> {
    self.router.next_handshake_deadline()
  }

  /// The earliest deadline the driver must wake for: the minimum over every group's consensus
  /// deadline AND the transport's earliest handshake deadline. The transport half matters when no
  /// group surfaces a deadline at all (zero hosted groups, every group poisoned, or a host of
  /// non-voter learners) — without it, un-validated connections would never be reaped. On expiry
  /// call [`handle_transport_timeout`](Self::handle_transport_timeout) unconditionally and
  /// [`handle_timeout`](Self::handle_timeout) for whichever groups are due.
  #[must_use]
  pub fn poll_timeout(&self) -> Option<Instant> {
    match (
      self.multi.poll_timeout(),
      self.router.next_handshake_deadline(),
    ) {
      (Some(a), Some(b)) => Some(a.min(b)),
      (a, None) => a,
      (None, b) => b,
    }
  }

  /// Fire the transport's own housekeeping (handshake-deadline reaping) without touching any
  /// group. Call at every [`poll_timeout`](Self::poll_timeout) expiry: the surfaced deadline may
  /// be a transport deadline with no group due.
  pub fn handle_transport_timeout(&mut self, now: impl Into<Now>) {
    let now: Now = now.into();
    self.router.reap_handshakes(now.mono());
  }

  /// Each group's next deadline — a driver's input for an aggregate timing wheel.
  pub fn deadlines(&self) -> impl Iterator<Item = (G, Instant)> + '_ {
    self.multi.deadlines()
  }

  /// Drain the next application event, stamped with its originating group.
  pub fn poll_event(&mut self) -> Option<(G, Event<I, F::Response>)> {
    self.multi.poll_event()
  }

  /// A group's endpoint, for observability (role, term, commit, the state machine). `None` if no
  /// such group.
  pub fn group(&self, gid: &G) -> Option<&Endpoint<I, F>> {
    self.multi.group(gid)
  }

  /// Route every group's queued outbound messages, stamping each frame with its group tag.
  /// `Heartbeat`/`HeartbeatResponse` are DIVERTED into per-peer batches (shipped coalesced at the
  /// [`poll_transmit`](Self::poll_transmit) chokepoint); everything else routes immediately as its
  /// own frame. Reordering a batched beat behind a same-crank `AppendEntries` is safe by
  /// construction: a heartbeat's `commit` is clamped to the follower's acked match, so no beat can
  /// reference state its follower has not durably acknowledged.
  ///
  /// A group with a pending quiesce intent has EVERY beat copy in this drain stamped with the
  /// quiesce flag (the whole broadcast — each follower must hear the promise), and the intent is
  /// consumed at drain end.
  fn flush(&mut self) {
    let mut group_bytes = Vec::new();
    let mut stamped: BTreeSet<G> = BTreeSet::new();
    while let Some((group, out)) = self.multi.poll_message() {
      let (to, msg) = out.into_parts();
      if msg.is_heartbeat() || msg.is_heartbeat_response() {
        // The quiesce flag rides ONLY the leader's own Heartbeat broadcast. Stamping a
        // HeartbeatResponse would leak a STALE intent: a leader that marked quiescing and was
        // then woken or deposed before its next beat still responds to the NEW leader's beats —
        // a flagged response would freeze the very leader that never chose to quiesce.
        let flags = if msg.is_heartbeat() && self.quiesce_intents.contains(&group) {
          stamped.insert(group.cheap_clone());
          COALESCED_FLAG_QUIESCE
        } else {
          0
        };
        let mut gb = Vec::new();
        group.encode(&mut gb);
        self
          .hb_batches
          .entry(to)
          .or_default()
          .push((flags, gb, msg));
      } else {
        group_bytes.clear();
        group.encode(&mut group_bytes);
        self.router.route(&group_bytes, to, &msg);
      }
    }
    for group in &stamped {
      self.quiesce_intents.remove(group);
    }
  }

  /// Ship the batched heartbeats: a batch of ONE unflagged beat goes as a normal single-message
  /// frame (no format change for the trivial case); anything else — many beats, or a flagged one
  /// (only a coalesced entry has a flags byte) — ships as coalesced frames.
  fn ship_heartbeats(&mut self) {
    if self.hb_batches.is_empty() {
      return;
    }
    for (to, batch) in core::mem::take(&mut self.hb_batches) {
      if let [(0, group_bytes, msg)] = batch.as_slice() {
        self.router.route(group_bytes, to, msg);
      } else {
        self.router.route_coalesced(to, &batch);
      }
    }
  }

  /// Queue the dispatch-driven [`GroupControl`]s for one delivered message: a `Wake` for every
  /// wake-class kind (see [`GroupControl::Wake`] — the heartbeat round's response tail is
  /// absorbed), then a `Quiesce` if the entry carried the flag — flag AFTER wake, so a flagged
  /// beat nets quiesced. Consecutive duplicates collapse (a burst of appends is one `Wake`).
  fn push_dispatch_controls(&mut self, group: &G, wake: bool, flags: u8) {
    if wake {
      self.push_control(group, GroupControl::Wake);
    }
    if flags & COALESCED_FLAG_QUIESCE != 0 {
      self.push_control(group, GroupControl::Quiesce);
    }
  }

  /// Whether a delivered message is WAKE-class for its group. The absorbed complement is exactly
  /// the response tail of one heartbeat round in this codebase — `HeartbeatResponse`, the empty
  /// `AppendEntries` the leader pumps back at each responder, and that probe's `AppendResponse` —
  /// so a quiescing group's FINAL flagged round dies out instead of re-waking either side (see
  /// [`GroupControl::Wake`] for the safety argument).
  fn is_wake_class(msg: &Message<I>) -> bool {
    match msg {
      Message::HeartbeatResponse(_) | Message::AppendResponse(_) => false,
      Message::AppendEntries(ae) => !ae.entries().is_empty(),
      _ => true,
    }
  }

  fn push_control(&mut self, group: &G, ctrl: GroupControl) {
    if self
      .controls
      .back()
      .is_some_and(|(g, c)| g == group && *c == ctrl)
    {
      return;
    }
    self.controls.push_back((group.cheap_clone(), ctrl));
  }
}

impl<G, I, F, R> Default for MultiStreamCoordinator<G, I, F, R>
where
  G: GroupId,
  I: NodeId,
  F: StateMachine,
  F::Command: Data,
  F::Snapshot: Data,
  F::Error: core::error::Error,
  R: RecordIo,
{
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
