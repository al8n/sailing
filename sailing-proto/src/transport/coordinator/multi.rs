//! `MultiStreamCoordinator<G, I, F, R>`: the multi-group stream-transport super state machine.
//!
//! It composes a [`MultiRaft`] of single-group endpoints with a [`PeerRouter`]: inbound frames are
//! demuxed by their group tag and fed to the owning group's endpoint, and each group's outbound
//! messages are routed back stamped with that group's id. One connection per peer carries every
//! co-located group's traffic. Storage is per group: [`handle_conn_data`](MultiStreamCoordinator::handle_conn_data)
//! resolves each decoded frame's group store through a caller-supplied [`GroupStores`], while the
//! single-group driving methods take the target group's store directly.
use super::super::{ConnId, TransportError, router::PeerRouter, stream::RecordIo};
use crate::{
  Config, Data, Endpoint, Event, GroupExists, GroupId, Index, Instant, LogStore, MultiRaft, NodeId,
  Now, ProposeError, StableStore, StateMachine, StorageProgress,
};
use std::vec::Vec;

/// Per-group storage a [`MultiStreamCoordinator`] uses to drive each group's endpoint when inbound
/// bytes span multiple groups. The caller implements it over its own per-group store table.
pub trait GroupStores<G, L, S> {
  /// The `(log, stable)` stores for `group`, or `None` if this host has no storage for it — an
  /// inbound message for an unknown group is then dropped (the sender retries on its own cadence).
  fn stores(&mut self, group: &G) -> Option<(&mut L, &mut S)>;
}

/// A multi-group consensus node speaking over framed reliable connections (`R` is the record layer,
/// e.g. `Labeled<Passthrough>` for TCP or `Labeled<TlsRecords>` for TLS).
pub struct MultiStreamCoordinator<G, I, F, R>
where
  G: GroupId,
  F: StateMachine,
{
  multi: MultiRaft<G, I, F>,
  router: PeerRouter<I, R>,
  next_conn_id: u64,
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
    }
  }

  /// Create a fresh group (see [`MultiRaft::create_group`]).
  ///
  /// # Errors
  /// [`GroupExists`] if the group id is already hosted.
  pub fn create_group(
    &mut self,
    gid: G,
    config: Config<I>,
    now: impl Into<Now>,
    seed: u64,
    fsm: F,
  ) -> Result<(), GroupExists> {
    self.multi.create_group(gid, config, now, seed, fsm)
  }

  /// Recover a group from durable storage (see [`MultiRaft::restore_group`]).
  ///
  /// # Errors
  /// [`GroupExists`] if the group id is already hosted.
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
  ) -> Result<(), GroupExists>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
    I: Data,
  {
    self
      .multi
      .restore_group(gid, config, now, seed, fsm, boot_epoch, log, stable)
  }

  /// Remove a group, returning its endpoint if present.
  pub fn remove_group(&mut self, gid: &G) -> Option<Endpoint<I, F>> {
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
  /// whose group tag fails to decode, or whose group has no store, is dropped (the sender retries).
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
    for (group_bytes, from, msg) in decoded {
      let Ok(group) = G::decode_exact(group_bytes) else {
        continue; // a malformed group tag is a peer fault; drop the frame
      };
      if let Some((log, stable)) = stores.stores(&group) {
        self
          .multi
          .handle_message(&group, now, log, stable, from, msg);
      }
    }
    self.flush();
  }

  /// Propose a command on `group`'s leader, replicating immediately. `None` if no such group.
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
    self.multi.flush_appends(group, now, log, stable);
    self.flush();
    Some(r)
  }

  /// Fire `group`'s timers (and the transport's handshake reaping), then flush. `None` if no such
  /// group.
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

  /// Drain queued outbound wire bytes as `(conn, bytes)` pairs for the driver to write.
  pub fn poll_transmit(&mut self) -> Vec<(ConnId, Vec<u8>)> {
    self.router.poll_transmit()
  }

  /// The earliest timer deadline across all groups.
  #[must_use]
  pub fn poll_timeout(&self) -> Option<Instant> {
    self.multi.poll_timeout()
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

  /// Route every group's queued outbound messages to peer connections, stamping each frame with its
  /// group tag.
  fn flush(&mut self) {
    let mut group_bytes = Vec::new();
    while let Some((group, out)) = self.multi.poll_message() {
      let (to, msg) = out.into_parts();
      group_bytes.clear();
      group.encode(&mut group_bytes);
      self.router.route(&group_bytes, to, &msg);
    }
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
mod tests {
  use super::*;
  use crate::{
    Config,
    testkit::{AsyncStable, CountSm, VecLog},
    transport::{Labeled, Passthrough},
  };
  use core::time::Duration;
  use std::collections::BTreeMap;

  type TestRecord = Labeled<Passthrough>;

  struct Stores {
    map: BTreeMap<u64, (VecLog, AsyncStable)>,
  }

  impl GroupStores<u64, VecLog, AsyncStable> for Stores {
    fn stores(&mut self, group: &u64) -> Option<(&mut VecLog, &mut AsyncStable)> {
      self.map.get_mut(group).map(|(l, s)| (l, s))
    }
  }

  fn single_voter(id: u64) -> Config<u64> {
    Config::try_new(
      id,
      std::vec![id],
      Duration::from_millis(1000),
      Duration::from_millis(100),
    )
    .unwrap()
  }

  #[test]
  fn coordinator_drives_isolated_groups() {
    let mut coord = MultiStreamCoordinator::<u64, u64, CountSm, TestRecord>::new();
    coord
      .create_group(100, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();
    coord
      .create_group(200, single_voter(1), Instant::ORIGIN, 1, CountSm::default())
      .unwrap();

    let mut stores = Stores {
      map: BTreeMap::new(),
    };
    stores
      .map
      .insert(100, (VecLog::default(), AsyncStable::default()));
    stores
      .map
      .insert(200, (VecLog::default(), AsyncStable::default()));

    // Drive group 100 to leadership through the coordinator's per-group storage. Group 200 is never
    // touched. (Single-voter groups need no peer connection, so this exercises the coordinator's
    // group threading + store routing without the wire.)
    let d = coord.group(&100).unwrap().poll_timeout().unwrap();
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_timeout(&100, d, l, s); // campaign
    }
    for _ in 0..2 {
      // First drain: the self-vote becomes durable and the group becomes leader (appending a no-op);
      // second drain: the no-op append completes, so quorum=1 commits and applies it.
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_storage(&100, d, l, s);
    }
    assert!(coord.group(&100).unwrap().role().is_leader());
    assert!(coord.group(&200).unwrap().role().is_follower());

    // Propose a command on group 100 and let quorum=1 commit + apply it.
    let cmd = bytes::Bytes::copy_from_slice(&[7u8]);
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.submit_propose(&100, d, l, s, &cmd).unwrap().unwrap();
    }
    {
      let (l, s) = stores.stores(&100).unwrap();
      coord.handle_storage(&100, d, l, s);
    }
    while let Some((g, _)) = coord.poll_event() {
      assert_eq!(g, 100, "events are stamped with the originating group");
    }
    // Group 100 applied the command; group 200 is pristine.
    assert!(coord.group(&100).unwrap().state_machine().count() >= 1);
    assert_eq!(coord.group(&200).unwrap().state_machine().count(), 0);
  }
}
