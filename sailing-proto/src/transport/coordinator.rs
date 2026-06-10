//! `StreamCoordinator<I, F, R>`: the stream-transport "super state machine".
//!
//! It composes the pure consensus [`Endpoint`] with a [`PeerRouter`]: inbound connection bytes
//! become decoded `Message`s fed to the endpoint, and the endpoint's outbound messages are routed
//! back to peer connections. The driver supplies sockets, timers, and storage; this type is fully
//! deterministic and Sans-I/O.
use super::{ConnId, router::PeerRouter, stream::RecordIo};
use crate::{
  Config, Endpoint, Event, Index, Instant, LogStore, NodeId, ProposeError, StableStore,
  StateMachine, TransferError,
};
use bytes::Bytes;
use std::vec::Vec;

/// A consensus node speaking over framed reliable connections (`R` is the record layer, e.g.
/// `Labeled<Passthrough>` for TCP or `Labeled<TlsRecords>` for TLS).
pub struct StreamCoordinator<I, F, R>
where
  F: StateMachine,
{
  endpoint: Endpoint<I, F>,
  router: PeerRouter<I, R>,
}

impl<I, F, R> StreamCoordinator<I, F, R>
where
  I: NodeId,
  F: StateMachine,
  F::Command: crate::Data,
  F::Snapshot: crate::Data,
  F::Error: core::error::Error,
  R: RecordIo,
{
  /// Create a coordinator wrapping a fresh [`Endpoint`] and an empty connection table.
  pub fn new(config: Config<I>, now: Instant, seed: u64, fsm: F) -> Self {
    Self {
      endpoint: Endpoint::new(config, now, seed, fsm),
      router: PeerRouter::new(),
    }
  }

  /// Register a freshly opened connection (the driver dialed or accepted a socket).
  pub fn on_conn_open(&mut self, conn: ConnId, record: R) {
    self.router.register(conn, record);
  }

  /// Tear down a closed connection.
  pub fn on_conn_close(&mut self, conn: ConnId) {
    self.router.remove(conn);
  }

  /// Feed inbound bytes from connection `conn`: decode any complete messages into the endpoint, then
  /// flush the endpoint's resulting outbound messages back to the peer connections. A transport
  /// fault closes the connection (it is removed); it never poisons the endpoint.
  pub fn handle_conn_data<L, S>(
    &mut self,
    conn: ConnId,
    bytes: &[u8],
    eof: bool,
    now: Instant,
    log: &mut L,
    stable: &mut S,
  ) where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let mut decoded = Vec::new();
    match self
      .router
      .handle_conn_data(conn, bytes, eof, now, &mut decoded)
    {
      Ok(()) => {}
      Err(_) => self.router.remove(conn),
    }
    for (from, msg) in decoded {
      self.endpoint.handle_message(now, log, stable, from, msg);
    }
    self.flush();
  }

  /// Propose a client command on this node (must be the leader).
  pub fn submit_propose<L, S>(
    &mut self,
    now: Instant,
    log: &mut L,
    stable: &S,
    cmd: &F::Command,
  ) -> Result<Index, ProposeError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let r = self.endpoint.propose(now, log, stable, cmd);
    self.flush();
    r
  }

  /// Initiate a linearizable read; the resulting `ReadState` surfaces via [`poll_event`].
  pub fn read_index<L, S>(
    &mut self,
    now: Instant,
    log: &L,
    stable: &S,
    context: Bytes,
  ) -> Result<(), crate::ReadIndexError>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let r = self.endpoint.read_index(now, log, stable, context);
    self.flush();
    r
  }

  /// Begin transferring leadership to `to`.
  pub fn transfer_leader<L, S>(
    &mut self,
    now: Instant,
    log: &L,
    stable: &S,
    to: I,
  ) -> Result<(), TransferError<I>>
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    let r = self.endpoint.transfer_leader(now, log, stable, to);
    self.flush();
    r
  }

  /// Fire the endpoint's timers, then flush any resulting outbound messages.
  pub fn handle_timeout<L, S>(&mut self, now: Instant, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.endpoint.handle_timeout(now, log, stable);
    self.flush();
  }

  /// Drain storage completions into the endpoint, then flush.
  pub fn handle_storage<L, S>(&mut self, now: Instant, log: &mut L, stable: &mut S)
  where
    L: LogStore,
    S: StableStore<NodeId = I>,
  {
    self.endpoint.handle_storage(now, log, stable);
    self.flush();
  }

  /// Drain queued outbound wire bytes as `(conn, bytes)` pairs for the driver to write.
  pub fn poll_transmit(&mut self) -> Vec<(ConnId, Vec<u8>)> {
    self.router.poll_transmit()
  }

  /// The endpoint's next timer deadline.
  pub fn poll_timeout(&self) -> Option<Instant> {
    self.endpoint.poll_timeout()
  }

  /// Drain the next application event (committed entry, read-state, …).
  pub fn poll_event(&mut self) -> Option<Event<I, F::Response>> {
    self.endpoint.poll_event()
  }

  /// Route every queued outbound message to its peer's connection (encode-once per message).
  fn flush(&mut self) {
    while let Some(out) = self.endpoint.poll_message() {
      let (to, msg) = out.into_parts();
      self.router.route(to, &msg);
    }
  }

  /// The bound connection for `peer`, if any (test/inspection helper).
  pub fn conn_of(&self, peer: &I) -> Option<ConnId> {
    self.router.conn_of(peer)
  }

  /// This node's current consensus role.
  pub const fn role(&self) -> crate::Role {
    self.endpoint.role()
  }

  /// Read-only access to the application state machine.
  pub const fn state_machine(&self) -> &F {
    self.endpoint.state_machine()
  }
}

#[cfg(test)]
mod tests;
