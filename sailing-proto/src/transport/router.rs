//! `PeerRouter<I, R>`: the per-peer connection table. It owns every live `Conn`, binds each to its
//! peer once validated, and routes an outbound `Message` to the right connection.
use super::{ConnId, TransportError, conn::Conn, stream::RecordIo};
use crate::{Instant, Message, NodeId};
use std::{collections::BTreeMap, vec::Vec};

/// Routes consensus messages over a table of per-peer connections.
///
/// A connection is registered by [`ConnId`] while still handshaking; once it validates, the router
/// binds `peer → conn`. If a second connection validates for an already-bound peer, the newer one
/// wins (a redial after a half-open connection) and the older is dropped — a deterministic
/// tie-break, since both connections carry the same authenticated peer.
pub struct PeerRouter<I, R> {
  conns: BTreeMap<ConnId, Conn<I, R>>,
  peer_of: BTreeMap<I, ConnId>,
}

impl<I: NodeId, R: RecordIo> PeerRouter<I, R> {
  /// An empty router.
  pub fn new() -> Self {
    Self {
      conns: BTreeMap::new(),
      peer_of: BTreeMap::new(),
    }
  }

  /// Register a freshly opened connection (still handshaking) under `id`.
  pub fn register(&mut self, id: ConnId, record: R) {
    self.conns.insert(id, Conn::new(record));
  }

  /// Remove a connection and clear any peer binding it held.
  pub fn remove(&mut self, id: ConnId) {
    self.conns.remove(&id);
    self.peer_of.retain(|_, &mut c| c != id);
  }

  /// Feed inbound bytes to connection `id`, decode any complete messages, and bind the peer on
  /// validation. Returns the decoded `(peer, message)` pairs. A transport fault closes the
  /// connection (reported via [`TransportError`]); the caller then calls [`remove`](Self::remove).
  pub fn handle_conn_data(
    &mut self,
    id: ConnId,
    bytes: &[u8],
    eof: bool,
    now: Instant,
    out: &mut Vec<(I, Message<I>)>,
  ) -> Result<(), TransportError> {
    let result = self.handle_conn_data_inner(id, bytes, eof, now, out);
    // A connection that errored OR reached EOF/Closed must drop its peer binding — otherwise the
    // next `route` to that peer would send into a dead connection and silently drop the message.
    if result.is_err() || self.conns.get(&id).is_some_and(|c| c.is_closed()) {
      self.remove(id);
    }
    result
  }

  fn handle_conn_data_inner(
    &mut self,
    id: ConnId,
    bytes: &[u8],
    eof: bool,
    now: Instant,
    out: &mut Vec<(I, Message<I>)>,
  ) -> Result<(), TransportError> {
    let conn = match self.conns.get_mut(&id) {
      Some(c) => c,
      None => return Ok(()),
    };
    conn.handle_data(bytes, eof, now)?;
    // Bind (or rebind) the peer the moment this connection validates. `ConnId`s are
    // driver-assigned and monotonically increasing, so "newer connection wins" is exactly
    // "higher id wins": a NEWER duplicate (a redial) evicts the older binding, while an OLDER
    // duplicate that validates late is itself dropped — it must never evict the healthy
    // replacement.
    if let Some(peer) = conn.peer() {
      if let Some(&prev) = self.peer_of.get(&peer) {
        if prev > id {
          self.conns.remove(&id); // stale older duplicate: drop it, keep the newer binding
          return Ok(());
        }
        if prev != id {
          self.conns.remove(&prev); // newer connection wins
        }
      }
      self.peer_of.insert(peer, id);
    }
    let conn = self.conns.get_mut(&id).expect("conn present");
    let mut msgs = Vec::new();
    conn.poll_decoded(&mut msgs)?;
    let peer = conn.peer();
    for m in msgs {
      if let Some(p) = peer {
        out.push((p, m));
      }
    }
    Ok(())
  }

  /// Encode `msg` once and queue it to `to`'s connection. Returns `false` if no validated connection
  /// to `to` exists (the message is dropped; the consensus layer will retry on its own cadence).
  /// A send that closes the connection (the outbound cap tripped — the peer stopped draining) drops
  /// the route immediately, so no later message is silently queued into a dead connection.
  pub fn route(&mut self, to: I, msg: &Message<I>) -> bool {
    let Some(&id) = self.peer_of.get(&to) else {
      return false;
    };
    let Some(conn) = self.conns.get_mut(&id) else {
      return false;
    };
    conn.send_message(msg);
    if conn.is_closed() {
      self.remove(id);
      return false;
    }
    true
  }

  /// Drain queued outbound wire bytes for every connection, as `(conn, bytes)` pairs.
  pub fn poll_transmit(&mut self) -> Vec<(ConnId, Vec<u8>)> {
    let mut out = Vec::new();
    for (&id, conn) in self.conns.iter_mut() {
      let mut bytes = Vec::new();
      if conn.poll_transmit(&mut bytes) > 0 {
        out.push((id, bytes));
      }
    }
    out
  }

  /// The connection id currently bound to `peer`, if any (test/inspection helper).
  pub fn conn_of(&self, peer: &I) -> Option<ConnId> {
    self.peer_of.get(peer).copied()
  }
}

impl<I: NodeId, R: RecordIo> Default for PeerRouter<I, R> {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
