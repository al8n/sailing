//! `PeerRouter<I, R>`: the per-peer connection table. It owns every live `Conn`, binds each to its
//! peer once validated, routes an outbound `Message` to the right connection, and reports every
//! connection the transport closes on its own initiative so the driver can release the socket.
use super::{ConnId, TransportError, conn::Conn, stream::RecordIo};
use crate::{Instant, Message, NodeId};
use core::time::Duration;
use std::{
  collections::{BTreeMap, VecDeque},
  vec::Vec,
};

/// How long a registered connection may sit un-validated (handshake incomplete) before it is
/// reaped. Without a deadline, a peer that connects and never completes the hello (or a dialed
/// socket whose peer never answers) would hold its `Conn` — and the driver's socket — forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Routes consensus messages over a table of per-peer connections.
///
/// A connection is registered by [`ConnId`] while still handshaking; once it validates, the router
/// binds `peer → conn`. If a second connection validates for an already-bound peer, the HIGHER id
/// (the newer dial — ids are driver-monotonic) wins and the other is dropped — a deterministic
/// tie-break, since both connections carry the same authenticated peer.
///
/// Every connection the router drops on its OWN initiative (transport fault, clean close,
/// duplicate tie-break, outbound-cap stall, handshake timeout) is queued and surfaced via
/// [`poll_conn_closed`](Self::poll_conn_closed) so the driver can close the socket and, for a
/// dialed peer, redial. Driver-initiated removals ([`remove`](Self::remove)) are not echoed back.
pub struct PeerRouter<I, R> {
  conns: BTreeMap<ConnId, Conn<I, R>>,
  peer_of: BTreeMap<I, ConnId>,
  /// Handshake deadline per not-yet-validated connection (registration time + the timeout).
  handshake_deadline: BTreeMap<ConnId, Instant>,
  /// Connections the router closed on its own initiative, with the fault that closed them
  /// (`None` = a clean close: peer EOF/close_notify, duplicate eviction, outbound-cap stall).
  closed: VecDeque<(ConnId, Option<TransportError>)>,
}

impl<I: NodeId, R: RecordIo> PeerRouter<I, R> {
  /// An empty router.
  pub fn new() -> Self {
    Self {
      conns: BTreeMap::new(),
      peer_of: BTreeMap::new(),
      handshake_deadline: BTreeMap::new(),
      closed: VecDeque::new(),
    }
  }

  /// Register a freshly opened connection (still handshaking) under `id`, starting its handshake
  /// deadline. Re-registering a LIVE id is a driver contract violation (ids are unique and
  /// monotonic): the registration is REJECTED — the existing connection stays untouched and the
  /// rejected attempt is reported via [`poll_conn_closed`](Self::poll_conn_closed) so the driver
  /// tears down whatever socket it tried to register. (Accepting the replacement would be
  /// ambiguous: a later close notification for the id could not say WHICH socket to release.)
  pub fn register(&mut self, id: ConnId, record: R, now: Instant) {
    if self.conns.contains_key(&id) {
      self
        .closed
        .push_back((id, Some(TransportError::DuplicateConnId)));
      return;
    }
    self.conns.insert(id, Conn::new(record));
    self.handshake_deadline.insert(id, now + HANDSHAKE_TIMEOUT);
  }

  /// Driver-initiated removal (the driver already knows the socket is gone — not echoed back).
  pub fn remove(&mut self, id: ConnId) {
    self.conns.remove(&id);
    self.handshake_deadline.remove(&id);
    self.peer_of.retain(|_, &mut c| c != id);
  }

  /// Router-initiated removal: drop the connection AND queue the close notification.
  fn remove_internal(&mut self, id: ConnId, reason: Option<TransportError>) {
    self.remove(id);
    self.closed.push_back((id, reason));
  }

  /// The next connection the router closed on its own initiative, with the fault (if any). The
  /// driver must close the underlying socket; for a dialed peer it may redial (the redial gets a
  /// fresh, higher `ConnId`).
  pub fn poll_conn_closed(&mut self) -> Option<(ConnId, Option<TransportError>)> {
    self.closed.pop_front()
  }

  /// Reap connections whose handshake deadline has passed without validating. Closes each as
  /// [`TransportError::NotValidated`] so the driver releases the socket.
  pub fn reap_handshakes(&mut self, now: Instant) {
    let expired: Vec<ConnId> = self
      .handshake_deadline
      .iter()
      .filter(|&(_, &deadline)| deadline <= now)
      .map(|(&id, _)| id)
      .collect();
    for id in expired {
      self.remove_internal(id, Some(TransportError::NotValidated));
    }
  }

  /// Feed inbound bytes to connection `id`, decode any complete messages, and bind the peer on
  /// validation. Returns the decoded `(peer, message)` pairs. A connection that faults or reaches a
  /// clean close is removed and reported via [`poll_conn_closed`](Self::poll_conn_closed) — after
  /// its final decoded frames (clean close only) have been delivered.
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
    match &result {
      Err(e) => self.remove_internal(id, Some(e.clone())),
      Ok(()) => {
        if self.conns.get(&id).is_some_and(|c| c.is_closed()) {
          self.remove_internal(id, None);
        }
      }
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
    // replacement. Only a LIVE connection binds: one that validated and clean-closed in the same
    // read still delivers its final frames below (attributed via `conn.peer()`), but must not
    // claim the route or evict a healthy binding on its way out.
    if !conn.is_closed() {
      if let Some(peer) = conn.peer() {
        self.handshake_deadline.remove(&id);
        if let Some(&prev) = self.peer_of.get(&peer) {
          if prev > id {
            // A stale older duplicate validated late: drop it, keep the newer binding.
            self.remove_internal(id, None);
            return Ok(());
          }
          if prev != id {
            self.remove_internal(prev, None); // newer connection wins
          }
        }
        self.peer_of.insert(peer, id);
      }
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
  /// the route immediately and reports the close, so no later message is silently queued into a
  /// dead connection.
  pub fn route(&mut self, to: I, msg: &Message<I>) -> bool {
    let Some(&id) = self.peer_of.get(&to) else {
      return false;
    };
    let Some(conn) = self.conns.get_mut(&id) else {
      return false;
    };
    conn.send_message(msg);
    if conn.is_closed() {
      self.remove_internal(id, None);
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
