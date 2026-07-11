//! `PeerRouter<I, R>`: the per-peer connection table. It owns every live `Conn`, binds each to its
//! peer once validated, routes an outbound `Message` to the right connection, and reports every
//! connection the transport closes on its own initiative so the driver can release the socket.
use super::{CoalescedEntry, ConnId, TransportError, conn::Conn, stream::RecordIo};
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

/// How long a VALIDATED connection may go without receiving ANY bytes before it is reaped as a
/// blackholed peer (socket alive, its bytes silently dropped). This is the peer-silence detector a
/// quiesced plane needs: a plane that sends nothing produces no close on its own, so without a
/// transport-level idle reap a silent blackhole would never wake the quiesced group. Refreshed by
/// any received bytes; a connection's own sends never refresh it. QUIC-parity with the quinn
/// max-idle default — tuning it rides the transport-hardening milestone.
const IDLE_TIMEOUT_MILLIS: u64 = 3000;
const IDLE_TIMEOUT: Duration = Duration::from_millis(IDLE_TIMEOUT_MILLIS);

/// How long a validated connection may go without SENDING before it emits an empty no-op probe, so
/// an idle-but-healthy peer keeps receiving bytes and never trips its own [`IDLE_TIMEOUT`]. QUIC-parity
/// with the quinn keep-alive default (idle/3): three probe windows fit inside one idle window, so a
/// single lost probe cannot by itself cause a spurious reap.
const PROBE_INTERVAL: Duration = Duration::from_millis(IDLE_TIMEOUT_MILLIS / 3);

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
  /// Per-VALIDATED-connection idle-reap deadline: last-received-bytes instant + [`IDLE_TIMEOUT`].
  /// Refreshed by any received bytes; a connection's own sends never touch it. A connection past
  /// this is a silent (blackholed) peer and is reaped by [`service_liveness`](Self::service_liveness).
  idle_deadline: BTreeMap<ConnId, Instant>,
  /// Per-VALIDATED-connection keep-alive-probe deadline: last-probe/validation instant +
  /// [`PROBE_INTERVAL`]. At this deadline an otherwise-idle connection emits an empty probe so the
  /// peer keeps receiving bytes and never reaps it.
  probe_deadline: BTreeMap<ConnId, Instant>,
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
      idle_deadline: BTreeMap::new(),
      probe_deadline: BTreeMap::new(),
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
    self.idle_deadline.remove(&id);
    self.probe_deadline.remove(&id);
    self.peer_of.retain(|_, &mut c| c != id);
  }

  /// Router-initiated removal: drop the connection AND queue the close notification.
  fn remove_internal(&mut self, id: ConnId, reason: Option<TransportError>) {
    self.remove(id);
    self.closed.push_back((id, reason));
  }

  /// Close `id` on the OWNER's initiative — for an integrity fault detected above the router (a
  /// group tag that does not decode, a deployment-shape mismatch). Queues the close notification
  /// like any router-detected fault; a no-op if the connection is already gone, so a fault found
  /// while draining a connection's final frames does not double-notify.
  pub fn close(&mut self, id: ConnId, reason: Option<TransportError>) {
    if self.conns.contains_key(&id) {
      self.remove_internal(id, reason);
    }
  }

  /// The next connection the router closed on its own initiative, with the fault (if any). The
  /// driver must close the underlying socket; for a dialed peer it may redial (the redial gets a
  /// fresh, higher `ConnId`).
  pub fn poll_conn_closed(&mut self) -> Option<(ConnId, Option<TransportError>)> {
    self.closed.pop_front()
  }

  /// The earliest pending handshake deadline, if any registered connection is still un-validated.
  /// Both the single- and multi-group coordinators fold this into their `poll_timeout` so the driver
  /// wakes to reap even when the endpoint surfaces no consensus deadline (a timerless observer, or —
  /// for multi — zero hosted groups, every group poisoned, or a host of non-voter learners).
  pub fn next_handshake_deadline(&self) -> Option<Instant> {
    self.handshake_deadline.values().min().copied()
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

  /// The earliest liveness deadline across VALIDATED connections — the minimum of every pending
  /// idle-reap and keep-alive-probe deadline. The multi-group coordinator folds this into its
  /// `transport_timeout` so a quiesced driver wakes to probe idle peers and reap silent (blackholed)
  /// ones. The single-group coordinator does NOT fold it (its groups never quiesce; election timers
  /// detect silence), so its validated connections' deadlines sit unserviced until removal.
  pub fn next_liveness_deadline(&self) -> Option<Instant> {
    self
      .idle_deadline
      .values()
      .chain(self.probe_deadline.values())
      .min()
      .copied()
  }

  /// Service validated-connection liveness at `now`. Reap any connection whose peer has been silent
  /// past `IDLE_TIMEOUT` — closed as [`TransportError::IdleTimeout`] so the driver releases the
  /// socket and, via its conn-loss wake, elections proceed. Then emit an empty keep-alive probe on
  /// any connection idle past `PROBE_INTERVAL` so a healthy idle peer keeps receiving bytes.
  /// Silence outranks a probe: a connection past BOTH deadlines is reaped, never probed.
  pub fn service_liveness(&mut self, now: Instant) {
    let reap: Vec<ConnId> = self
      .idle_deadline
      .iter()
      .filter(|&(_, &deadline)| deadline <= now)
      .map(|(&id, _)| id)
      .collect();
    for id in &reap {
      self.remove_internal(*id, Some(TransportError::IdleTimeout));
    }
    let due: Vec<ConnId> = self
      .probe_deadline
      .iter()
      .filter(|&(id, &deadline)| deadline <= now && !reap.contains(id))
      .map(|(&id, _)| id)
      .collect();
    for id in due {
      let Some(conn) = self.conns.get_mut(&id) else {
        continue;
      };
      conn.send_probe();
      if conn.is_closed() {
        // The probe tripped the outbound cap (the peer stopped draining): drop the route like any
        // send-close, so no later message is queued into a dead connection.
        self.remove_internal(id, None);
      } else {
        self.probe_deadline.insert(id, now + PROBE_INTERVAL);
      }
    }
  }

  /// Feed inbound bytes to connection `id`, decode any complete messages, and bind the peer on
  /// validation. Returns the decoded `(group, entry_flags, peer, message)` tuples (flags are `0`
  /// for a single-message frame; a coalesced frame expands to one tuple per entry). A connection
  /// that faults or reaches a clean close is removed and reported via
  /// [`poll_conn_closed`](Self::poll_conn_closed) — after its final decoded frames (clean close
  /// only) have been delivered.
  pub fn handle_conn_data(
    &mut self,
    id: ConnId,
    bytes: &[u8],
    eof: bool,
    now: Instant,
    out: &mut Vec<(bytes::Bytes, u8, I, Message<I>)>,
  ) -> Result<(), TransportError> {
    let result = self.handle_conn_data_inner(id, bytes, eof, now, out);
    // A connection that errored OR reached EOF/Closed must drop its peer binding — otherwise the
    // next `route` to that peer would send into a dead connection and silently drop the message.
    match &result {
      Err(e) => self.remove_internal(id, Some(e.clone())),
      Ok(()) => {
        if self.conns.get(&id).is_some_and(|c| c.is_closed()) {
          self.remove_internal(id, None);
        } else if !bytes.is_empty() && self.conns.get(&id).is_some_and(|c| c.peer().is_some()) {
          // A validated, live connection received bytes: any inbound (a probe or real traffic)
          // proves the peer is alive, so its silence deadline restarts. A connection's own sends
          // never refresh it — only proof the PEER is still there does.
          self.idle_deadline.insert(id, now + IDLE_TIMEOUT);
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
    out: &mut Vec<(bytes::Bytes, u8, I, Message<I>)>,
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
    if !conn.is_closed()
      && let Some(peer) = conn.peer()
    {
      // The handshake→validated transition (the FIRST read that binds a peer) arms the keep-alive
      // probe; the idle-silence deadline is armed and refreshed by `handle_conn_data` on every
      // received read, this one included.
      let first_validation = self.handshake_deadline.remove(&id).is_some();
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
      if first_validation {
        self.probe_deadline.insert(id, now + PROBE_INTERVAL);
      }
    }
    let conn = self.conns.get_mut(&id).expect("conn present");
    let mut msgs = Vec::new();
    conn.poll_decoded(&mut msgs)?;
    let peer = conn.peer();
    for (group, flags, msg) in msgs {
      if let Some(p) = &peer {
        out.push((group, flags, p.cheap_clone(), msg));
      }
    }
    Ok(())
  }

  /// Encode `msg` once and queue it to `to`'s connection. Returns `false` if no validated connection
  /// to `to` exists (the message is dropped; the consensus layer will retry on its own cadence).
  /// A send that closes the connection (the outbound cap tripped — the peer stopped draining) drops
  /// the route immediately and reports the close, so no later message is silently queued into a
  /// dead connection.
  pub fn route(&mut self, group: &[u8], to: I, msg: &Message<I>) -> bool {
    let Some(&id) = self.peer_of.get(&to) else {
      return false;
    };
    let Some(conn) = self.conns.get_mut(&id) else {
      return false;
    };
    // `group` is the group-demux tag stamped onto the frame: an empty slice for a single-group host,
    // the encoded `GroupId` for a multi-group coordinator.
    conn.send_message(group, msg);
    if conn.is_closed() {
      self.remove_internal(id, None);
      return false;
    }
    true
  }

  /// Encode a batch of `(flags, encoded_group, message)` entries as coalesced control frames and
  /// queue them to `to`'s connection — [`route`](Self::route)'s batch counterpart, with the same
  /// no-route drop and the same drop-the-route-on-close discipline (the cap/oversize checks run in
  /// [`Conn::send_coalesced`] against each whole coalesced payload).
  pub fn route_coalesced(&mut self, to: I, entries: &[CoalescedEntry<I>]) -> bool {
    let Some(&id) = self.peer_of.get(&to) else {
      return false;
    };
    let Some(conn) = self.conns.get_mut(&id) else {
      return false;
    };
    conn.send_coalesced(entries);
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
