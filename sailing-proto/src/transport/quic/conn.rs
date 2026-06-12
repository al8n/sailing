//! Per-connection state and the connection table for the QUIC transport.

use std::{
  collections::{BTreeMap, VecDeque},
  time::Instant,
  vec::Vec,
};

use quinn_proto::{Connection, ConnectionHandle, StreamId};

use super::super::frame::FrameDecoder;
use crate::NodeId;

/// Per-connection lifecycle phase. Consensus-stream I/O is unreachable until [`Phase::Validated`].
///
/// Transitions: `Handshaking → Authenticating → Validated → Closed` (or to `Closed` from any
/// earlier phase on failure). The QUIC handshake completing only carries a connection to
/// `Authenticating`: the identity-binding step (the coordinator's
/// [`IdentitySource`](super::IdentitySource) `authenticate` + binding policy) is what promotes it
/// to `Validated`, after which consensus frames flow. The control preface is written in
/// `Authenticating`; consensus frames are gated until `Validated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
  /// TLS handshake in progress; no streams, no data exchanged yet.
  Handshaking,
  /// QUIC handshake complete, but peer identity not yet bound. This side's preface is sent here
  /// and the peer's first frame is routed to `authenticate`; no consensus frame flows.
  Authenticating,
  /// Identity bound; the per-peer bidi stream carries consensus messages.
  Validated,
  /// Connection is being torn down; no further I/O.
  Closed,
}

impl Phase {
  #[inline(always)]
  pub(crate) const fn is_handshaking(self) -> bool {
    matches!(self, Self::Handshaking)
  }

  #[inline(always)]
  pub(crate) const fn is_authenticating(self) -> bool {
    matches!(self, Self::Authenticating)
  }

  #[inline(always)]
  pub(crate) const fn is_validated(self) -> bool {
    matches!(self, Self::Validated)
  }

  #[inline(always)]
  pub(crate) const fn is_closed(self) -> bool {
    matches!(self, Self::Closed)
  }
}

/// One pooled QUIC connection: the quinn state, its lifecycle phase, the bound peer (once known),
/// and the single consensus bidi stream's half-stream state.
///
/// **Half-streams.** quinn-proto's `streams().open(Dir::Bi)` mints a stream id owned by THIS side,
/// while `streams().accept(Dir::Bi)` adopts the id the PEER opened — distinct ids (different
/// initiator bit). Each side WRITES the stream it opened and READS the stream the peer opened;
/// conflating them means each side reads its own write half and never sees the peer's frames. The
/// transport uses ONE consensus stream per side per connection (single-stream v1), so a second
/// peer-opened stream is a protocol violation the bridge closes on.
///
/// All fields are `pub(crate)` — an internal plumbing struct whose sole mutator is the bridge.
pub(crate) struct ConnEntry<I> {
  pub(crate) conn: Connection,
  pub(crate) phase: Phase,
  /// The peer this connection was DIALED to reach (`Some` on the connect path, `None` on accept).
  /// The coordinator's binding policy requires the authenticated candidate to equal this on a
  /// dialed connection (match-or-abort); an accepted connection adopts whatever candidate
  /// authenticates.
  pub(crate) dialed_expectation: Option<I>,
  /// The authenticated, coordinator-bound peer. `None` until the identity-binding step promotes
  /// the connection to [`Phase::Validated`]; routing and frame surfacing key off this being
  /// `Some`.
  pub(crate) peer: Option<I>,
  /// Whether this side's preface frame has been staged. The preface is the FIRST frame on the
  /// send stream; consensus frames are gated behind it (and behind `Validated`).
  pub(crate) preface_done: bool,
  /// The bidi stream THIS side opened (`streams().open(Dir::Bi)`); this side writes it. `None`
  /// until the preface step opens it on `Connected`.
  pub(crate) send: Option<StreamId>,
  /// The bidi stream the PEER opened (`streams().accept(Dir::Bi)`); this side reads it. `None`
  /// until the first read adopts it.
  pub(crate) recv: Option<StreamId>,
  /// The inbound frame decoder for the peer's stream.
  pub(crate) decoder: FrameDecoder,
  /// Strict-FIFO staging buffer for framed bytes the send stream could not accept yet (no stream
  /// open, or quinn reported `Blocked`). Drained from the FRONT so on-wire frame order is the
  /// order frames were written.
  pub(crate) outbound: VecDeque<u8>,
  /// Total bytes fed to the decoder while [`Phase::Authenticating`] — the pre-authentication
  /// intake position. The only legitimate pre-validation frame is the peer's identity preface
  /// (at most one hello), so the bridge reads EXACTLY the first frame before the connection
  /// validates: its 4-byte length prefix first, then precisely the declared remainder. This
  /// counts how far into that frame prior passes have read.
  pub(crate) preauth_fed: usize,
  /// The first frame's length-prefix bytes seen so far (valid up to `preauth_fed.min(4)`): the
  /// pre-authentication read is steered by the frame's own declared length, and a hello can
  /// trickle in chunks smaller than the header, so the seen prefix persists across passes.
  pub(crate) preauth_hdr: [u8; 4],
  /// The peer GRACEFULLY finished its send half (a consumed FIN). The bytes read before the FIN
  /// are complete frames the coordinator must DELIVER first; the connection is closed only after
  /// that drain (deliver-before-close).
  pub(crate) fin_received: bool,
  /// Monotonic creation sequence, assigned by [`ConnTable::insert`] from a strictly-increasing
  /// per-table counter — a RECENCY order over the table's connections (a higher `seq` was created
  /// later). The per-peer connection bound uses it to reap the OLDEST same-peer connections:
  /// under mutual dial a peer pair legitimately holds TWO connections (each side dials the other
  /// and both are kept), and a reconnecting peer briefly a third, so a flapping valid-cert member
  /// could otherwise accumulate unbounded same-peer connections and exhaust the global cap.
  /// Unrelated to quinn's `ConnectionHandle` (a slab index that may be reused after a drain);
  /// `seq` is never reused.
  pub(crate) seq: u64,
  /// Deadline by which this connection must reach [`Phase::Validated`], stamped when it ENTERS
  /// `Authenticating` (the QUIC handshake completed) and CLEARED whenever it leaves — on
  /// `Validated` (bound) or `Closed` (reaped / lost). A peer that completed mTLS with a valid
  /// cluster cert but never sends a valid preface would otherwise sit in `Authenticating` forever
  /// (its keep-alive PINGs refresh quinn's idle timeout) and N such peers exhaust
  /// `max_connections`. The bridge closes any connection still `Authenticating` past this; it is
  /// folded into `poll_timeout` as a connection timer scoped to `Authenticating` entries.
  pub(crate) auth_deadline: Option<Instant>,
}

impl<I> ConnEntry<I> {
  /// Wraps a freshly-minted `quinn_proto::Connection` in a `Handshaking` entry.
  /// `dialed_expectation` is `Some(peer)` on the connect path and `None` on the accept path.
  pub(crate) fn new(conn: Connection, dialed_expectation: Option<I>) -> Self {
    Self {
      conn,
      phase: Phase::Handshaking,
      dialed_expectation,
      peer: None,
      preface_done: false,
      send: None,
      recv: None,
      decoder: FrameDecoder::new(),
      outbound: VecDeque::new(),
      preauth_fed: 0,
      preauth_hdr: [0; 4],
      fin_received: false,
      // Placeholder; the recency sequence is assigned by `ConnTable::insert` the moment the entry
      // enters the table (the single insertion choke-point).
      seq: 0,
      auth_deadline: None,
    }
  }

  /// True when identity has been bound and the bidi stream may carry consensus frames.
  #[inline(always)]
  pub(crate) fn is_validated(&self) -> bool {
    self.phase.is_validated()
  }

  /// True while the QUIC handshake is complete but identity is not yet bound (the preface /
  /// `authenticate` window).
  #[inline(always)]
  pub(crate) fn is_authenticating(&self) -> bool {
    self.phase.is_authenticating()
  }
}

/// The bridge's pool of live connections plus the per-peer outbound routing index.
///
/// `by_peer` points each bound peer at ONE handle — the most-recently-VALIDATED connection
/// (last-established-wins) — which is where outbound consensus frames route. Under mutual dial a
/// peer legitimately holds TWO live connections (each side dialed the other); both deliver the
/// peer's INBOUND frames (the bridge reads every validated connection), but outbound rides the
/// routed one. Closing the "displaced" sibling would break the steady-state mesh, so it is kept;
/// what IS bounded is the per-peer live COUNT (see the bridge's bind policy).
pub(crate) struct ConnTable<I> {
  entries: BTreeMap<ConnectionHandle, ConnEntry<I>>,
  by_peer: BTreeMap<I, ConnectionHandle>,
  /// The strictly-increasing creation counter behind [`ConnEntry::seq`].
  next_seq: u64,
}

impl<I: NodeId> ConnTable<I> {
  pub(crate) fn new() -> Self {
    Self {
      entries: BTreeMap::new(),
      by_peer: BTreeMap::new(),
      next_seq: 1,
    }
  }

  /// The number of live entries (dialed + accepted) — the connection-cap denominator.
  pub(crate) fn len(&self) -> usize {
    self.entries.len()
  }

  /// Insert a fresh entry under `h`, stamping its creation recency. The single insertion
  /// choke-point, so `seq` is always table-assigned and never reused.
  pub(crate) fn insert(&mut self, h: ConnectionHandle, mut entry: ConnEntry<I>) {
    entry.seq = self.next_seq;
    self.next_seq += 1;
    self.entries.insert(h, entry);
  }

  /// Mutable access to the entry under `h`, if it is still pooled.
  pub(crate) fn entry(&mut self, h: ConnectionHandle) -> Option<&mut ConnEntry<I>> {
    self.entries.get_mut(&h)
  }

  /// Every pooled handle, in deterministic (handle) order.
  pub(crate) fn handles(&self) -> Vec<ConnectionHandle> {
    self.entries.keys().copied().collect()
  }

  /// Drop the entry under `h` entirely (the terminal `Drained` reap), clearing any routing slot
  /// that still points at it.
  pub(crate) fn remove(&mut self, h: ConnectionHandle) {
    // Nested rather than a let-chain: the crate's MSRV (1.85) predates stabilized let-chains.
    if let Some(e) = self.entries.remove(&h)
      && let Some(p) = e.peer
      && self.by_peer.get(&p) == Some(&h)
    {
      self.by_peer.remove(&p);
    }
  }

  /// Clear `h`'s routing slot (its peer becomes unrouteable through `h`); the entry itself is
  /// KEPT so the service pump can drive the quinn connection to `Drained`.
  pub(crate) fn unbind(&mut self, h: ConnectionHandle) {
    let peer = self.entries.get(&h).and_then(|e| e.peer);
    if let Some(p) = peer
      && self.by_peer.get(&p) == Some(&h)
    {
      self.by_peer.remove(&p);
    }
  }

  /// The handle outbound frames for `peer` route to, if a validated connection is bound.
  pub(crate) fn handle_for(&self, peer: &I) -> Option<ConnectionHandle> {
    self.by_peer.get(peer).copied()
  }

  /// Bind `h` as `peer`'s routing slot (last-validated-wins) and select the per-peer EXCESS to
  /// reap: the OLDEST (by creation `seq`) live same-peer connections beyond `limit`, EXCLUDING
  /// `h` — the just-validated connection is never a reap candidate even when it is the oldest by
  /// creation (a slow hello can validate late, well after newer reconnects). Returns the stale
  /// excess, oldest-first; empty in the common within-bound case.
  pub(crate) fn validate_routing(
    &mut self,
    h: ConnectionHandle,
    peer: &I,
    limit: usize,
  ) -> Vec<ConnectionHandle> {
    if let Some(e) = self.entries.get_mut(&h) {
      e.peer = Some(*peer);
    }
    self.by_peer.insert(*peer, h);
    // Live same-peer connections, excluding `h`, newest first; everything past the `limit - 1`
    // newest "others" is stale excess (`h` itself occupies the final slot of the bound).
    let mut others: Vec<(u64, ConnectionHandle)> = self
      .entries
      .iter()
      .filter(|(hh, e)| **hh != h && e.peer.as_ref() == Some(peer) && !e.phase.is_closed())
      .map(|(hh, e)| (e.seq, *hh))
      .collect();
    others.sort_by_key(|&(seq, _)| core::cmp::Reverse(seq));
    others
      .into_iter()
      .skip(limit.saturating_sub(1))
      .map(|(_, hh)| hh)
      .collect()
  }

  /// Re-point `peer`'s empty routing slot at the NEWEST live same-peer connection, if any — the
  /// routing recovery run after a close unbinds the routed handle, so a peer holding a
  /// still-validated mutual-dial sibling keeps an outbound route across the loss. A no-op when
  /// the slot is still bound or no live sibling remains.
  pub(crate) fn promote_routing_if_unbound(&mut self, peer: &I) {
    if self.by_peer.contains_key(peer) {
      return;
    }
    let newest = self
      .entries
      .iter()
      .filter(|(_, e)| e.peer.as_ref() == Some(peer) && e.phase.is_validated())
      .max_by_key(|(_, e)| e.seq)
      .map(|(hh, _)| *hh);
    if let Some(hh) = newest {
      self.by_peer.insert(*peer, hh);
    }
  }

  /// The number of live (non-`Closed`) connections bound to `peer`.
  pub(crate) fn live_peer_count(&self, peer: &I) -> usize {
    self
      .entries
      .values()
      .filter(|e| e.peer.as_ref() == Some(peer) && !e.phase.is_closed())
      .count()
  }

  /// The earliest quinn timer across all pooled connections.
  pub(crate) fn min_conn_timeout(&mut self) -> Option<Instant> {
    self
      .entries
      .values_mut()
      .filter_map(|e| e.conn.poll_timeout())
      .min()
  }

  /// The earliest authentication deadline across `Authenticating` entries (the filter keeps a
  /// stale past deadline on a since-validated/closed entry from ever being reported).
  pub(crate) fn earliest_auth_deadline(&self) -> Option<Instant> {
    self
      .entries
      .values()
      .filter(|e| e.is_authenticating())
      .filter_map(|e| e.auth_deadline)
      .min()
  }
}
