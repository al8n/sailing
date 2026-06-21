//! Pluggable peer-identity seam for the QUIC transport.
//!
//! [`IdentitySource`] is the single trait the coordinator calls to (a) write a control-stream
//! preface and (b) authenticate the peer from post-handshake material. The coordinator owns the
//! binding policy (dialed-match / cluster cross-check / not-self); the source owns only the
//! extraction logic.

use std::vec::Vec;

use rustls::pki_types::CertificateDer;

use super::super::{ClusterId, labeled};
use crate::Data;

/// A settled, authenticated peer identity — an UNTRUSTED candidate the coordinator re-checks.
///
/// Carries BOTH the attested peer AND the cluster it was attested for. The source REPORTS the
/// cluster it parsed (from the hello frame, or a certificate extension); the coordinator then
/// re-asserts that cluster equals its own configured cluster.
///
/// For the provided [`Hello`] source this re-assertion is an un-bypassable gate: it reports the
/// GENUINE attested cluster it parsed from the handshake material, so the coordinator's check
/// rejects any wrong-cluster peer. A custom source supplied through
/// `dangerous_custom_identity` owns its own cluster correctness — it can mint an `Identified`
/// with ANY cluster (this constructor is `pub`), so the coordinator's check only re-confirms
/// whatever cluster that source asserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identified<I> {
  who: I,
  cluster: ClusterId,
}

impl<I> Identified<I> {
  /// Wrap a claimed peer attested for `cluster` (the coordinator re-checks BOTH before binding).
  /// The provided source passes the genuine parsed cluster; a custom source is trusted to pass
  /// the cluster it actually attested (see the type docs).
  pub const fn new(who: I, cluster: ClusterId) -> Self {
    Self { who, cluster }
  }

  /// The claimed peer identity.
  #[inline(always)]
  pub const fn who(&self) -> &I {
    &self.who
  }

  /// The cluster this identity was attested for. The coordinator binds only when this equals its
  /// own configured cluster.
  #[inline(always)]
  pub const fn cluster(&self) -> &ClusterId {
    &self.cluster
  }

  /// Decompose into the claimed peer (the coordinator takes ownership at bind).
  #[inline(always)]
  pub fn into_who(self) -> I {
    self.who
  }
}

/// The result of an [`IdentitySource::authenticate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityOutcome<I> {
  /// More control-stream input is needed before identity can be determined (no control frame has
  /// been delivered yet — the cert-only probe).
  Pending,
  /// A candidate identity was extracted. The coordinator re-checks before binding.
  Identified(Identified<I>),
  /// The peer cannot be authenticated; the coordinator must close the connection.
  Rejected,
}

/// Read-only handshake material handed to [`IdentitySource::authenticate`].
///
/// Carries NO dialed expectation and NO message body — so `authenticate` cannot collapse the
/// authenticator into a message's self-claim. The coordinator owns the binding policy.
///
/// The control-frame payload is an [`Option`] so a source can tell the two pre-bind calls apart:
/// the coordinator calls `authenticate` ONCE at `Connected` with NO control frame yet (the
/// cert-only probe — [`None`]), then again with the FIRST delivered control frame ([`Some`], even
/// when that frame is empty or short). On QUIC that delivered frame is a COMPLETE, already-popped
/// frame — not a byte-stream prefix with more bytes to come — so it is the SOLE hello
/// opportunity: a preface source must reject a malformed/short first frame here rather than wait
/// for a later one.
#[non_exhaustive]
pub struct IdentityCtx<'a> {
  peer_certs: &'a [CertificateDer<'a>],
  control_frame: Option<&'a [u8]>,
  our_cluster: ClusterId,
}

impl<'a> IdentityCtx<'a> {
  /// Construct the context from validated handshake material. `control_frame` is [`None`] for the
  /// coordinator's `Connected` cert-only probe and [`Some`] for the first delivered control frame
  /// — even an empty/short one.
  pub(crate) const fn new(
    peer_certs: &'a [CertificateDer<'a>],
    control_frame: Option<&'a [u8]>,
    our_cluster: ClusterId,
  ) -> Self {
    Self {
      peer_certs,
      control_frame,
      our_cluster,
    }
  }

  /// The peer's certificate chain as validated by the TLS layer (empty when none was presented).
  #[inline(always)]
  pub const fn peer_certs(&self) -> &[CertificateDer<'a>] {
    self.peer_certs
  }

  /// The peer's first control-stream frame payload, or [`None`] when no control frame has been
  /// delivered yet (the coordinator's `Connected` cert-only probe).
  ///
  /// On QUIC a [`Some`] payload is a COMPLETE control FRAME the bridge already popped — never a
  /// byte-stream prefix awaiting more bytes. So a preface-based source treats [`Some`] as the
  /// SOLE hello opportunity (a short/empty/partial frame must be REJECTED, not deferred), and
  /// [`None`] as "wait for the first frame". A cert-only source ignores this entirely.
  #[inline(always)]
  pub const fn control_frame(&self) -> Option<&[u8]> {
    self.control_frame
  }

  /// The cluster id this coordinator was built for.
  #[inline(always)]
  pub const fn our_cluster(&self) -> &ClusterId {
    &self.our_cluster
  }
}

/// Establishes the authenticated peer id for a QUIC connection.
///
/// One impl per cluster, chosen at coordinator construction. The provided [`Hello`] scheme
/// requires cluster-private roots + mandatory mTLS (supplied via [`ClusterTls`](super::ClusterTls))
/// — its control-preface self-claim is trustworthy only because the TLS layer already proved the
/// peer holds a cluster certificate.
///
/// The coordinator — never the impl — applies the binding policy: `authenticate` returns an
/// UNTRUSTED candidate; the coordinator does the dialed→match-or-abort / accepted→adopt step, the
/// not-our-own-id gate, and the unconditional cluster cross-check.
pub trait IdentitySource<I> {
  /// Append this node's control-channel preface to `out`. Written as the FIRST frame on the
  /// consensus send stream. Impls whose identity rides entirely in the TLS certificate write
  /// nothing.
  ///
  /// # Size contract
  ///
  /// The appended preface MUST be at most `MAX_HELLO_LEN` bytes (the hello header plus the
  /// 1024-byte id cap) — the receive side bounds its pre-authentication intake to one hello's
  /// worth of frame, so an oversized preface can never authenticate. A violation is NOT a
  /// construction error: the bridge closes the connection before a single preface byte is sent
  /// (and the peer would reject it anyway), so it surfaces as a connection-level failure. The
  /// provided [`Hello`] stays within the bound for any node id whose encoding fits the hello's
  /// 1..=1024-byte id field; an id encoding beyond that is a deployment misconfiguration that
  /// fails exactly that way.
  fn write_control_preface(&self, me: &I, out: &mut Vec<u8>);

  /// Authenticate the peer from handshake material only.
  ///
  /// The returned candidate inside [`IdentityOutcome::Identified`] is re-checked by the
  /// coordinator (dialed-match / cluster cross-check / not-self) — never a binding.
  fn authenticate(&self, ctx: &IdentityCtx<'_>) -> IdentityOutcome<I>;
}

/// Identity via the `Labeled` hello as the first control-stream frame — the SAME wire format the
/// TCP/TLS stream transport's handshake uses (`[magic][version][cluster(16)][u16 len][peer id]`),
/// so one parser and one version byte govern both transports. `authenticate` parses the frame
/// against the endpoint's cluster and returns the claimed peer as a candidate.
pub struct Hello {
  cluster: ClusterId,
}

impl Hello {
  /// Build a `Hello` identity source for the given cluster — the cluster this node's OWN preface
  /// advertises. It must equal the coordinator's configured cluster (the coordinator asserts this
  /// at construction); the authenticated-peer parse uses the coordinator's cluster, not this
  /// field.
  pub const fn new(cluster: ClusterId) -> Self {
    Self { cluster }
  }

  /// The cluster this source writes into its preface.
  #[inline(always)]
  pub const fn cluster(&self) -> &ClusterId {
    &self.cluster
  }
}

impl<I: Data> IdentitySource<I> for Hello {
  fn write_control_preface(&self, me: &I, out: &mut Vec<u8>) {
    let mut id = Vec::new();
    me.encode(&mut id);
    out.extend_from_slice(&labeled::build_hello(&self.cluster, &id));
  }

  fn authenticate(&self, ctx: &IdentityCtx<'_>) -> IdentityOutcome<I> {
    // No control frame delivered yet (the `Connected` cert-only probe): the hello rides a control
    // frame, none of which has arrived, so wait for the first one. This is the ONLY path to
    // `Pending` — once a frame is delivered below, it is the sole hello opportunity and an
    // incomplete parse is a hard reject, not a deferral.
    let Some(frame) = ctx.control_frame() else {
      return IdentityOutcome::Pending;
    };
    // Parse against the ENDPOINT's cluster (`ctx.our_cluster()`), not this source's configured
    // field: the coordinator single-sources the cluster, and `parse_hello_frame` rejects a hello
    // whose encoded cluster differs — so on success the attested cluster IS `our_cluster` (the
    // genuine parse the coordinator's cross-check then re-confirms). The parse is TOTAL: a short,
    // truncated, or trailing-bytes frame is rejected, never deferred.
    let Some(id_bytes) = labeled::parse_hello_frame(frame, ctx.our_cluster()) else {
      return IdentityOutcome::Rejected;
    };
    // The id must decode consuming exactly its advertised bytes (the same exact-consumption rule
    // the stream transport's hello applies): two byte strings must never decode to the same peer.
    match I::decode_exact(bytes::Bytes::copy_from_slice(id_bytes)) {
      Ok(who) => IdentityOutcome::Identified(Identified::new(who, *ctx.our_cluster())),
      Err(_) => IdentityOutcome::Rejected,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn cluster(b: u8) -> ClusterId {
    ClusterId([b; 16])
  }

  fn preface_of(id: u64, c: ClusterId) -> Vec<u8> {
    let h = Hello::new(c);
    let mut out = Vec::new();
    <Hello as IdentitySource<u64>>::write_control_preface(&h, &id, &mut out);
    out
  }

  fn auth(frame: Option<&[u8]>, our: ClusterId) -> IdentityOutcome<u64> {
    let h = Hello::new(our);
    h.authenticate(&IdentityCtx::new(&[], frame, our))
  }

  #[test]
  fn hello_roundtrip_identifies_the_peer() {
    let c = cluster(7);
    let preface = preface_of(42, c);
    match auth(Some(&preface), c) {
      IdentityOutcome::Identified(id) => {
        assert_eq!(*id.who(), 42u64);
        assert_eq!(
          *id.cluster(),
          c,
          "attested cluster is the genuine parsed one"
        );
      }
      other => panic!("expected Identified, got {other:?}"),
    }
  }

  #[test]
  fn no_frame_is_pending_not_rejected() {
    // The cert-only probe: the hello rides a control frame none of which has arrived.
    assert_eq!(auth(None, cluster(7)), IdentityOutcome::Pending);
  }

  #[test]
  fn wrong_cluster_is_rejected() {
    let preface = preface_of(42, cluster(7));
    assert_eq!(auth(Some(&preface), cluster(9)), IdentityOutcome::Rejected);
  }

  #[test]
  fn malformed_frames_are_hard_rejects_not_deferrals() {
    let c = cluster(7);
    let good = preface_of(42, c);

    // A delivered first frame is the SOLE hello opportunity: truncation at EVERY byte boundary is
    // a hard reject (on the byte-stream transport a prefix legitimately waits for more bytes; on
    // QUIC the frame is complete by construction, so a short one can never grow).
    for cut in 0..good.len() {
      assert_eq!(
        auth(Some(&good[..cut]), c),
        IdentityOutcome::Rejected,
        "truncated hello at byte {cut} must reject"
      );
    }

    // Trailing bytes after a valid hello: a framing violation, not a hello.
    let mut trailing = good.clone();
    trailing.push(0xEE);
    assert_eq!(auth(Some(&trailing), c), IdentityOutcome::Rejected);

    // Magic / version violations.
    let mut bad_magic = good.clone();
    bad_magic[0] ^= 0xFF;
    assert_eq!(auth(Some(&bad_magic), c), IdentityOutcome::Rejected);
    let mut bad_version = good.clone();
    bad_version[1] = 0xFE;
    assert_eq!(auth(Some(&bad_version), c), IdentityOutcome::Rejected);

    // A zero-length id is no identity at all.
    let mut zero_id = good.clone();
    zero_id[18] = 0;
    zero_id[19] = 0;
    zero_id.truncate(super::super::super::labeled::HELLO_HEADER);
    assert_eq!(auth(Some(&zero_id), c), IdentityOutcome::Rejected);
  }

  #[test]
  fn id_must_decode_exactly() {
    // A 9-byte id whose first 8 bytes decode as a u64 but leave one trailing byte: the id-length
    // field admits it through the hello header, but the NodeId decode must consume the id bytes
    // exactly — a trailing byte is a distinct byte string and must not alias the 8-byte peer.
    let c = cluster(7);
    let mut id9 = Vec::new();
    42u64.encode(&mut id9);
    id9.push(0xAB);
    let frame = labeled::build_hello(&c, &id9);
    assert_eq!(auth(Some(&frame), c), IdentityOutcome::Rejected);
  }

  #[test]
  fn preface_fits_the_preauth_budget() {
    // The size contract: the provided Hello's preface is at most MAX_HELLO_LEN, so the receive
    // side's pre-authentication intake bound always admits it.
    let preface = preface_of(u64::MAX, cluster(0xFF));
    assert!(preface.len() <= super::super::MAX_HELLO_LEN);
  }
}
