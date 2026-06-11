//! Crypto-provider plumbing for the QUIC transport (backed by the same rustls provider as `tls`).
//!
//! [`QuicOptions`] holds the quinn-proto configs plus a tuned `TransportConfig` whose timer/window
//! values come from a [`QuicTuning`] (defaults shown):
//!
//! - `max_idle_timeout` = 3 000 ms — above the default 1 000 ms election timeout, the longest
//!   legitimate silence on a mesh edge while keep-alive is off (see below for why it is on).
//! - `keep_alive_interval` = idle/3. Raft's steady-state traffic is leader→follower only, so the
//!   follower↔follower mesh edges carry NOTHING between elections — yet a candidate needs every
//!   edge hot the moment an election starts (a cold redial costs a handshake mid-election).
//!   Keep-alive pings are what hold those zero-traffic edges under the idle timeout; idle/3 keeps
//!   two consecutive lost pings from idling a healthy edge out. quinn's default is no keep-alive.
//! - `initial_rtt` = 50 ms: the pre-sample loss-recovery estimate. The derived initial PTO
//!   (~150 ms) sits well under the election timeout, so a dropped handshake datagram retransmits
//!   long before a follower's election timer can fire over a not-yet-connected link.
//! - `max_concurrent_bidi_streams` = 4: each side opens ONE consensus stream; 4 leaves headroom
//!   for the mutual-dial doubling plus a stream reopen.
//! - `receive_window` (connection) = 16 MiB, `stream_receive_window` = 8 MiB. The single consensus
//!   stream carries frames up to [`MAX_FRAME_LEN`](crate::transport::frame::MAX_FRAME_LEN)
//!   (64 MiB, a snapshot install); a window smaller than the frame only THROTTLES it (credit
//!   regrants as the reader drains — it cannot deadlock), so the windows bound per-connection
//!   memory instead of admitting a whole snapshot in flight.
//!
//! Use [`ClusterTls`] to build a [`QuicOptions`] with mandatory mutual TLS over cluster-private
//! roots: the stock WebPki verifiers perform chain validation against the cluster CA, so a peer
//! without a valid cluster cert is rejected at the TLS handshake before any stream opens. The
//! security-relevant construction (roots, mTLS, TLS 1.3, ALPN) is pinned; only the timer/window
//! values are tunable via [`ClusterTls::tuning`].

use std::{sync::Arc, time::Duration, vec, vec::Vec};

use quinn_proto::{
  ClientConfig, EndpointConfig, IdleTimeout, ServerConfig, TransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// The rustls [`CryptoProvider`](rustls::crypto::CryptoProvider) the QUIC TLS configs are built
/// from, selected by the same `tls-*` features the byte-stream `tls` layer links (so the provider
/// matches the one `quinn-proto` is compiled against). `ring` takes precedence when both are
/// present; the `quic` feature itself enables `tls-ring`, so at least one arm is always live.
fn active_provider() -> Arc<rustls::crypto::CryptoProvider> {
  #[cfg(feature = "tls-ring")]
  {
    Arc::new(rustls::crypto::ring::default_provider())
  }
  #[cfg(all(feature = "tls-aws-lc-rs", not(feature = "tls-ring")))]
  {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
  }
}

/// The ALPN protocol every sailing QUIC connection negotiates. A non-sailing peer (or a
/// version-skewed one once this changes) fails the TLS handshake instead of mis-decoding frames.
const ALPN: &[u8] = b"sailing";

/// Default idle timeout. Must exceed the election timeout: between elections a follower↔follower
/// edge carries no consensus traffic at all (Raft is leader-centric), and even the leader→follower
/// edges go quiet for up to an election timeout during leadership changes. Keep-alive pings (below)
/// are what actually hold the zero-traffic edges; this is the backstop they refresh.
const IDLE_TIMEOUT_MILLIS: u64 = 3_000;

/// Keep-alive PING interval derived from the idle timeout: one third, so up to two consecutive
/// lost keep-alives still refresh the peer's idle timer in time. quinn's default is NO keep-alive,
/// under which a healthy-but-quiet connection idles out — and the follower↔follower mesh edges are
/// exactly that between elections, precisely the edges a candidate needs live when its election
/// timer fires. A zero result (an idle timeout too small to subdivide) leaves keep-alive off.
const fn keep_alive_interval_millis(idle_timeout_millis: u64) -> u64 {
  idle_timeout_millis / 3
}

/// Default initial RTT estimate for loss recovery BEFORE the first RTT sample. quinn derives the
/// initial Probe Timeout (the delay before a lost handshake packet first retransmits) from this;
/// its WAN-tuned default (333 ms → ~1 s PTO) would let one dropped Initial stall a dial right
/// through an election timeout. 50 ms (PTO ~150 ms) keeps handshake recovery well inside the
/// election timeout while staying ~50× a real datacenter RTT (no spurious early retransmits). A
/// geo-replicated cluster raises it via [`QuicTuning::with_initial_rtt_millis`] together with its
/// consensus timing.
const INITIAL_RTT_MILLIS: u64 = 50;

/// Pinned max concurrent bidi streams. Each side opens ONE consensus stream per connection; 4
/// covers the mutual-dial doubling plus a reopen without admitting unbounded peer-minted streams.
pub(crate) const MAX_BIDI_STREAMS: u32 = 4;

/// Connection-level receive window. Deliberately BELOW the 64 MiB max frame: a snapshot install is
/// throttled through the window (credit regrants as the reader drains; flow control cannot
/// deadlock), bounding per-connection buffered memory instead of admitting a whole snapshot.
const CONNECTION_RECEIVE_WINDOW: u64 = 16 * 1024 * 1024;

/// Per-stream receive window, bounded below the connection window so the single consensus stream
/// cannot pin the entire connection window at once.
const STREAM_RECEIVE_WINDOW: u64 = 8 * 1024 * 1024;

/// The largest value a QUIC `VarInt` can carry (`2^62 - 1`). The tuning setters clamp to this so an
/// embedder-supplied window/timeout can never make the `VarInt` conversions in
/// [`QuicOptions::build_transport`] fail at construction time.
const MAX_VARINT_U64: u64 = (1 << 62) - 1;

/// Clamp an embedder-supplied tuning value into `1..=MAX_VARINT_U64`: never zero (a zero timeout or
/// window is a wedge, not a tuning), never past the QUIC `VarInt` range.
const fn clamp_tuning(v: u64) -> u64 {
  if v == 0 {
    1
  } else if v > MAX_VARINT_U64 {
    MAX_VARINT_U64
  } else {
    v
  }
}

/// Embedder-tunable timer and flow-control values for the QUIC `TransportConfig`, with `Default` =
/// the pinned LAN-tuned constants (see each constant for the rationale). A geo-replicated cluster —
/// where the defaults' assumptions (sub-50 ms RTT, election-timeout headroom) do not hold —
/// overrides them via [`ClusterTls::tuning`].
///
/// **Scope (the security posture).** This carries ONLY performance knobs — timers and window
/// sizes. The security-relevant construction (cluster-private roots, mandatory mTLS, TLS 1.3,
/// ALPN) lives exclusively inside [`ClusterTls::build`] and no tuning value can reach it.
///
/// Setters clamp to `1..=2^62-1` (the QUIC `VarInt` range) so no embedder value can wedge the
/// transport with a zero timeout/window or fail the `VarInt` conversions at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuicTuning {
  /// `max_idle_timeout`, milliseconds. Default [`IDLE_TIMEOUT_MILLIS`].
  idle_timeout_millis: u64,
  /// `keep_alive_interval`, milliseconds. `None` (the default) derives idle/3 — the two-lost-pings
  /// margin [`keep_alive_interval_millis`] documents; an explicit `Some(0)` disables keep-alive.
  keep_alive_interval_millis: Option<u64>,
  /// `initial_rtt`, milliseconds. Default [`INITIAL_RTT_MILLIS`].
  initial_rtt_millis: u64,
  /// Connection-level `receive_window`, bytes. Default [`CONNECTION_RECEIVE_WINDOW`].
  connection_receive_window: u64,
  /// Per-stream `stream_receive_window`, bytes. Default [`STREAM_RECEIVE_WINDOW`].
  stream_receive_window: u64,
}

impl QuicTuning {
  /// The default tuning — exactly the constants the transport pins without an override.
  #[must_use]
  pub const fn new() -> Self {
    Self {
      idle_timeout_millis: IDLE_TIMEOUT_MILLIS,
      keep_alive_interval_millis: None,
      initial_rtt_millis: INITIAL_RTT_MILLIS,
      connection_receive_window: CONNECTION_RECEIVE_WINDOW,
      stream_receive_window: STREAM_RECEIVE_WINDOW,
    }
  }

  /// Idle timeout in milliseconds (see `IDLE_TIMEOUT_MILLIS` for the consensus coupling).
  #[inline(always)]
  pub const fn idle_timeout_millis(&self) -> u64 {
    self.idle_timeout_millis
  }

  /// The RESOLVED keep-alive interval in milliseconds: the explicit override when one was set
  /// (`0` = keep-alive off), otherwise idle/3 — derived from the CURRENT idle timeout, so raising
  /// the idle timeout scales the keep-alive with it.
  #[inline(always)]
  pub const fn keep_alive_interval_millis(&self) -> u64 {
    match self.keep_alive_interval_millis {
      Some(ms) => ms,
      None => keep_alive_interval_millis(self.idle_timeout_millis),
    }
  }

  /// Initial RTT estimate in milliseconds (see `INITIAL_RTT_MILLIS` for why the default is
  /// LAN-tuned and what a geo-replicated cluster must raise it to).
  #[inline(always)]
  pub const fn initial_rtt_millis(&self) -> u64 {
    self.initial_rtt_millis
  }

  /// Connection-level receive window in bytes (see `CONNECTION_RECEIVE_WINDOW`).
  #[inline(always)]
  pub const fn connection_receive_window(&self) -> u64 {
    self.connection_receive_window
  }

  /// Per-stream receive window in bytes (see `STREAM_RECEIVE_WINDOW`).
  #[inline(always)]
  pub const fn stream_receive_window(&self) -> u64 {
    self.stream_receive_window
  }

  /// Override the idle timeout (milliseconds; clamped to `1..=2^62-1`). Must stay ABOVE the
  /// election timeout, or healthy links idle out between elections; with the default (derived)
  /// keep-alive the idle/3 ping interval scales along with it.
  #[must_use]
  pub const fn with_idle_timeout_millis(mut self, millis: u64) -> Self {
    self.idle_timeout_millis = clamp_tuning(millis);
    self
  }

  /// Override the keep-alive interval (milliseconds; `0` disables keep-alive). Without an override
  /// the interval is derived as idle/3. Disabling keep-alive on a production mesh is a liveness
  /// hazard: the zero-traffic follower↔follower edges then idle out between elections (see
  /// `IDLE_TIMEOUT_MILLIS`).
  #[must_use]
  pub const fn with_keep_alive_interval_millis(mut self, millis: u64) -> Self {
    self.keep_alive_interval_millis = Some(millis);
    self
  }

  /// Override the initial RTT estimate (milliseconds; clamped to `1..=2^62-1`). Keep it at or
  /// above the real inter-node RTT: an estimate far below it provokes spurious handshake
  /// retransmits, while the election timeout must in turn stay above the resulting ~3×RTT initial
  /// probe timeout.
  #[must_use]
  pub const fn with_initial_rtt_millis(mut self, millis: u64) -> Self {
    self.initial_rtt_millis = clamp_tuning(millis);
    self
  }

  /// Override the connection-level receive window (bytes; clamped to `1..=2^62-1`). A window below
  /// the 64 MiB max frame only throttles a snapshot install (credit regrants as the reader
  /// drains); it cannot deadlock it.
  #[must_use]
  pub const fn with_connection_receive_window(mut self, bytes: u64) -> Self {
    self.connection_receive_window = clamp_tuning(bytes);
    self
  }

  /// Override the per-stream receive window (bytes; clamped to `1..=2^62-1`). Keep it at or below
  /// the connection window.
  #[must_use]
  pub const fn with_stream_receive_window(mut self, bytes: u64) -> Self {
    self.stream_receive_window = clamp_tuning(bytes);
    self
  }
}

impl Default for QuicTuning {
  fn default() -> Self {
    Self::new()
  }
}

/// Default cap on the number of LIVE connections the bridge holds at once (dialed + accepted). The
/// network is untrusted: an inbound flood of foreign-CA / no-cert Initials would otherwise each
/// allocate a `Connection` before identity validation could reject it. At the cap the bridge
/// statelessly refuses further inbound attempts instead of allocating. The coordinator RAISES the
/// effective cap to [`mesh_connection_floor`] of the tracked peer count at construction, so a
/// default-cap node on a large cluster still admits its whole steady-state mesh.
const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// A small constant floor on the connection cap, so even a 1- or 2-node cluster (whose mutual-dial
/// mesh is tiny) keeps a little accept/reconnect headroom.
const MIN_CONNECTION_FLOOR: usize = 4;

/// The minimum live-connection cap that admits a node's full steady-state mutual-dial mesh over
/// `peers` TRACKED peers (every id the consensus endpoint replicates to, EXCLUDING this node —
/// voters in both joint halves, learners, and incoming learners alike: the transport meshes with
/// all of them), plus reconnect headroom: `max(MIN_CONNECTION_FLOOR, 3 * peers)`.
///
/// The mutual-dial design keeps TWO physical connections per peer pair (each side dials the other
/// and both are kept; see the bridge's bind policy), so a node holds `2*peers` steady-state
/// connections; a reconnecting peer can briefly hold a THIRD (the new dial/accept overlapping the
/// old one), so one reconnect slot per peer is added. The coordinator raises `max_connections` to
/// this when the configured cap is lower, so the cap can never refuse a legitimate steady-state
/// mesh connection; it still bounds an untrusted-network flood.
pub(crate) const fn mesh_connection_floor(peers: usize) -> usize {
  let mesh_with_reconnect = peers * 3;
  if mesh_with_reconnect > MIN_CONNECTION_FLOOR {
    mesh_with_reconnect
  } else {
    MIN_CONNECTION_FLOOR
  }
}

/// Immutable QUIC config bundle handed to the coordinator. Accessor-only; all fields are private
/// and cannot be mutated after construction (the security-relevant parts are pinned by
/// [`ClusterTls::build`]).
pub struct QuicOptions {
  endpoint: Arc<EndpointConfig>,
  client: Option<ClientConfig>,
  server: Option<Arc<ServerConfig>>,
  /// Set to `true` by [`ClusterTls::build`]; `false` for the accept-any test path.
  requires_client_auth: bool,
  /// Cap on the number of live connections the bridge holds at once. Inbound attempts past this
  /// are refused (stateless close) instead of allocating, bounding an accept flood.
  max_connections: usize,
}

impl QuicOptions {
  /// Build from caller-supplied configs and a tuning. The tuned `TransportConfig` (idle timeout +
  /// keep-alive + stream caps + flow-control windows) is constructed internally and installed on
  /// both the server and client configs.
  pub fn new(
    endpoint: EndpointConfig,
    client: Option<ClientConfig>,
    server: Option<ServerConfig>,
    tuning: QuicTuning,
  ) -> Self {
    Self::new_inner(endpoint, client, server, tuning, false)
  }

  fn new_inner(
    endpoint: EndpointConfig,
    client: Option<ClientConfig>,
    server: Option<ServerConfig>,
    tuning: QuicTuning,
    requires_client_auth: bool,
  ) -> Self {
    let transport = Self::build_transport(&tuning);
    let server = server.map(|mut s| {
      s.transport_config(transport.clone());
      Arc::new(s)
    });
    let mut client = client;
    if let Some(ref mut c) = client {
      c.transport_config(transport);
    }
    Self {
      endpoint: Arc::new(endpoint),
      client,
      server,
      requires_client_auth,
      max_connections: DEFAULT_MAX_CONNECTIONS,
    }
  }

  /// Cheap clone of the endpoint config arc.
  #[inline(always)]
  pub fn endpoint_config(&self) -> Arc<EndpointConfig> {
    self.endpoint.clone()
  }

  /// Cheap clone of the client config used for outbound dials, if any.
  #[inline(always)]
  pub fn client_config(&self) -> Option<ClientConfig> {
    self.client.clone()
  }

  /// Cheap clone of the server config arc, if any.
  #[inline(always)]
  pub fn server_config(&self) -> Option<Arc<ServerConfig>> {
    self.server.clone()
  }

  /// Whether the server config was built with mandatory client-certificate authentication. `true`
  /// only when constructed via [`ClusterTls::build`]; the provided identity scheme requires it.
  #[inline(always)]
  pub const fn requires_client_auth(&self) -> bool {
    self.requires_client_auth
  }

  /// The cap on live connections (dialed + accepted). The bridge refuses inbound attempts once the
  /// table holds this many, bounding an untrusted-network accept flood.
  #[inline(always)]
  pub const fn max_connections(&self) -> usize {
    self.max_connections
  }

  /// Override the live-connection cap (see [`Self::max_connections`]). The coordinator RAISES the
  /// effective cap to the membership-sized mesh floor — `max(4, 3 * peers)` over every TRACKED
  /// peer (voters in both joint halves, learners, and incoming learners, minus the node itself) —
  /// whenever the value set here is lower, and recomputes that floor as committed configuration
  /// changes apply, so the cap can never refuse a legitimate mesh connection even as the
  /// membership grows. A value of 0 is clamped to 1 so at least one connection is always
  /// admissible.
  #[must_use]
  pub const fn with_max_connections(mut self, max: usize) -> Self {
    self.max_connections = if max == 0 { 1 } else { max };
    self
  }

  /// Build the tuned `TransportConfig` shared between server and client. The timer/window values
  /// come from `tuning`; the stream caps and the closed protocol surfaces below are pinned.
  fn build_transport(tuning: &QuicTuning) -> Arc<TransportConfig> {
    let mut tc = TransportConfig::default();
    let idle = IdleTimeout::try_from(Duration::from_millis(tuning.idle_timeout_millis()))
      .expect("idle timeout within VarInt range (clamped by the tuning setter)");
    tc.max_idle_timeout(Some(idle));
    // Keep-alive pings hold the zero-traffic follower↔follower mesh edges under the idle timeout
    // (see `keep_alive_interval_millis`); a resolved 0 means keep-alive off.
    let keep_alive = tuning.keep_alive_interval_millis();
    if keep_alive > 0 {
      tc.keep_alive_interval(Some(Duration::from_millis(keep_alive)));
    }
    tc.initial_rtt(Duration::from_millis(tuning.initial_rtt_millis()));
    tc.max_concurrent_bidi_streams(VarInt::from_u32(MAX_BIDI_STREAMS));
    // Close the protocol surfaces this transport does NOT use, so a buggy/version-skewed but
    // fully validated peer cannot pin connection-level receive credit or memory on them:
    // - Incoming UNIDIRECTIONAL streams: limit 0 — consensus rides ONE framed bidi stream; a peer
    //   cannot mint a uni stream against a 0 limit, and forcing one is a protocol violation quinn
    //   turns into a connection close.
    // - DATAGRAM receive: `None` stops advertising `max_datagram_size`, so an unsolicited QUIC
    //   DATAGRAM frame is a protocol violation rather than buffered bytes.
    tc.max_concurrent_uni_streams(VarInt::from_u32(0));
    tc.datagram_receive_buffer_size(None);
    tc.receive_window(
      VarInt::from_u64(tuning.connection_receive_window())
        .expect("connection window within VarInt range (clamped by the tuning setter)"),
    );
    tc.stream_receive_window(
      VarInt::from_u64(tuning.stream_receive_window())
        .expect("stream window within VarInt range (clamped by the tuning setter)"),
    );
    Arc::new(tc)
  }
}

/// Builds a [`QuicOptions`] bundle with mandatory mutual TLS over a cluster-private root CA.
///
/// Both directions are fully authenticated:
///
/// - **Server side** uses [`rustls::server::WebPkiClientVerifier`] rooted at the cluster CA, which
///   makes client certificates mandatory by default. A peer without a cert (or whose cert does not
///   chain to the cluster CA) is rejected at the TLS handshake before any QUIC stream opens.
/// - **Client side** uses [`rustls::client::WebPkiServerVerifier`] with the same cluster CA, and
///   presents this node's cert chain for mutual authentication.
///
/// Both configs are TLS 1.3-only with ALPN `b"sailing"`.
///
/// ## SNI server name
///
/// The stock `WebPkiServerVerifier` validates the SNI `server_name` the dialer supplies against
/// the server cert's Subject Alternative Names. Mint each node's cert with a DNS SAN of the form
/// `node-<id-hex>.<cluster-hex>.sailing` (the coordinator derives that name on `connect` from the
/// dialed peer's `Data` encoding and the cluster id), so the verifier can match it. DNS bounds
/// one label to 63 octets, so this SAN form supports id encodings up to 29 bytes; larger ids
/// dial with an explicit name (`connect_with_server_name`) and certs minted to match.
pub struct ClusterTls {
  roots: rustls::RootCertStore,
  chain: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
  tuning: QuicTuning,
}

impl ClusterTls {
  /// Create a new `ClusterTls` builder.
  ///
  /// - `roots` — the cluster-private CA(s); only peers whose cert chains to one of these roots
  ///   will complete the handshake.
  /// - `chain` — this node's certificate chain (leaf first).
  /// - `key` — the private key for the leaf certificate.
  pub fn new(
    roots: rustls::RootCertStore,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
  ) -> Self {
    Self {
      roots,
      chain,
      key,
      tuning: QuicTuning::new(),
    }
  }

  /// Override the transport timer/window tuning for the built [`QuicOptions`]. The default is
  /// [`QuicTuning::new`] (the LAN-tuned constants). Tuning carries ONLY performance knobs — the
  /// mandatory-mTLS construction this builder performs is unaffected by any tuning value.
  #[must_use]
  pub fn tuning(mut self, tuning: QuicTuning) -> Self {
    self.tuning = tuning;
    self
  }

  /// Consume the builder and produce a [`QuicOptions`] with both a server config (mandatory client
  /// auth) and a client config (mTLS).
  ///
  /// # Panics
  ///
  /// Panics if the supplied roots/chain/key are not a valid cluster-CA bundle (an empty root
  /// store, a key that does not match the leaf, TLS 1.3 unsupported by the linked provider) —
  /// construction-time configuration errors, not runtime conditions.
  pub fn build(self) -> QuicOptions {
    use quinn_proto::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use rustls::{client::WebPkiServerVerifier, server::WebPkiClientVerifier};

    let provider = active_provider();
    let roots = Arc::new(self.roots);

    // Server: mandatory client-cert auth via the cluster CA.
    let client_verifier =
      WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
        .build()
        .expect("WebPkiClientVerifier with valid cluster roots");
    let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3 is supported by the active provider")
      .with_client_cert_verifier(client_verifier)
      .with_single_cert(self.chain.clone(), self.key.clone_key())
      .expect("valid cluster cert and key");
    rustls_server.alpn_protocols = vec![ALPN.to_vec()];
    let qsc = QuicServerConfig::try_from(Arc::new(rustls_server))
      .expect("QuicServerConfig from cluster-CA rustls ServerConfig");
    let server = ServerConfig::with_crypto(Arc::new(qsc));

    // Client: verify the server against the cluster CA; present this node's cert.
    let server_verifier = WebPkiServerVerifier::builder_with_provider(roots, provider.clone())
      .build()
      .expect("WebPkiServerVerifier with valid cluster roots");
    let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])
      .expect("TLS 1.3 is supported by the active provider")
      .dangerous()
      .with_custom_certificate_verifier(server_verifier)
      .with_client_auth_cert(self.chain, self.key)
      .expect("valid cluster cert and key for client auth");
    rustls_client.alpn_protocols = vec![ALPN.to_vec()];
    let qcc = QuicClientConfig::try_from(Arc::new(rustls_client))
      .expect("QuicClientConfig from cluster-CA rustls ClientConfig");
    let client = ClientConfig::new(Arc::new(qcc));

    QuicOptions::new_inner(
      EndpointConfig::default(),
      Some(client),
      Some(server),
      self.tuning,
      true,
    )
  }
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;

  /// A test-only cluster CA + per-node certificate issuer (rcgen). `issue_node` issues leaf certs
  /// signed by the CA with the DNS SAN `node-<id-hex>.<cluster-hex>.sailing` — the name
  /// [`sni_for`](super::super::sni_for) derives on `connect`, so the stock WebPki verifier
  /// matches it.
  pub(crate) struct TestClusterCa {
    ca_cert: rcgen::Certificate,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
  }

  pub(crate) struct TestNodeCert {
    pub(crate) cert: rcgen::Certificate,
    pub(crate) key: rcgen::KeyPair,
  }

  impl TestClusterCa {
    pub(crate) fn generate() -> Self {
      let mut params = rcgen::CertificateParams::new(Vec::new()).expect("empty CA params");
      params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
      params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
      let key = rcgen::KeyPair::generate().expect("CA key pair");
      let ca_cert = params.self_signed(&key).expect("self-signed CA");
      let issuer = rcgen::Issuer::new(
        rcgen::CertificateParams::new(Vec::new()).expect("issuer params"),
        key,
      );
      Self { ca_cert, issuer }
    }

    /// Build a `RootCertStore` containing the CA certificate.
    pub(crate) fn roots(&self) -> rustls::RootCertStore {
      let mut store = rustls::RootCertStore::empty();
      store
        .add(CertificateDer::from(self.ca_cert.der().to_vec()))
        .expect("CA cert parses as a trust anchor");
      store
    }

    /// Issue a leaf certificate signed by this CA with the SAN `node-<id_hex>.<cluster_hex>.sailing`.
    pub(crate) fn issue_node(&self, san: &str) -> TestNodeCert {
      let mut params =
        rcgen::CertificateParams::new(std::vec![san.to_string()]).expect("valid DNS SAN");
      params
        .key_usages
        .push(rcgen::KeyUsagePurpose::DigitalSignature);
      params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
      params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
      let key = rcgen::KeyPair::generate().expect("leaf key pair");
      let cert = params.signed_by(&key, &self.issuer).expect("signed leaf");
      TestNodeCert { cert, key }
    }

    /// A ready [`ClusterTls`] for one node (roots + the node's chain + key).
    pub(crate) fn cluster_tls(&self, san: &str) -> ClusterTls {
      let node = self.issue_node(san);
      ClusterTls::new(
        self.roots(),
        std::vec![CertificateDer::from(node.cert.der().to_vec())],
        PrivateKeyDer::try_from(node.key.serialize_der()).expect("leaf key DER"),
      )
    }
  }

  #[test]
  fn cluster_tls_builds_mutual_auth_options() {
    let ca = TestClusterCa::generate();
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .build();
    assert!(
      opts.requires_client_auth(),
      "ClusterTls pins mandatory mTLS"
    );
    assert!(opts.client_config().is_some(), "dial config present");
    assert!(opts.server_config().is_some(), "accept config present");
    assert_eq!(opts.max_connections(), DEFAULT_MAX_CONNECTIONS);
  }

  #[test]
  fn tuning_clamps_and_derives() {
    let t = QuicTuning::new();
    assert_eq!(t.idle_timeout_millis(), IDLE_TIMEOUT_MILLIS);
    assert_eq!(
      t.keep_alive_interval_millis(),
      IDLE_TIMEOUT_MILLIS / 3,
      "keep-alive derives idle/3 by default"
    );
    assert_eq!(t.initial_rtt_millis(), INITIAL_RTT_MILLIS);

    // Zero clamps to 1 (a zero timeout/window is a wedge, not a tuning).
    let t = QuicTuning::new()
      .with_idle_timeout_millis(0)
      .with_initial_rtt_millis(0)
      .with_connection_receive_window(0)
      .with_stream_receive_window(0);
    assert_eq!(t.idle_timeout_millis(), 1);
    assert_eq!(t.initial_rtt_millis(), 1);
    assert_eq!(t.connection_receive_window(), 1);
    assert_eq!(t.stream_receive_window(), 1);

    // Past the VarInt range clamps to 2^62-1 so build_transport's conversions cannot fail.
    let t = QuicTuning::new().with_connection_receive_window(u64::MAX);
    assert_eq!(t.connection_receive_window(), MAX_VARINT_U64);

    // An explicit keep-alive override replaces the derivation; 0 = off.
    let t = QuicTuning::new().with_keep_alive_interval_millis(0);
    assert_eq!(t.keep_alive_interval_millis(), 0);
    let t = QuicTuning::new().with_keep_alive_interval_millis(250);
    assert_eq!(t.keep_alive_interval_millis(), 250);

    // The raised idle timeout scales the derived keep-alive with it.
    let t = QuicTuning::new().with_idle_timeout_millis(9_000);
    assert_eq!(t.keep_alive_interval_millis(), 3_000);
  }

  #[test]
  fn mesh_floor_covers_the_mutual_dial_mesh() {
    assert_eq!(mesh_connection_floor(0), MIN_CONNECTION_FLOOR);
    assert_eq!(mesh_connection_floor(1), MIN_CONNECTION_FLOOR);
    assert_eq!(mesh_connection_floor(2), 6);
    assert_eq!(mesh_connection_floor(4), 12);
    // 63 peers (a 64-node mesh): 3*63 = 189 — the per-peer bound times the peer count.
    assert_eq!(mesh_connection_floor(63), 189);
  }

  #[test]
  fn options_cap_override_is_clamped() {
    let ca = TestClusterCa::generate();
    let opts = ca
      .cluster_tls("node-01.00000000000000000000000000000000.sailing")
      .build()
      .with_max_connections(0);
    assert_eq!(opts.max_connections(), 1, "0 clamps to 1");
  }
}
