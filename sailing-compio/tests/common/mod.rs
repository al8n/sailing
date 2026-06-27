//! Minimal contract-conformant in-memory storage + a counting state machine for the
//! integration tests — the smallest honest embedder. Writes complete synchronously (every
//! submit queues its completion immediately), which satisfies the prefix-ordered durability
//! contract trivially; the storage-ready seam is exercised separately.

use std::collections::VecDeque;

use bytes::Bytes;
use sailing_proto::{
  EntriesRead, Entry, HardState, Index, LogDone, LogStore, MaybeOwned, OpId, SnapshotChunkRead, SnapshotMeta,
  StableDone, StableStore, StateMachine, Term,
};

/// A counting state machine: applies are counted; the response is the post-apply count.
#[derive(Default)]
pub struct CountSm {
  count: u64,
}

impl CountSm {
  pub fn count(&self) -> u64 {
    self.count
  }
}

impl StateMachine for CountSm {
  type Command = Bytes;
  type Response = u64;
  type Snapshot = u64;
  type Error = std::convert::Infallible;

  fn apply(&mut self, _index: Index, _cmd: Bytes) -> Result<u64, Self::Error> {
    self.count += 1;
    Ok(self.count)
  }

  fn snapshot(&self) -> Result<u64, Self::Error> {
    Ok(self.count)
  }

  fn restore(&mut self, snapshot: u64) -> Result<(), Self::Error> {
    self.count = snapshot;
    Ok(())
  }
}

/// A synchronous in-memory log honoring the normative `term`/`entries` domain contract.
#[derive(Default)]
pub struct MemLog {
  /// Entries above the compaction boundary; `entries[0]` is at `first`.
  entries: Vec<Entry>,
  /// The first retained index (boundary + 1).
  first: u64,
  /// The term at the compaction boundary (`first - 1`).
  boundary_term: Term,
  completions: VecDeque<LogDone>,
}

impl MemLog {
  pub fn new() -> Self {
    Self {
      entries: Vec::new(),
      first: 1,
      boundary_term: Term::ZERO,
      completions: VecDeque::new(),
    }
  }
}

impl LogStore for MemLog {
  type Error = std::convert::Infallible;

  fn first_index(&self) -> Index {
    Index::new(self.first)
  }

  fn last_index(&self) -> Index {
    Index::new(self.first + self.entries.len() as u64 - 1)
  }

  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    let i = index.get();
    // The normative domain: boundary → boundary term; in-range → entry term; anything else →
    // ZERO, never Err.
    if i + 1 == self.first {
      return Ok(self.boundary_term);
    }
    if i >= self.first && i < self.first + self.entries.len() as u64 {
      return Ok(self.entries[(i - self.first) as usize].term());
    }
    Ok(Term::ZERO)
  }

  fn entries(
    &self,
    range: std::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    let start = (range.start.get() - self.first) as usize;
    let end = (range.end.get() - self.first) as usize;
    let slice = &self.entries[start..end.min(self.entries.len())];
    // Honour the byte cap (roughly, always at least one entry when non-empty) — the contract a
    // real store implements, so the fixture exercises the proto's batching honestly.
    let mut budget = max_bytes;
    let mut count = 0usize;
    for e in slice {
      let sz = e.data().len() as u64;
      if count > 0 && sz > budget {
        break;
      }
      budget = budget.saturating_sub(sz);
      count += 1;
    }
    Ok(EntriesRead::Ready(MaybeOwned::Borrowed(&slice[..count])))
  }

  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if let Some(first_new) = entries.first() {
      // Truncate any conflicting suffix, then extend.
      let at = first_new.index().get();
      if at >= self.first {
        self.entries.truncate((at - self.first) as usize);
      }
      self.entries.extend_from_slice(entries);
    }
    // Synchronous: durable immediately; the completion is drained by the next poll.
    self.completions.push_back(LogDone::Appended(id));
  }

  fn compact(&mut self, up_to: Index) {
    let up = up_to.get();
    if up < self.first {
      return;
    }
    let keep_from = (up + 1 - self.first) as usize;
    self.boundary_term = self
      .entries
      .get(keep_from.saturating_sub(1))
      .map(|e| e.term())
      .unwrap_or(self.boundary_term);
    self.entries.drain(..keep_from.min(self.entries.len()));
    self.first = up + 1;
    self.completions.push_back(LogDone::Compacted(up_to));
  }

  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.entries.clear();
    self.first = last_index.get() + 1;
    self.boundary_term = last_term;
    // Stale completions for discarded indices must be dropped.
    self.completions.clear();
  }

  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    !self.completions.is_empty()
  }
}

/// A synchronous in-memory stable store (every write durable immediately, so the visible and
/// durable snapshot slots coincide).
pub struct MemStable {
  hs: HardState<u64>,
  snap: Option<(SnapshotMeta<u64>, Bytes)>,
  completions: VecDeque<StableDone>,
  /// Optional async-store signal: cloned from `DriverConfig::storage_ready`'s sender so the
  /// storage-ready seam can be exercised (signalled on every completion enqueue).
  ready: Option<flume::Sender<()>>,
  /// Chunked-snapshot staging accumulator (one in-flight transfer at a time).
  snapshot_staging: Option<(SnapshotMeta<u64>, sailing_proto::SnapshotStaging)>,
}

impl MemStable {
  pub fn new() -> Self {
    Self {
      hs: HardState::initial(),
      snap: None,
      completions: VecDeque::new(),
      ready: None,
      snapshot_staging: None,
    }
  }

  #[allow(dead_code)]
  pub fn with_ready(mut self, tx: flume::Sender<()>) -> Self {
    self.ready = Some(tx);
    self
  }

  fn signal(&self) {
    if let Some(tx) = &self.ready {
      let _ = tx.try_send(());
    }
  }
}

impl Default for MemStable {
  fn default() -> Self {
    Self::new()
  }
}

impl StableStore for MemStable {
  type NodeId = u64;
  type Error = std::convert::Infallible;

  fn hard_state(&self) -> HardState<u64> {
    self.hs
  }

  fn submit_write(&mut self, id: OpId, hard_state: HardState<u64>) {
    self.hs = hard_state;
    self.completions.push_back(StableDone::Wrote(id));
    self.signal();
  }

  fn submit_snapshot(&mut self, id: OpId, meta: SnapshotMeta<u64>, data: Bytes) {
    self.snap = Some((meta, data));
    self.completions.push_back(StableDone::SnapshotWritten(id));
    self.signal();
  }

  fn snapshot(&self) -> Option<(SnapshotMeta<u64>, Bytes)> {
    self.snap.clone()
  }

  fn durable_snapshot(&self) -> Option<SnapshotMeta<u64>> {
    // Synchronous store: submitted == durable.
    self.snap.as_ref().map(|(m, _)| m.clone())
  }

  #[allow(clippy::type_complexity)]
  fn snapshot_chunk(
    &self,
    offset: u64,
    len: u64,
  ) -> Option<Result<(SnapshotMeta<u64>, u64, SnapshotChunkRead), Self::Error>> {
    // Fully resident: slice the whole blob (an O(1) `Bytes` slice).
    self.resident_snapshot_chunk(offset, len)
  }

  fn accept_snapshot_chunk(
    &mut self,
    meta: &SnapshotMeta<u64>,
    total_len: u64,
    offset: u64,
    data: &Bytes,
  ) -> Result<u64, Self::Error> {
    let boundary = meta.last_index();
    match &self.snapshot_staging {
      Some((m, _)) if m.last_index() > boundary => return Ok(0),
      Some((m, s)) if !m.identity_eq(meta) || s.total_len() != total_len => {
        self.snapshot_staging = None
      }
      _ => {}
    }
    if self.snapshot_staging.is_none() {
      // A generous cap bounds a forged length without OOM; these test stores see only small snapshots,
      // so an over-cap None is unreachable — treat it as a no-op stage rather than panic.
      match sailing_proto::SnapshotStaging::new(boundary, total_len, 1 << 30) {
        Some(s) => self.snapshot_staging = Some((meta.clone(), s)),
        None => return Ok(0),
      }
    }
    Ok(
      self
        .snapshot_staging
        .as_mut()
        .expect("staging set above")
        .1
        .accept(offset, data)
        .expect("test store: small snapshots stay within MAX_STAGING_RUNS"),
    )
  }

  fn take_staged_snapshot(&mut self, meta: &SnapshotMeta<u64>) -> Option<Bytes> {
    let complete = matches!(
      &self.snapshot_staging,
      Some((m, s)) if m.identity_eq(meta) && s.is_complete()
    );
    complete.then(|| {
      let (_, s) = self
        .snapshot_staging
        .take()
        .expect("checked complete above");
      Bytes::from(s.into_vec())
    })
  }

  fn discard_snapshot_staging(&mut self) {
    self.snapshot_staging = None;
  }

  fn poll(&mut self) -> Option<Result<StableDone, Self::Error>> {
    self.completions.pop_front().map(Ok)
  }

  fn has_pending(&self) -> bool {
    !self.completions.is_empty()
  }
}

/// A log whose appends can be made to FAIL on demand — the poison trigger. A completion error
/// is a genuine storage fault, which the endpoint answers by fail-stopping (poisoning); the
/// driver must then fail everything parked with the typed verdict and exit.
pub struct PoisonableLog {
  inner: MemLog,
  fail_appends: std::sync::Arc<std::sync::atomic::AtomicBool>,
  failed: std::collections::VecDeque<std::io::Error>,
}

#[allow(dead_code)]
impl PoisonableLog {
  pub fn new() -> (Self, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    (
      Self {
        inner: MemLog::new(),
        fail_appends: flag.clone(),
        failed: std::collections::VecDeque::new(),
      },
      flag,
    )
  }
}

impl LogStore for PoisonableLog {
  type Error = std::io::Error;

  fn first_index(&self) -> Index {
    self.inner.first_index()
  }
  fn last_index(&self) -> Index {
    self.inner.last_index()
  }
  fn term(&self, index: Index) -> Result<Term, Self::Error> {
    Ok(self.inner.term(index).unwrap_or(Term::ZERO))
  }
  fn entries(
    &self,
    range: std::ops::Range<Index>,
    max_bytes: u64,
  ) -> Result<EntriesRead<'_>, Self::Error> {
    Ok(
      self
        .inner
        .entries(range, max_bytes)
        .unwrap_or(EntriesRead::Ready(MaybeOwned::Borrowed(&[]))),
    )
  }
  fn submit_append(&mut self, id: OpId, entries: &[Entry]) {
    if self.fail_appends.load(std::sync::atomic::Ordering::Acquire) {
      // The write "fails": queue an Err completion instead of applying anything.
      self
        .failed
        .push_back(std::io::Error::other("injected storage fault"));
      return;
    }
    self.inner.submit_append(id, entries);
  }
  fn compact(&mut self, up_to: Index) {
    self.inner.compact(up_to);
  }
  fn restore(&mut self, last_index: Index, last_term: Term) {
    self.inner.restore(last_index, last_term);
  }
  fn poll(&mut self) -> Option<Result<LogDone, Self::Error>> {
    if let Some(e) = self.failed.pop_front() {
      return Some(Err(e));
    }
    self.inner.poll().map(|r| r.map_err(|_| unreachable!()))
  }

  fn has_pending(&self) -> bool {
    // `poll()` yields the injected-error queue first, then the inner store's completions.
    !self.failed.is_empty() || self.inner.has_pending()
  }
}

/// A test cluster CA minting per-node certs with the SAN the QUIC coordinator's dial derives.
// Used by the QUIC integration tests; the stream tests share this module without it.
#[allow(dead_code)]
pub struct TestCa {
  ca_cert: rcgen::Certificate,
  issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

#[allow(dead_code)]
impl TestCa {
  pub fn new() -> Self {
    let mut params = rcgen::CertificateParams::new(Vec::new()).expect("CA params");
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.key_usages.push(rcgen::KeyUsagePurpose::KeyCertSign);
    let key = rcgen::KeyPair::generate().expect("CA key");
    let ca_cert = params.self_signed(&key).expect("self-signed CA");
    let issuer = rcgen::Issuer::new(
      rcgen::CertificateParams::new(Vec::new()).expect("issuer params"),
      key,
    );
    Self { ca_cert, issuer }
  }

  /// The QUIC options bundle for node `id` in `cluster`: cluster-private roots + this node's
  /// leaf cert, SAN'd as `node-<id-hex>.<cluster-hex>.sailing` so peers' derived dials verify.
  pub fn options(
    &self,
    id: u64,
    cluster: &sailing_proto::ClusterId,
  ) -> sailing_proto::quic::QuicOptions {
    use core::fmt::Write as _;
    let mut san = String::from("node-");
    let mut enc = Vec::new();
    sailing_proto::Data::encode(&id, &mut enc);
    for b in &enc {
      let _ = write!(san, "{b:02x}");
    }
    san.push('.');
    for b in &cluster.0 {
      let _ = write!(san, "{b:02x}");
    }
    san.push_str(".sailing");

    let mut params = rcgen::CertificateParams::new(vec![san]).expect("SAN params");
    params
      .key_usages
      .push(rcgen::KeyUsagePurpose::DigitalSignature);
    params
      .extended_key_usages
      .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    params
      .extended_key_usages
      .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);
    let key = rcgen::KeyPair::generate().expect("leaf key");
    let cert = params.signed_by(&key, &self.issuer).expect("leaf cert");

    let mut roots = rustls::RootCertStore::empty();
    roots
      .add(rustls::pki_types::CertificateDer::from(
        self.ca_cert.der().to_vec(),
      ))
      .expect("CA in roots");
    sailing_proto::quic::ClusterTls::new(
      roots,
      vec![rustls::pki_types::CertificateDer::from(cert.der().to_vec())],
      rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).expect("key DER"),
    )
    .build()
  }
}
