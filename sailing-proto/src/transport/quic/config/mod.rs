//! Build a [`QuicOptions`] from cert / key / CA files on disk.
//!
//! [`ClusterTls`](super::ClusterTls) takes already-parsed DER (`RootCertStore` + chain + key) and
//! makes the security-policy choice (mandatory mTLS over a cluster-private CA). This module is the
//! `serde`/`clap`-able convenience layer a CLI / daemon uses to construct that bundle from PEM
//! files plus a few tunables — exactly how the `tls`/`quic` story separates the serde-able SOURCE
//! config from the non-serde-able built crypto bundle.
//!
//! # Fully fallible
//!
//! [`QuicConfigOptions::build`] reads three PEM files and returns a [`QuicConfigError`] for any I/O
//! or PEM-parse failure (a missing/unreadable file, a file with no certificate, a key file with no
//! private key). It then hands the parsed DER to
//! [`ClusterTls::try_build`](super::ClusterTls::try_build), whose mandatory-mTLS assembly returns a
//! recoverable error — surfaced here as [`QuicConfigError::Tls`] — when the bundle is invalid (an
//! empty root store, a key that does not match the leaf, TLS 1.3 unsupported by the linked
//! provider). A mismatched cert/key is an ordinary cert-rotation mistake, so the WHOLE PEM→bundle
//! path is recoverable; nothing in `build` panics. (The infallible-looking
//! [`ClusterTls::build`](super::ClusterTls::build) still panics on a bad bundle, for callers that
//! construct the DER bundle by hand and treat a misconfig as a programming error.)
//!
//! # Crypto provider
//!
//! Like [`ClusterTls`](super::ClusterTls), the built configs use the rustls provider selected by
//! the `tls-*` features (`ring` by default under `quic`); there is no process-default-provider
//! requirement.

use std::path::{Path, PathBuf};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::{
  ClusterTls, ClusterTlsError, QuicOptions, QuicTuning, crypto::DEFAULT_MAX_CONNECTIONS,
};

#[cfg(feature = "serde")]
const fn default_max_connections() -> usize {
  DEFAULT_MAX_CONNECTIONS
}

/// Construct a [`QuicOptions`] from this node's cert / key / CA PEM files on disk plus a few
/// tunables, then [`build`](Self::build) the mandatory-mTLS bundle.
///
/// `cert_file`, `key_file`, and `ca_file` are REQUIRED PEM paths — there is no default, so a config
/// that omits one is a deserialize error. `tuning` carries the QUIC timer/window knobs, and
/// `max_connections` caps the live-connection table.
///
/// Sailing's QUIC is mutual-mTLS-ALWAYS over the cluster CA, so — unlike memberlist's QUIC config —
/// there is no client-authentication-mode knob: the authentication model is fixed by
/// [`ClusterTls`](super::ClusterTls).
///
/// `clap` parses this through a private mirror: the three required path fields go directly,
/// `max_connections` carries a `default_value_t`, and `tuning` is `command(flatten)`ed. The
/// hand-written [`FromArgMatches`](clap::FromArgMatches) UPDATE applies only command-line / env
/// values (a `value_source` gate), so a partial `try_update_from` preserves every un-flagged field —
/// it never resets `max_connections` or a non-default (WAN/large-window) `tuning` back to its
/// default the way a bare derived update would.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct QuicConfigOptions {
  /// Path to the PEM file holding this node's certificate chain (leaf first).
  cert_file: PathBuf,
  /// Path to the PEM file holding this node's private key.
  key_file: PathBuf,
  /// Path to the PEM file holding the cluster CA certificate(s).
  ca_file: PathBuf,
  /// QUIC transport timer/window tuning. Defaults to the LAN-tuned [`QuicTuning::new`]; parses
  /// through the same clamping path as a programmatic builder (see [`QuicTuning`]).
  #[cfg_attr(feature = "serde", serde(default))]
  tuning: QuicTuning,
  /// Cap on the number of live connections the bridge holds at once (see
  /// [`QuicOptions::max_connections`]). Defaults to the pinned `DEFAULT_MAX_CONNECTIONS`.
  #[cfg_attr(feature = "serde", serde(default = "default_max_connections"))]
  max_connections: usize,
}

impl QuicConfigOptions {
  /// Construct from the three required PEM file paths, with the default [`QuicTuning`] and default
  /// connection cap.
  pub fn new(cert_file: PathBuf, key_file: PathBuf, ca_file: PathBuf) -> Self {
    Self {
      cert_file,
      key_file,
      ca_file,
      tuning: QuicTuning::new(),
      max_connections: DEFAULT_MAX_CONNECTIONS,
    }
  }

  /// Builder: set the QUIC transport tuning.
  #[must_use]
  pub const fn with_tuning(mut self, tuning: QuicTuning) -> Self {
    self.tuning = tuning;
    self
  }

  /// Builder: set the live-connection cap (see [`QuicOptions::max_connections`]).
  #[must_use]
  pub const fn with_max_connections(mut self, max: usize) -> Self {
    self.max_connections = max;
    self
  }

  /// The configured certificate-chain file path.
  #[inline(always)]
  pub fn cert_file(&self) -> &PathBuf {
    &self.cert_file
  }

  /// The configured private-key file path.
  #[inline(always)]
  pub fn key_file(&self) -> &PathBuf {
    &self.key_file
  }

  /// The configured CA-certificate file path.
  #[inline(always)]
  pub fn ca_file(&self) -> &PathBuf {
    &self.ca_file
  }

  /// The configured QUIC transport tuning.
  #[inline(always)]
  pub const fn tuning(&self) -> &QuicTuning {
    &self.tuning
  }

  /// The configured live-connection cap.
  #[inline(always)]
  pub const fn max_connections(&self) -> usize {
    self.max_connections
  }

  /// Load the three PEM files and assemble a [`QuicOptions`] with mandatory mutual TLS over the
  /// cluster CA, the configured [`QuicTuning`], and connection cap.
  ///
  /// The certificate chain is parsed from `cert_file`, the private key from `key_file`, and the CA
  /// certificate(s) from `ca_file` into a `RootCertStore`. The whole path is fully fallible: this
  /// `Result` covers file/PEM failures AND an invalid cluster-CA bundle (a mismatched cert/key, an
  /// invalid leaf, a provider without TLS 1.3), the latter via [`QuicConfigError::Tls`] from
  /// [`ClusterTls::try_build`](super::ClusterTls::try_build). A mismatched cert/key is an ordinary
  /// cert-rotation mistake, so it is a recoverable `Err` here rather than a process panic.
  pub fn build(&self) -> Result<QuicOptions, QuicConfigError> {
    let chain = load_certs(&self.cert_file)?;
    let key = load_private_key(&self.key_file)?;
    let roots = load_roots(&self.ca_file)?;

    Ok(
      ClusterTls::new(roots, chain, key)
        .tuning(self.tuning)
        .try_build()?
        .with_max_connections(self.max_connections),
    )
  }
}

/// Clap-only parse mirror for [`QuicConfigOptions`]. NOT part of the public API — it carries the clap
/// `Args` derive and the per-field `#[arg(...)]` / `#[command(flatten)]` attributes; the public struct
/// keeps only the `serde` derive. Splitting the clap derive off is what lets [`QuicConfigOptions`]
/// hand-write a `value_source`-gated UPDATE (preserving un-flagged fields) instead of inheriting the
/// derive's reset-omitted-fields behavior.
#[cfg(feature = "clap")]
#[derive(clap::Args)]
struct QuicConfigOptionsCli {
  #[arg(
    id = "quic-cert-file",
    long = "cert-file",
    env = "SAILING_QUIC_CERT_FILE"
  )]
  cert_file: PathBuf,
  #[arg(id = "quic-key-file", long = "key-file", env = "SAILING_QUIC_KEY_FILE")]
  key_file: PathBuf,
  #[arg(id = "quic-ca-file", long = "ca-file", env = "SAILING_QUIC_CA_FILE")]
  ca_file: PathBuf,
  #[command(flatten)]
  tuning: QuicTuning,
  #[arg(
    id = "quic-max-connections",
    long = "quic-max-connections",
    env = "SAILING_QUIC_MAX_CONNECTIONS",
    default_value_t = DEFAULT_MAX_CONNECTIONS
  )]
  max_connections: usize,
}

#[cfg(feature = "clap")]
impl From<QuicConfigOptionsCli> for QuicConfigOptions {
  fn from(c: QuicConfigOptionsCli) -> Self {
    Self {
      cert_file: c.cert_file,
      key_file: c.key_file,
      ca_file: c.ca_file,
      tuning: c.tuning,
      max_connections: c.max_connections,
    }
  }
}

#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  impl Args for QuicConfigOptions {
    fn augment_args(cmd: Command) -> Command {
      QuicConfigOptionsCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      QuicConfigOptionsCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for QuicConfigOptions {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      QuicConfigOptionsCli::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the command line or an
      // env var, not a clap default. A bare derived update treats the `default_value_t`
      // `max_connections` (and every flattened-`tuning` default) as present and would reset the
      // un-flagged fields back to their defaults, silently shrinking a non-default config on a
      // partial reload. The three required path fields have no default, so an absent one is already
      // `value_source` `None` and naturally preserved.
      macro_rules! take {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(v) = m.get_one::<$ty>($id) {
              self.$field = v.clone();
            }
          }
        };
      }
      take!("quic-cert-file", cert_file, PathBuf);
      take!("quic-key-file", key_file, PathBuf);
      take!("quic-ca-file", ca_file, PathBuf);
      take!("quic-max-connections", max_connections, usize);
      // The flattened `QuicTuning` carries the SAME value-source-gated update (its args appear
      // directly in `m` under the flatten), so an un-flagged tuning knob is preserved too.
      super::crypto::update_quic_tuning(&mut self.tuning, m);
      Ok(())
    }
  }
};

/// Parse the certificate chain (leaf first) from a PEM file. Errors if the file is unreadable or
/// holds no certificate.
fn load_certs(path: &Path) -> Result<std::vec::Vec<CertificateDer<'static>>, QuicConfigError> {
  let bytes = std::fs::read(path).map_err(|e| QuicConfigError::read(path, e))?;
  let certs = rustls_pemfile::certs(&mut &bytes[..])
    .collect::<Result<std::vec::Vec<_>, _>>()
    .map_err(|e| QuicConfigError::read(path, e))?;
  if certs.is_empty() {
    return Err(QuicConfigError::NoCerts(path.to_path_buf()));
  }
  Ok(certs)
}

/// Parse the first private key from a PEM file. Errors if the file is unreadable or holds no key.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, QuicConfigError> {
  let bytes = std::fs::read(path).map_err(|e| QuicConfigError::read(path, e))?;
  rustls_pemfile::private_key(&mut &bytes[..])
    .map_err(|e| QuicConfigError::read(path, e))?
    .ok_or_else(|| QuicConfigError::NoKey(path.to_path_buf()))
}

/// Parse the CA certificate(s) from a PEM file into a `RootCertStore`. Errors if the file is
/// unreadable, holds no certificate, or a cert is not a valid trust anchor.
fn load_roots(path: &Path) -> Result<rustls::RootCertStore, QuicConfigError> {
  let certs = load_certs(path)?;
  let mut roots = rustls::RootCertStore::empty();
  for cert in certs {
    roots
      .add(cert)
      .map_err(|e| QuicConfigError::Anchor(path.to_path_buf(), e))?;
  }
  Ok(roots)
}

/// A failure building a [`QuicOptions`] in [`QuicConfigOptions::build`].
///
/// Covers the whole PEM→bundle path: the FILE/PARSE failures (a missing/unreadable file, a file
/// with no certificate, a key file with no key, a CA cert that is not a valid trust anchor) AND an
/// invalid cluster-CA bundle ([`Self::Tls`]) — a mismatched cert/key, an invalid leaf, a provider
/// without TLS 1.3 — surfaced from [`ClusterTls::try_build`](super::ClusterTls::try_build). The
/// whole path is recoverable; nothing here panics.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum QuicConfigError {
  /// Reading or PEM-parsing one of the cert / key / CA files failed.
  #[error("failed to read {path}: {source}")]
  Io {
    /// The file that could not be read or parsed.
    path: PathBuf,
    /// The underlying I/O / PEM-parse error.
    source: std::io::Error,
  },
  /// The certificate (or CA) file parsed but held no certificate.
  #[error("no certificates found in {0}")]
  NoCerts(PathBuf),
  /// The key file parsed but held no private key.
  #[error("no private key found in {0}")]
  NoKey(PathBuf),
  /// A CA certificate could not be added to the root store as a trust anchor.
  #[error("invalid CA certificate in {0}: {1}")]
  Anchor(PathBuf, rustls::Error),
  /// The PEM files parsed, but assembling the mandatory-mTLS bundle from them failed — a
  /// mismatched cert/key, an invalid leaf, an empty root store, or no TLS 1.3 (see
  /// [`ClusterTlsError`]).
  #[error("invalid cluster TLS bundle: {0}")]
  Tls(#[from] ClusterTlsError),
}

impl QuicConfigError {
  /// A read / PEM-parse failure carrying the offending path.
  fn read(path: &Path, source: std::io::Error) -> Self {
    Self::Io {
      path: path.to_path_buf(),
      source,
    }
  }
}

#[cfg(test)]
mod tests;
