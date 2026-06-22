use std::{
  path::PathBuf,
  sync::atomic::{AtomicU64, Ordering},
};

use super::{QuicConfigError, QuicConfigOptions};
use crate::transport::quic::{QuicTuning, crypto::tests::TestClusterCa};

/// A unique temp directory for one test's PEM files. Avoids a `tempfile` dev-dep: a
/// process-pid + monotonic-counter suffix keeps concurrent test threads from colliding, and
/// [`TempPem::drop`] removes the directory.
struct TempPem {
  dir: PathBuf,
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

impl TempPem {
  fn new() -> Self {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir =
      std::env::temp_dir().join(format!("sailing-quic-config-{}-{}", std::process::id(), n));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    Self { dir }
  }

  /// Write `contents` to `name` under the temp dir and return the path.
  fn write(&self, name: &str, contents: &str) -> PathBuf {
    let path = self.dir.join(name);
    std::fs::write(&path, contents).expect("write temp PEM");
    path
  }

  /// A path under the temp dir that is NOT created (for the missing-file test).
  fn missing(&self, name: &str) -> PathBuf {
    self.dir.join(name)
  }
}

impl Drop for TempPem {
  fn drop(&mut self) {
    let _ = std::fs::remove_dir_all(&self.dir);
  }
}

/// Mint a CA + one node cert, write all three PEM files, and return the temp dir plus a
/// [`QuicConfigOptions`] pointing at them.
fn options_for_real_certs() -> (TempPem, QuicConfigOptions) {
  let ca = TestClusterCa::generate();
  let node = ca.issue_node("node-01.00000000000000000000000000000000.sailing");

  let tmp = TempPem::new();
  let cert_file = tmp.write("cert.pem", &node.cert.pem());
  let key_file = tmp.write("key.pem", &node.key.serialize_pem());
  let ca_file = tmp.write("ca.pem", &ca.ca_cert_pem());

  let opts = QuicConfigOptions::new(cert_file, key_file, ca_file);
  (tmp, opts)
}

#[test]
fn build_round_trips_to_mutual_auth_options() {
  let (_tmp, opts) = options_for_real_certs();
  let built = opts.build().expect("build from real PEM succeeds");
  // Sailing's QUIC is mutual-mTLS-always: the built bundle carries both directions and the
  // mandatory client-auth flag.
  assert!(
    built.requires_client_auth(),
    "the from-files build pins mandatory mTLS"
  );
  assert!(built.client_config().is_some(), "dial config present");
  assert!(built.server_config().is_some(), "accept config present");
}

#[test]
fn build_threads_max_connections() {
  let (_tmp, opts) = options_for_real_certs();
  let opts = opts.with_max_connections(7);
  let built = opts.build().expect("build succeeds");
  assert_eq!(built.max_connections(), 7);
}

#[test]
fn build_missing_file_is_err_not_panic() {
  let tmp = TempPem::new();
  // None of the three files exist under the fresh temp dir.
  let opts = QuicConfigOptions::new(
    tmp.missing("cert.pem"),
    tmp.missing("key.pem"),
    tmp.missing("ca.pem"),
  );
  match opts.build() {
    Err(QuicConfigError::Io { .. }) => {}
    other => panic!(
      "a missing file must surface as the Io variant (not a panic), got {:?}",
      other.err()
    ),
  }
}

#[test]
fn build_empty_cert_file_is_no_certs() {
  let ca = TestClusterCa::generate();
  let node = ca.issue_node("node-01.00000000000000000000000000000000.sailing");
  let tmp = TempPem::new();
  // A syntactically empty (no PEM block) cert file → NoCerts, still an Err, never a panic.
  let cert_file = tmp.write("cert.pem", "");
  let key_file = tmp.write("key.pem", &node.key.serialize_pem());
  let ca_file = tmp.write("ca.pem", &ca.ca_cert_pem());
  match QuicConfigOptions::new(cert_file, key_file, ca_file).build() {
    Err(QuicConfigError::NoCerts(_)) => {}
    other => panic!("an empty cert file must be NoCerts, got {:?}", other.err()),
  }
}

#[test]
fn build_mismatched_cert_and_key_is_tls_err_not_panic() {
  // A syntactically valid PEM set whose cert and key DO NOT match: mint two distinct leaves off
  // the same test CA and pair leaf-A's certificate with leaf-B's private key. rustls'
  // `with_single_cert` rejects the mismatched SubjectPublicKeyInfo — an ordinary cert-rotation
  // mistake that MUST be a recoverable `Err`, not a process panic.
  let ca = TestClusterCa::generate();
  let leaf_a = ca.issue_node("node-0a.00000000000000000000000000000000.sailing");
  let leaf_b = ca.issue_node("node-0b.00000000000000000000000000000000.sailing");

  let tmp = TempPem::new();
  let cert_file = tmp.write("cert.pem", &leaf_a.cert.pem());
  // The DELIBERATELY mismatched key: leaf B's private key paired with leaf A's certificate.
  let key_file = tmp.write("key.pem", &leaf_b.key.serialize_pem());
  let ca_file = tmp.write("ca.pem", &ca.ca_cert_pem());

  match QuicConfigOptions::new(cert_file, key_file, ca_file).build() {
    Err(QuicConfigError::Tls(_)) => {}
    other => panic!(
      "a mismatched cert/key must surface as the Tls variant (not a panic), got {:?}",
      other.err()
    ),
  }
}

#[test]
fn accessors_and_builders() {
  let opts = QuicConfigOptions::new(
    PathBuf::from("/c.pem"),
    PathBuf::from("/k.pem"),
    PathBuf::from("/ca.pem"),
  );
  assert_eq!(opts.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(opts.key_file(), &PathBuf::from("/k.pem"));
  assert_eq!(opts.ca_file(), &PathBuf::from("/ca.pem"));
  // Defaults.
  assert_eq!(opts.tuning(), &QuicTuning::new());
  assert_eq!(opts.max_connections(), super::DEFAULT_MAX_CONNECTIONS);

  let opts = opts
    .with_max_connections(9)
    .with_tuning(QuicTuning::new().with_idle_timeout_millis(9_000));
  assert_eq!(opts.max_connections(), 9);
  assert_eq!(opts.tuning().idle_timeout_millis(), 9_000);
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trips_full_config() {
  let opts = QuicConfigOptions::new(
    PathBuf::from("/etc/sailing/cert.pem"),
    PathBuf::from("/etc/sailing/key.pem"),
    PathBuf::from("/etc/sailing/ca.pem"),
  )
  .with_max_connections(128)
  .with_tuning(
    QuicTuning::new()
      .with_idle_timeout_millis(9_000)
      .with_keep_alive_interval_millis(250),
  );
  let json = serde_json::to_string(&opts).unwrap();
  let back: QuicConfigOptions = serde_json::from_str(&json).unwrap();
  assert_eq!(back, opts);
}

#[cfg(feature = "serde")]
#[test]
fn serde_partial_fills_defaults() {
  // Only the three required paths → tuning / max_connections at defaults.
  let json = r#"{
    "cert_file": "/c.pem",
    "key_file": "/k.pem",
    "ca_file": "/ca.pem"
  }"#;
  let opts: QuicConfigOptions = serde_json::from_str(json).unwrap();
  assert_eq!(opts.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(opts.tuning(), &QuicTuning::new());
  assert_eq!(opts.max_connections(), super::DEFAULT_MAX_CONNECTIONS);
}

#[cfg(feature = "serde")]
#[test]
fn serde_missing_required_path_is_err() {
  // A required path absent (no serde default) is a deserialize error.
  let json = r#"{ "cert_file": "/c.pem", "key_file": "/k.pem" }"#;
  assert!(
    serde_json::from_str::<QuicConfigOptions>(json).is_err(),
    "ca_file has no default, so omitting it must fail"
  );
}

#[cfg(feature = "serde")]
#[test]
fn serde_rejects_unknown_field() {
  let json = r#"{
    "cert_file": "/c.pem",
    "key_file": "/k.pem",
    "ca_file": "/ca.pem",
    "ca_fil": "/typo.pem"
  }"#;
  assert!(
    serde_json::from_str::<QuicConfigOptions>(json).is_err(),
    "deny_unknown_fields rejects a typo'd key"
  );
}

#[cfg(feature = "clap")]
#[test]
fn clap_parses_paths_tuning_and_cap() {
  use clap::Parser;
  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  let cli = Cli::try_parse_from([
    "app",
    "--cert-file",
    "/c.pem",
    "--key-file",
    "/k.pem",
    "--ca-file",
    "/ca.pem",
    "--quic-max-connections",
    "256",
    "--quic-idle-timeout-millis",
    "9000",
  ])
  .unwrap();
  assert_eq!(cli.quic.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(cli.quic.key_file(), &PathBuf::from("/k.pem"));
  assert_eq!(cli.quic.ca_file(), &PathBuf::from("/ca.pem"));
  assert_eq!(cli.quic.max_connections(), 256);
  assert_eq!(cli.quic.tuning().idle_timeout_millis(), 9_000);
}

#[cfg(feature = "clap")]
#[test]
fn clap_defaults_when_only_paths_given() {
  use clap::Parser;
  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  let cli = Cli::try_parse_from([
    "app",
    "--cert-file",
    "/c.pem",
    "--key-file",
    "/k.pem",
    "--ca-file",
    "/ca.pem",
  ])
  .unwrap();
  assert_eq!(cli.quic.max_connections(), super::DEFAULT_MAX_CONNECTIONS);
  assert_eq!(cli.quic.tuning(), &QuicTuning::new());
}

#[cfg(feature = "clap")]
#[test]
fn clap_requires_the_three_paths() {
  use clap::Parser;
  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }
  // The three file paths are required posit-less options: omitting one fails the parse.
  assert!(
    Cli::try_parse_from(["app", "--cert-file", "/c.pem", "--key-file", "/k.pem"]).is_err(),
    "ca-file is required"
  );
}

#[cfg(feature = "clap")]
#[test]
fn clap_update_preserves_omitted_non_default_fields() {
  use clap::Parser;
  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }

  // A base carrying NON-default values across the cap AND the flattened tuning, plus the three
  // required paths.
  let base = QuicConfigOptions::new(
    PathBuf::from("/c.pem"),
    PathBuf::from("/k.pem"),
    PathBuf::from("/ca.pem"),
  )
  .with_max_connections(256)
  .with_tuning(
    QuicTuning::new()
      .with_idle_timeout_millis(9_000)
      .with_connection_receive_window(32 * 1024 * 1024),
  );

  // Update supplying EXACTLY ONE flag — the cap. A bare derived update would reset the flattened
  // tuning knobs (and re-default any other `default_value_t` field) to their defaults; the
  // value_source gate must preserve them. The required paths (no default) are preserved too.
  let mut cli = Cli { quic: base.clone() };
  cli
    .try_update_from(["app", "--quic-max-connections", "512"])
    .unwrap();
  assert_eq!(cli.quic.max_connections(), 512);
  // Falsifying assertions: these fail if the gate is removed and the flattened tuning is reset.
  assert_eq!(cli.quic.tuning().idle_timeout_millis(), 9_000);
  assert_eq!(
    cli.quic.tuning().connection_receive_window(),
    32 * 1024 * 1024
  );
  // The required paths are preserved across the partial update.
  assert_eq!(cli.quic.cert_file(), &PathBuf::from("/c.pem"));
  assert_eq!(cli.quic.key_file(), &PathBuf::from("/k.pem"));
  assert_eq!(cli.quic.ca_file(), &PathBuf::from("/ca.pem"));

  // Update supplying ONLY a flattened-tuning flag: the cap (a `default_value_t` field) must be
  // preserved, not reset to DEFAULT_MAX_CONNECTIONS.
  let mut cli = Cli { quic: base.clone() };
  cli
    .try_update_from(["app", "--quic-idle-timeout-millis", "12000"])
    .unwrap();
  assert_eq!(cli.quic.tuning().idle_timeout_millis(), 12_000);
  assert_eq!(
    cli.quic.max_connections(),
    256,
    "the un-flagged cap must be preserved, not reset to the default"
  );
  // The OTHER un-flagged tuning knob is preserved as well.
  assert_eq!(
    cli.quic.tuning().connection_receive_window(),
    32 * 1024 * 1024
  );
}

#[cfg(feature = "clap")]
#[test]
fn clap_env_is_wired() {
  use clap::CommandFactory;
  #[derive(clap::Parser)]
  struct Cli {
    #[command(flatten)]
    quic: QuicConfigOptions,
  }
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "quic-cert-file")
    .unwrap();
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("SAILING_QUIC_CERT_FILE")
  );
}
