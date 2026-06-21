#![allow(missing_docs)]
//! Run every data-driven interaction scenario in `tests/interaction/*.txt`
//! against the proto via the harness, comparing each directive's rendered output against its
//! recorded golden. Regenerate the goldens with `SAILING_REWRITE=1 cargo test -p sailing-simulation
//! --test interaction`.

use sailing_simulation::run_interaction_file;
use std::path::PathBuf;

fn corpus_dir() -> PathBuf {
  PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/interaction")
}

#[test]
fn interaction_corpus() {
  let dir = corpus_dir();
  let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
    .unwrap_or_else(|e| panic!("cannot read interaction corpus dir {dir:?}: {e}"))
    .filter_map(Result::ok)
    .map(|e| e.path())
    .filter(|p| p.extension().is_some_and(|x| x == "txt"))
    .collect();
  files.sort();
  assert!(
    !files.is_empty(),
    "no interaction scenarios found in {dir:?}"
  );
  for f in &files {
    run_interaction_file(f);
  }
}
