#![allow(missing_docs)]
//! Run every multi-group data-driven scenario in `tests/multi_interaction/*.txt` against
//! `MultiRaft` containers via the multi interaction harness, comparing each directive's rendered
//! output against its recorded golden. Regenerate with `SAILING_REWRITE=1 cargo test -p
//! sailing-simulation --test multi_interaction`.

use sailing_simulation::run_multi_interaction_file;
use std::path::PathBuf;

fn corpus_dir() -> PathBuf {
  PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/multi_interaction")
}

#[test]
fn multi_interaction_corpus() {
  let dir = corpus_dir();
  let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
    .unwrap_or_else(|e| panic!("cannot read multi interaction corpus dir {dir:?}: {e}"))
    .filter_map(Result::ok)
    .map(|e| e.path())
    .filter(|p| p.extension().is_some_and(|x| x == "txt"))
    .collect();
  files.sort();
  assert!(
    !files.is_empty(),
    "no multi interaction scenarios found in {dir:?}"
  );
  for f in &files {
    run_multi_interaction_file(f);
  }
}
