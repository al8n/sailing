use super::*;

/// A parsed directive argument: a key with zero-or-more values. A bare positional argument is
/// stored with an empty key; `key=val` is `(key, [val])`; `key=(a,b,c)` is `(key, [a,b,c])`.
#[derive(Debug, Clone)]
pub(crate) struct Arg {
  pub(crate) key: String,
  pub(crate) vals: Vec<String>,
}

/// The parsed form of a single directive line: the command name and its arguments.
#[derive(Debug, Clone)]
pub(crate) struct Directive {
  pub(crate) cmd: String,
  pub(crate) args: Vec<Arg>,
}

impl Directive {
  /// Parse a directive line like `add-nodes 3 voters=(1,2,3) index=2 prevote=true`.
  pub(crate) fn parse(line: &str) -> Self {
    let mut toks = tokenize(line);
    let cmd = toks.next().unwrap_or_default();
    let mut args = Vec::new();
    for tok in toks {
      if let Some((k, v)) = tok.split_once('=') {
        let vals = if let Some(inner) = v.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
          inner
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect()
        } else {
          std::vec![v.to_string()]
        };
        args.push(Arg {
          key: k.to_string(),
          vals,
        });
      } else {
        args.push(Arg {
          key: String::new(),
          vals: std::vec![tok],
        });
      }
    }
    Self { cmd, args }
  }

  /// The `i`-th positional (keyless) argument, if present.
  pub(crate) fn positional(&self, i: usize) -> Option<&str> {
    self
      .args
      .iter()
      .filter(|a| a.key.is_empty())
      .nth(i)
      .and_then(|a| a.vals.first())
      .map(String::as_str)
  }

  /// All values of the named argument (e.g. `voters`), or empty if absent.
  pub(crate) fn values(&self, key: &str) -> &[String] {
    self
      .args
      .iter()
      .find(|a| a.key == key)
      .map(|a| a.vals.as_slice())
      .unwrap_or(&[])
  }

  /// The single value of the named argument, parsed as `T`.
  pub(crate) fn value<T: core::str::FromStr>(&self, key: &str) -> Option<T> {
    self.values(key).first().and_then(|v| v.parse().ok())
  }

  /// A boolean flag: `key` (bare) or `key=true` ⇒ true; absent ⇒ false.
  pub(crate) fn flag(&self, key: &str) -> bool {
    self
      .args
      .iter()
      .any(|a| a.key == key && a.vals.first().map(String::as_str) != Some("false"))
  }
}

/// Split a directive line into whitespace-separated tokens, keeping `(...)` groups intact so a
/// `voters=(1, 2, 3)` value is not split on its inner spaces.
pub(crate) fn tokenize(line: &str) -> std::vec::IntoIter<String> {
  let mut out = Vec::new();
  let mut cur = String::new();
  let mut depth = 0i32;
  for ch in line.trim().chars() {
    match ch {
      '(' => {
        depth += 1;
        cur.push(ch);
      }
      ')' => {
        depth -= 1;
        cur.push(ch);
      }
      c if c.is_whitespace() && depth == 0 => {
        if !cur.is_empty() {
          out.push(core::mem::take(&mut cur));
        }
      }
      c => cur.push(c),
    }
  }
  if !cur.is_empty() {
    out.push(cur);
  }
  out.into_iter()
}

/// One parsed `command / ---- / expected-output` block, with the comment/blank lines that preceded
/// it (preserved verbatim so a rewrite round-trips the file's documentation).
pub(crate) struct Block {
  pub(crate) comments: Vec<String>,
  pub(crate) command: String,
  pub(crate) expected: Vec<String>,
}

/// Parse a data-driven file into its blocks. The format (etcd's `datadriven`, simple variant): any
/// run of `#`-comment / blank lines, then a single-line `command args`, then a `----` line, then the
/// expected-output lines up to the next blank line (or EOF). The harness never emits blank lines
/// inside a block's output, so the simple single-`----` delimiter always suffices.
pub(crate) fn parse_blocks(content: &str) -> Vec<Block> {
  let mut blocks = Vec::new();
  let mut comments = Vec::new();
  let mut lines = content.lines().peekable();
  while let Some(line) = lines.next() {
    let trimmed = line.trim();
    if trimmed.is_empty() {
      // Blank lines are structural separators; the emitter re-inserts exactly one between blocks,
      // so we drop them here rather than carrying them as "comments" (which would double up).
      continue;
    }
    if trimmed.starts_with('#') {
      comments.push(line.to_string());
      continue;
    }
    let command = line.to_string();
    // The next line should be the `----` separator; consume it if present.
    if lines.peek().map(|l| l.trim()) == Some("----") {
      lines.next();
    }
    let mut expected = Vec::new();
    while let Some(l) = lines.peek() {
      if l.trim().is_empty() {
        break;
      }
      expected.push(lines.next().unwrap().to_string());
    }
    blocks.push(Block {
      comments: core::mem::take(&mut comments),
      command,
      expected,
    });
  }
  blocks
}
