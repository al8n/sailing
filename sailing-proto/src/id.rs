//! Node identity. A blanket-impl marker so users never implement it by hand.
use crate::Data;
use cheap_clone::CheapClone;
use core::{
  fmt::{Debug, Display},
  hash::Hash,
};

/// Marker for a Raft node identifier. Blanket-implemented for any type meeting the bounds.
///
/// Identifiers are cloned, not copied: the bound is [`CheapClone`] rather than [`Copy`], so an
/// embedder can use an `Arc<str>`/`Arc<[u8]>`, a string-backed, a UUID-backed, or any custom id
/// whose clone is O(1). `u64: CheapClone` with `cheap_clone() == *self`, so the built-in numeric
/// id stays behaviour-identical. `Ord` is required because ids are `BTreeMap` keys.
///
/// A **custom** id type must implement [`CheapClone`] explicitly — even one deriving [`Copy`] —
/// because [`CheapClone`] is not blanket-implemented for `Copy`: the crate covers the primitives,
/// `Arc`, and `Rc`, but not arbitrary `Copy` newtypes. The impl is one line taking the default
/// `cheap_clone()` = `clone()`: `impl CheapClone for MyId {}`.
pub trait NodeId: Data + CheapClone + Ord + Hash + Debug + Display + 'static {}

impl<T> NodeId for T where T: Data + CheapClone + Ord + Hash + Debug + Display + 'static {}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{ByteCursor, DecodeError};
  use bytes::Bytes;
  use std::{sync::Arc, vec::Vec};

  fn assert_node_id<T: NodeId>() {}

  #[test]
  fn u64_is_a_node_id() {
    assert_node_id::<u64>();
  }

  /// A non-`Copy`, `Arc<str>`-backed id: the proof that the relaxed bound admits a
  /// reference-counted identifier (its `Clone`, hence `cheap_clone`, is O(1)).
  #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
  struct StrId(Arc<str>);

  impl Display for StrId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
      Display::fmt(&self.0, f)
    }
  }

  // `Arc<str>` is not `Copy`, so this default-bodied impl (clone) is the only way to satisfy
  // `CheapClone` — exactly the case the trait was relaxed to admit.
  impl CheapClone for StrId {}

  impl Data for StrId {
    fn encode(&self, buf: &mut Vec<u8>) {
      Bytes::copy_from_slice(self.0.as_bytes()).encode(buf);
    }
    fn decode(cur: &mut ByteCursor) -> Result<Self, DecodeError> {
      let bytes = Bytes::decode(cur)?;
      let s = core::str::from_utf8(&bytes).map_err(|_| DecodeError::Invalid("StrId utf8"))?;
      Ok(StrId(Arc::from(s)))
    }
  }

  #[test]
  fn non_copy_str_id_is_a_node_id() {
    assert_node_id::<StrId>();
    // Round-trips through the wire codec like any other id.
    let id = StrId(Arc::from("node-7"));
    let mut buf = Vec::new();
    id.encode(&mut buf);
    let got = StrId::decode_exact(Bytes::from(buf)).unwrap();
    assert_eq!(id, got);
    // `cheap_clone` yields the same value (O(1) refcount bump for the `Arc`).
    assert_eq!(id, id.cheap_clone());
  }
}
