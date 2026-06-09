//! Node identity. A blanket-impl marker so users never implement it by hand.
use core::{
  fmt::{Debug, Display},
  hash::Hash,
};

/// Marker for a Raft node identifier. Blanket-implemented for any type meeting the bounds.
///
/// `Data` (the wire-codec bound) is added as a supertrait in the `data` module's task.
pub trait NodeId: Copy + Ord + Hash + Debug + Display + 'static {}

impl<T> NodeId for T where T: Copy + Ord + Hash + Debug + Display + 'static {}

#[cfg(test)]
mod tests {
  use super::*;
  fn assert_node_id<T: NodeId>() {}
  #[test]
  fn u64_is_a_node_id() {
    assert_node_id::<u64>();
  }
}
