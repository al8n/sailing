//! Group identity. A blanket-impl marker, mirroring [`NodeId`](crate::NodeId).
use crate::Data;
use cheap_clone::CheapClone;
use core::{
  fmt::{Debug, Display},
  hash::Hash,
};

/// Marker for a Raft group identifier — the demux key that selects one group within a
/// [`MultiRaft`](crate::MultiRaft) host. Blanket-implemented for any type meeting the bounds.
///
/// The bounds mirror [`NodeId`](crate::NodeId): [`Data`] so the id can be tagged onto the wire
/// frame (the group-demux envelope) and folded into each group's election seed, [`CheapClone`]
/// because ids are cloned on every output drain, and [`Ord`] because ids key a `BTreeMap`. `u64`
/// satisfies it out of the box; a custom id implements [`CheapClone`] the same one-line way a
/// custom [`NodeId`](crate::NodeId) does.
pub trait GroupId: Data + CheapClone + Ord + Hash + Debug + Display + 'static {}

impl<T> GroupId for T where T: Data + CheapClone + Ord + Hash + Debug + Display + 'static {}
