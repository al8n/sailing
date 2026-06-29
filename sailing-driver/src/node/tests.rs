use super::*;

use std::net::SocketAddr;

use sailing_proto::CheapClone;

/// The borrowing accessors, `Display` (`id(addr)`), and the consuming `into_parts` — the surface the
/// drivers reach only through `into_parts`, so the rest needs its own coverage. Uses the drivers'
/// own `Node<I, SocketAddr>` shape.
#[test]
fn accessors_borrow_and_consume() {
  let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
  let node = Node::new(7u64, addr);
  assert_eq!(*node.id_ref(), 7);
  assert_eq!(*node.addr_ref(), addr);
  assert_eq!(format!("{node}"), format!("7({addr})"));
  assert_eq!(node.into_parts(), (7u64, addr));
}

/// `CheapClone` is a deep-but-cheap copy of both parts (both must themselves be `CheapClone`).
#[test]
fn cheap_clone_copies_both_parts() {
  let node = Node::new(7u64, 9000u64);
  let cloned = node.cheap_clone();
  assert_eq!(*cloned.id_ref(), 7);
  assert_eq!(*cloned.addr_ref(), 9000);
}
