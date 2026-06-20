use super::*;

#[test]
fn borrowed_derefs_and_predicates() {
  let v = std::vec![1u32, 2, 3];
  let mo: MaybeOwned<'_, [u32]> = (&v[..]).into();
  assert!(mo.is_borrowed() && !mo.is_owned());
  assert_eq!(&*mo, &[1, 2, 3]);
  assert_eq!(mo.iter().sum::<u32>(), 6);
}

#[test]
fn owned_from_vec_derefs_and_predicates() {
  let mo: MaybeOwned<'_, [u32]> = std::vec![4u32, 5].into();
  assert!(mo.is_owned() && !mo.is_borrowed());
  assert_eq!(&*mo, &[4, 5]);
}

#[test]
fn into_vec_clones_borrowed_and_unboxes_owned() {
  let v = std::vec![7u32, 8];
  let borrowed: MaybeOwned<'_, [u32]> = (&v[..]).into();
  assert_eq!(borrowed.into_vec(), std::vec![7, 8]); // clone
  let owned: MaybeOwned<'_, [u32]> = std::vec![9u32].into();
  assert_eq!(owned.into_vec(), std::vec![9]); // unbox-in-place
}

#[test]
fn from_box_is_owned() {
  let boxed: std::boxed::Box<str> = std::string::String::from("hi").into_boxed_str();
  let mo: MaybeOwned<'_, str> = boxed.into();
  assert!(mo.is_owned());
  assert_eq!(&*mo, "hi");
}
