//! [`MaybeOwned`] — a borrowed-or-owned `?Sized` value.

use core::ops::Deref;
use std::{boxed::Box, vec::Vec};

/// A value that is either borrowed (`&'a T`) or owned ([`Box<T>`]).
///
/// Like [`Cow`](std::borrow::Cow), but the owned side is a [`Box<T>`] rather than
/// `<T as ToOwned>::Owned`, so naming the type needs no `T: Clone` / `ToOwned` bound. Read it
/// through the [`Deref`] to `T` (`view.iter()`, `&*view`); for a slice `[E]`, take the elements
/// with [`into_vec`](MaybeOwned::into_vec), which is allocation-free when already
/// [`Owned`](MaybeOwned::Owned).
#[derive(Debug)]
pub enum MaybeOwned<'a, T>
where
  T: ?Sized,
{
  /// A borrowed value.
  Borrowed(&'a T),
  /// An owned value.
  Owned(Box<T>),
}

impl<T> MaybeOwned<'_, T>
where
  T: ?Sized,
{
  /// Whether this is the [`Borrowed`](MaybeOwned::Borrowed) variant.
  #[inline]
  pub const fn is_borrowed(&self) -> bool {
    matches!(self, Self::Borrowed(_))
  }

  /// Whether this is the [`Owned`](MaybeOwned::Owned) variant.
  #[inline]
  pub const fn is_owned(&self) -> bool {
    matches!(self, Self::Owned(_))
  }
}

impl<T> MaybeOwned<'_, [T]>
where
  T: Clone,
{
  /// Take the elements as a `Vec`: allocation-free when already [`Owned`](MaybeOwned::Owned)
  /// (the boxed slice is unboxed in place), clones a [`Borrowed`](MaybeOwned::Borrowed) slice.
  #[inline]
  pub fn into_vec(self) -> Vec<T> {
    match self {
      Self::Borrowed(slice) => slice.to_vec(),
      Self::Owned(boxed) => boxed.into_vec(),
    }
  }
}

impl<T> Deref for MaybeOwned<'_, T>
where
  T: ?Sized,
{
  type Target = T;

  #[inline]
  fn deref(&self) -> &T {
    match self {
      Self::Borrowed(value) => value,
      Self::Owned(boxed) => boxed,
    }
  }
}

impl<'a, T> From<&'a T> for MaybeOwned<'a, T>
where
  T: ?Sized,
{
  #[inline]
  fn from(value: &'a T) -> Self {
    Self::Borrowed(value)
  }
}

impl<T> From<Box<T>> for MaybeOwned<'_, T>
where
  T: ?Sized,
{
  #[inline]
  fn from(boxed: Box<T>) -> Self {
    Self::Owned(boxed)
  }
}

impl<T> From<Vec<T>> for MaybeOwned<'_, [T]> {
  #[inline]
  fn from(vec: Vec<T>) -> Self {
    Self::Owned(vec.into_boxed_slice())
  }
}

#[cfg(test)]
mod tests;
