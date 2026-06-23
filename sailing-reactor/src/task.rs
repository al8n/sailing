use agnostic_lite::{AsyncSpawner, JoinHandle, RuntimeLite};

/// A spawned task handle whose drop ABORTS the task — on every runtime.
///
/// The runtimes behind [`RuntimeLite`] do not agree on what dropping a raw spawn handle means:
/// tokio's handle DETACHES (the task keeps running, now unsupervised), while smol's wrapper cancels
/// on drop. Driver state holding raw handles would therefore leak live tasks on one runtime exactly
/// where it tears them down on the other — and a test suite green on one runtime could mask the
/// opposite behavior on the other. This wrapper normalizes through the trait's consuming
/// [`JoinHandle::abort`] (tokio: a real abort; smol: the cancel-on-drop), so dropping the OWNER of a
/// task — a connection, a dial in flight — IS the task's teardown, a structural invariant rather
/// than a per-runtime accident.
///
/// This is the only handle type driver state may hold: every `R::spawn` is wrapped at the spawn
/// site. A task meant to outlive its spawner uses `R::spawn_detach` explicitly instead.
///
/// The compio driver needs no analog: a compio `JoinHandle` already aborts its task on drop, so it
/// holds the raw handle directly. Only the readiness runtimes need the normalization.
pub(crate) struct AbortOnDrop<R: RuntimeLite>(Option<<R::Spawner as AsyncSpawner>::JoinHandle<()>>);

impl<R: RuntimeLite> AbortOnDrop<R> {
  /// Wrap the handle returned by `R::spawn` for a `()`-output task.
  pub(crate) fn new(handle: <R::Spawner as AsyncSpawner>::JoinHandle<()>) -> Self {
    Self(Some(handle))
  }
}

impl<R: RuntimeLite> Drop for AbortOnDrop<R> {
  fn drop(&mut self) {
    if let Some(handle) = self.0.take() {
      handle.abort();
    }
  }
}
