//! The crashable byte seam: a device abstraction, an in-memory filesystem behind it, and the three
//! crash classes a durable store must survive.

use std::{
  cell::RefCell,
  collections::BTreeMap,
  rc::Rc,
  string::{String, ToString},
  vec::Vec,
};

/// How a crash takes data away.
///
/// The three classes are not severities of one fault; they are three DIFFERENT faults, and an
/// implementation can pass any two while failing the third.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrashClass {
  /// The process disappears without further writes. Everything already handed to the device
  /// survives, synced or not — the weakest class, and the one a store that never calls `sync`
  /// still passes. It is in the set precisely so a suite can distinguish "loses data on any
  /// crash" from "loses only unsynced data".
  Clean,
  /// EVERY byte written but not [`Device::sync`]ed is gone. This is the only class that proves
  /// fsync: a store that never syncs loses everything here, and a store that syncs its barrier
  /// before releasing completions loses exactly the post-barrier tail.
  LoseUnsyncedWrites,
  /// [`LoseUnsyncedWrites`](Self::LoseUnsyncedWrites), and then the crash-tail device — the last
  /// one appended to — keeps only its first `keep_bytes` bytes. The cut is at an ARBITRARY offset,
  /// so it lands mid-record whenever the suite asks it to: a recovery that replays a record whose
  /// bytes are only half present is exactly the half-a-barrier failure this class exists to find.
  TornTail {
    /// Bytes of the crash-tail device that survive. Offsets past its surviving length are a no-op
    /// (nothing to cut), which makes a sweep over `0..=len` well defined at both ends.
    keep_bytes: u64,
  },
}

/// A byte device a durable store appends to and syncs — the seam a file-backed engine threads
/// through so the same engine runs over a real directory in production and over [`Vfs`] here.
///
/// Deliberately append-and-truncate only: a write-ahead log needs nothing else, and an interface
/// that cannot seek-and-overwrite cannot express a torn in-place update the kit would then have to
/// model.
pub trait Device {
  /// A device fault. Fatal to the store by contract — a barrier that cannot be written must
  /// fail-stop rather than release completions.
  type Error: core::fmt::Debug;

  /// Queue `bytes` at the end of the device. NOT durable until [`sync`](Self::sync).
  fn append(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;

  /// Make every byte appended so far durable. Returns only once they would survive
  /// [`CrashClass::LoseUnsyncedWrites`].
  fn sync(&mut self) -> Result<(), Self::Error>;

  /// The whole device content, durable and queued alike — what a reader in THIS process sees.
  fn read_all(&self) -> Result<Vec<u8>, Self::Error>;

  /// The device length a reader in this process sees (durable + queued).
  fn len(&self) -> u64;

  /// Cut the device back to `len` bytes and make the cut durable.
  ///
  /// RECOVERY needs this and nothing else does: a crash leaves a torn record beyond the last
  /// complete one, and appending after that garbage would put every future record past a prefix
  /// replay always stops at — durable bytes that can never be read again. A length at or above the
  /// current one is a no-op.
  fn truncate(&mut self, len: u64) -> Result<(), Self::Error>;

  /// Whether the device holds no bytes at all.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }
}

/// A device fault. The in-memory filesystem raises one only when a test asks it to
/// ([`SharedVfs::open_failing`]); a real device raises it for ENOSPC, EIO, and their kin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceError;

impl core::fmt::Display for DeviceError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("the journal device refused the write")
  }
}

impl core::error::Error for DeviceError {}

/// One device's bytes, split at the durability line.
#[derive(Debug, Default)]
struct FileState {
  durable: Vec<u8>,
  queued: Vec<u8>,
}

/// An in-memory filesystem that can be crashed.
///
/// Every device it hands out writes into a `(durable, queued)` pair; [`crash`](Self::crash) applies
/// a [`CrashClass`] to all of them at once, leaving the VFS holding exactly what the medium would
/// hold after that fault. Reopening a store over the same VFS is therefore a real recovery from a
/// real (if simulated) medium, not a replay of an in-process snapshot.
#[derive(Debug, Default)]
pub struct Vfs {
  files: BTreeMap<String, FileState>,
  /// The device most recently appended to — where [`CrashClass::TornTail`] cuts. A WAL is the last
  /// thing a barrier writes, so this is the tail a torn write actually lands on.
  tail: Option<String>,
  syncs: u64,
  crashes: u64,
}

impl Vfs {
  /// An empty filesystem.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  fn entry(&mut self, path: &str) -> &mut FileState {
    if !self.files.contains_key(path) {
      self.files.insert(path.to_string(), FileState::default());
    }
    self.files.get_mut(path).expect("inserted directly above")
  }

  fn append(&mut self, path: &str, bytes: &[u8]) {
    self.tail = Some(path.to_string());
    self.entry(path).queued.extend_from_slice(bytes);
  }

  fn sync(&mut self, path: &str) {
    self.syncs += 1;
    let file = self.entry(path);
    let queued = core::mem::take(&mut file.queued);
    file.durable.extend_from_slice(&queued);
  }

  fn truncate(&mut self, path: &str, len: u64) {
    let file = self.entry(path);
    let keep = usize::try_from(len).unwrap_or(usize::MAX);
    file.queued.clear();
    file.durable.truncate(keep);
  }

  fn read_all(&self, path: &str) -> Vec<u8> {
    self.files.get(path).map_or_else(Vec::new, |f| {
      let mut out = f.durable.clone();
      out.extend_from_slice(&f.queued);
      out
    })
  }

  fn len(&self, path: &str) -> u64 {
    self
      .files
      .get(path)
      .map_or(0, |f| (f.durable.len() + f.queued.len()) as u64)
  }

  /// The bytes of `path` that would survive [`CrashClass::LoseUnsyncedWrites`].
  #[must_use]
  pub fn durable_len(&self, path: &str) -> u64 {
    self.files.get(path).map_or(0, |f| f.durable.len() as u64)
  }

  /// The device a [`CrashClass::TornTail`] would cut, and its surviving length — what a suite
  /// sweeps torn offsets over.
  #[must_use]
  pub fn tail_durable_len(&self) -> u64 {
    self
      .tail
      .as_deref()
      .map_or(0, |path| self.durable_len(path))
  }

  /// How many [`Device::sync`] calls this filesystem has served. A store that never syncs shows
  /// zero here, which is the direct evidence behind a lost-write violation.
  #[must_use]
  pub const fn syncs(&self) -> u64 {
    self.syncs
  }

  /// How many crashes this filesystem has been through.
  #[must_use]
  pub const fn crashes(&self) -> u64 {
    self.crashes
  }

  /// Apply `class` to every device, leaving the VFS holding exactly the surviving medium.
  pub fn crash(&mut self, class: CrashClass) {
    self.crashes += 1;
    match class {
      CrashClass::Clean => {
        // A clean drop keeps the queued bytes: they were handed to the medium and nothing
        // discarded them. Only the classes below model a fault that does.
        for file in self.files.values_mut() {
          let queued = core::mem::take(&mut file.queued);
          file.durable.extend_from_slice(&queued);
        }
      }
      CrashClass::LoseUnsyncedWrites => {
        for file in self.files.values_mut() {
          file.queued.clear();
        }
      }
      CrashClass::TornTail { keep_bytes } => {
        for file in self.files.values_mut() {
          file.queued.clear();
        }
        if let Some(path) = self.tail.clone()
          && let Some(file) = self.files.get_mut(&path)
        {
          let keep = usize::try_from(keep_bytes)
            .unwrap_or(usize::MAX)
            .min(file.durable.len());
          file.durable.truncate(keep);
        }
      }
    }
  }
}

/// A [`Vfs`] shared between the devices opened over it and the code that crashes it — the handle a
/// store keeps across a crash and a reopen.
#[derive(Debug, Clone, Default)]
pub struct SharedVfs(Rc<RefCell<Vfs>>);

impl SharedVfs {
  /// A fresh, empty filesystem.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// A device over `path`, created empty if it does not exist.
  #[must_use]
  pub fn open(&self, path: &str) -> VfsDevice {
    self.0.borrow_mut().entry(path);
    VfsDevice {
      vfs: self.clone(),
      path: path.to_string(),
      honours_sync: true,
      fail_from: None,
      writes: 0,
      fails_read: false,
    }
  }

  /// A device over `path` whose [`Device::sync`] SILENTLY DOES NOTHING — the fsync a store forgot
  /// to make, or made against the wrong handle.
  ///
  /// A deliberate fault, and the sharpest one in the kit: everything looks identical until
  /// [`CrashClass::LoseUnsyncedWrites`], where every byte the store believed durable is gone.
  #[must_use]
  pub fn open_never_syncing(&self, path: &str) -> VfsDevice {
    let mut device = self.open(path);
    device.honours_sync = false;
    device
  }

  /// A device over `path` whose READS fail. The medium is there; the engine cannot see it.
  #[must_use]
  pub fn open_unreadable(&self, path: &str) -> VfsDevice {
    let mut device = self.open(path);
    device.fails_read = true;
    device
  }

  /// A device over `path` that FAILS every write from the `nth` one onward (1 = fail immediately).
  ///
  /// The medium going away underneath a live engine — a full disk, a failing controller. What an
  /// engine does next is the whole question: a barrier it could not write must never release the
  /// completions that claim it did.
  #[must_use]
  pub fn open_failing(&self, path: &str, nth: u32) -> VfsDevice {
    let mut device = self.open(path);
    device.fail_from = Some(nth.max(1));
    device
  }

  /// Crash the whole filesystem — see [`Vfs::crash`].
  pub fn crash(&self, class: CrashClass) {
    self.0.borrow_mut().crash(class);
  }

  /// [`Vfs::syncs`].
  #[must_use]
  pub fn syncs(&self) -> u64 {
    self.0.borrow().syncs()
  }

  /// [`Vfs::tail_durable_len`].
  #[must_use]
  pub fn tail_durable_len(&self) -> u64 {
    self.0.borrow().tail_durable_len()
  }

  /// [`Vfs::durable_len`] for `path`.
  #[must_use]
  pub fn durable_len(&self, path: &str) -> u64 {
    self.0.borrow().durable_len(path)
  }
}

/// A [`Device`] over one path of a [`SharedVfs`].
#[derive(Debug, Clone)]
pub struct VfsDevice {
  vfs: SharedVfs,
  path: String,
  /// Whether [`Device::sync`] actually makes bytes durable. False only for the deliberate no-fsync
  /// fault ([`SharedVfs::open_never_syncing`]).
  honours_sync: bool,
  /// The write ordinal from which every operation fails, for the deliberate device fault
  /// ([`SharedVfs::open_failing`]).
  fail_from: Option<u32>,
  writes: u32,
  /// Whether [`Device::read_all`] fails ([`SharedVfs::open_unreadable`]).
  fails_read: bool,
}

impl VfsDevice {
  /// Charge one write against the injected-failure ordinal.
  fn admit_write(&mut self) -> Result<(), DeviceError> {
    self.writes += 1;
    match self.fail_from {
      Some(nth) if self.writes >= nth => Err(DeviceError),
      _ => Ok(()),
    }
  }
}

impl Device for VfsDevice {
  type Error = DeviceError;

  fn append(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
    self.admit_write()?;
    self.vfs.0.borrow_mut().append(&self.path, bytes);
    Ok(())
  }

  fn sync(&mut self) -> Result<(), Self::Error> {
    self.admit_write()?;
    if self.honours_sync {
      self.vfs.0.borrow_mut().sync(&self.path);
    }
    Ok(())
  }

  fn read_all(&self) -> Result<Vec<u8>, Self::Error> {
    if self.fails_read {
      return Err(DeviceError);
    }
    Ok(self.vfs.0.borrow().read_all(&self.path))
  }

  fn len(&self) -> u64 {
    self.vfs.0.borrow().len(&self.path)
  }

  fn truncate(&mut self, len: u64) -> Result<(), Self::Error> {
    self.admit_write()?;
    self.vfs.0.borrow_mut().truncate(&self.path, len);
    Ok(())
  }
}

#[cfg(test)]
mod tests;
