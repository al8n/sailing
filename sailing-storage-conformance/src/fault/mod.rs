//! Reference fault types: the crash seam a durable store is tested over, the completion-delivery
//! faults an async store must tolerate, and the reference implementations that show what a
//! conforming answer looks like.

mod completion;
pub use completion::{CompletionFaults, FaultyLog, FaultyStable, prior_incarnation_op_id};

mod codec;
pub use codec::{DecodeFault, ReferenceCodec, crc32};

mod journal;
pub use journal::{
  JournalDefects, JournalEngine, JournalEngineSubject, JournalFraming, JournalLog,
  JournalPersistence, JournalRecovery, JournalStable, JournalStorageError,
};

mod probe;
pub use probe::{
  ProbingLog, ProbingLogSubject, ProbingStable, ProbingStableSubject, StagingUnallocatable,
};

mod reference;
pub use reference::{ReferenceEngineSubject, ReferenceLogSubject, ReferenceStableSubject};

mod vfs;
pub use vfs::{CrashClass, Device, DeviceError, SharedVfs, Vfs, VfsDevice};
