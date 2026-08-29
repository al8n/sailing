use super::*;

#[test]
fn a_clean_crash_keeps_queued_bytes() {
  let vfs = SharedVfs::new();
  let mut dev = vfs.open("wal");
  dev.append(b"abc").unwrap();
  dev.sync().unwrap();
  dev.append(b"def").unwrap();
  vfs.crash(CrashClass::Clean);
  assert_eq!(
    vfs.open("wal").read_all().unwrap(),
    b"abcdef".to_vec(),
    "a clean drop discards nothing"
  );
}

#[test]
fn losing_unsynced_writes_keeps_exactly_the_synced_prefix() {
  let vfs = SharedVfs::new();
  let mut dev = vfs.open("wal");
  dev.append(b"abc").unwrap();
  dev.sync().unwrap();
  dev.append(b"def").unwrap();
  assert_eq!(dev.len(), 6, "the live reader sees the queued tail");
  assert_eq!(vfs.durable_len("wal"), 3);
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  assert_eq!(vfs.open("wal").read_all().unwrap(), b"abc".to_vec());
}

#[test]
fn a_torn_tail_cuts_the_last_appended_device_mid_record() {
  let vfs = SharedVfs::new();
  let mut meta = vfs.open("meta");
  meta.append(b"MMMM").unwrap();
  meta.sync().unwrap();
  let mut wal = vfs.open("wal");
  wal.append(b"0123456789").unwrap();
  wal.sync().unwrap();
  assert_eq!(vfs.tail_durable_len(), 10, "the WAL was appended to last");
  vfs.crash(CrashClass::TornTail { keep_bytes: 4 });
  assert_eq!(vfs.open("wal").read_all().unwrap(), b"0123".to_vec());
  assert_eq!(
    vfs.open("meta").read_all().unwrap(),
    b"MMMM".to_vec(),
    "a torn tail cuts the tail device only"
  );
}

#[test]
fn a_torn_offset_past_the_end_keeps_everything_synced() {
  let vfs = SharedVfs::new();
  let mut wal = vfs.open("wal");
  wal.append(b"0123").unwrap();
  wal.sync().unwrap();
  wal.append(b"456").unwrap();
  vfs.crash(CrashClass::TornTail { keep_bytes: 999 });
  assert_eq!(
    vfs.open("wal").read_all().unwrap(),
    b"0123".to_vec(),
    "the cut clamps to the surviving length, and the unsynced tail is still lost"
  );
}

#[test]
fn syncs_are_counted_so_a_never_syncing_store_is_visible() {
  let vfs = SharedVfs::new();
  let mut dev = vfs.open("wal");
  dev.append(b"x").unwrap();
  assert_eq!(vfs.syncs(), 0);
  dev.sync().unwrap();
  dev.sync().unwrap();
  assert_eq!(vfs.syncs(), 2);
}

#[test]
fn an_unopened_path_reads_empty() {
  let vfs = SharedVfs::new();
  assert!(vfs.open("absent").is_empty());
  assert_eq!(vfs.durable_len("absent"), 0);
}

#[test]
fn a_never_syncing_device_loses_everything_it_claimed_to_persist() {
  let vfs = SharedVfs::new();
  let mut dev = vfs.open_never_syncing("wal");
  dev.append(b"durable?").unwrap();
  dev.sync().unwrap();
  assert_eq!(
    dev.read_all().unwrap(),
    b"durable?".to_vec(),
    "the live reader sees the bytes either way — that is what makes the fault silent"
  );
  vfs.crash(CrashClass::LoseUnsyncedWrites);
  assert!(
    vfs.open("wal").read_all().unwrap().is_empty(),
    "a sync that does nothing leaves nothing behind"
  );
}
