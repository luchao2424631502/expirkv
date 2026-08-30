#![allow(dead_code, unused_imports)]

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/snapshot.rs"]
mod snapshot;
pub(crate) use snapshot::Snapshot;
#[path = "../src/cursor.rs"]
mod cursor;
pub(crate) use cursor::{DbIterator, KeyRange, RangeCursor};

#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};
#[path = "../src/batch.rs"]
mod batch;
pub(crate) use batch::WriteBatch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/options.rs"]
mod options;
pub(crate) use options::{Options, ReadOptions, WriteOptions};
#[path = "../src/db.rs"]
mod db;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/recovery/mod.rs"]
mod recovery;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use commit::{CommitCoordinator, DurableFrontier, DurableVLogEnd, TxUuidSource, preflight_put};
use crc32c::crc32c;
use db::{Db, ReadRuntime, ReadStateSnapshot, ValueReader};
use index::{
    FjallBackend, FjallIndexOptions, IndexAtomicBatch, IndexBackend, IndexCommitMode,
    IndexCompression, IndexMutation, initialization_batch,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, PositionedRead, VLogDirectory};
use vlog::format::{RECORD_HEADER_ENCODED_LEN, VLogGeometry, ValuePointer};
use vlog::reader::ValueLogReader;
use vlog::writer::ValueLogWriter;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x52; 16];
const KEY: &[u8] = b"target-key";
const VALUE: &[u8] = b"target-value";

impl ReadRuntime for RuntimeControl {
    fn state_snapshot(&self) -> ReadStateSnapshot {
        let state = self.state();
        ReadStateSnapshot {
            instance_state: state.instance_state,
            state_epoch: state.state_epoch,
        }
    }

    fn latch_read_failure(&self, target: InstanceState, error: &StorageError) -> ReadStateSnapshot {
        let state = self.latch_failure(target, error).current;
        ReadStateSnapshot {
            instance_state: state.instance_state,
            state_epoch: state.state_epoch,
        }
    }

    fn read_stats(&self) -> DbStats {
        self.stats()
    }
}

impl ValueReader for ValueLogReader {
    fn read_value(&self, encoded_pointer: &[u8], expected_key: &[u8]) -> Result<Vec<u8>> {
        ValueLogReader::read_value(self, encoded_pointer, expected_key)
    }
}

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

struct Harness {
    _temporary: TempDir,
    backend: Arc<FjallBackend>,
    runtime: Arc<RuntimeControl>,
    coordinator: CommitCoordinator<FjallBackend, FixedUuid>,
    directory: Arc<VLogDirectory>,
    files: Arc<FileSet>,
    db: Db,
}

impl Harness {
    fn new() -> TestResult<Self> {
        Self::new_with_positioned_read(None)
    }

    fn new_with_positioned_read(
        positioned_read: Option<Arc<dyn PositionedRead>>,
    ) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;

        let backend = Arc::new(FjallBackend::create(&index_path, fjall_options())?);
        backend
            .commit_atomic(
                initialization_batch(0, DATABASE_UUID)
                    .map_err(|error| io::Error::other(format!("initial batch: {error:?}")))?,
                IndexCommitMode::SyncAll,
            )
            .map_err(|error| io::Error::other(format!("initial commit: {error:?}")))?;

        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        let writer = ValueLogWriter::empty(
            Arc::clone(&directory),
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::clone(&catalog),
        )?;
        let files = Arc::new(FileSet::new(
            Arc::clone(&directory),
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::clone(&catalog),
            2,
        )?);
        let reader = Arc::new(match positioned_read {
            Some(positioned_read) => ValueLogReader::new_with_positioned_read(
                Arc::clone(&files),
                VLogGeometry::PRODUCTION,
                positioned_read,
            )?,
            None => ValueLogReader::new(Arc::clone(&files), VLogGeometry::PRODUCTION)?,
        });
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let coordinator = CommitCoordinator::new(
            Arc::clone(&runtime),
            stats,
            Arc::clone(&backend),
            writer,
            FixedUuid(1),
            0,
            DurableFrontier {
                durable_seq: 0,
                durable_vlog_end: DurableVLogEnd::Empty,
            },
            None,
        )?;
        let db = Db::from_read_components(Arc::clone(&runtime), Arc::clone(&backend), reader);
        Ok(Self {
            _temporary: temporary,
            backend,
            runtime,
            coordinator,
            directory,
            files,
            db,
        })
    }

    fn put_target(&self) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_put(KEY, VALUE, false)?)?;
        Ok(())
    }

    fn encoded_pointer(&self) -> TestResult<Vec<u8>> {
        Ok(self
            .backend
            .get_user(KEY, None)?
            .expect("target pointer must exist"))
    }

    fn pointer(&self) -> TestResult<ValuePointer> {
        Ok(ValuePointer::decode(&self.encoded_pointer()?)?)
    }

    fn replace_pointer(&self, encoded_pointer: Vec<u8>) -> TestResult {
        let mut batch = IndexAtomicBatch::new();
        batch
            .try_push(IndexMutation::PutUser {
                user_key: KEY.to_vec(),
                encoded_pointer,
            })
            .map_err(|error| io::Error::other(format!("pointer batch: {error:?}")))?;
        self.backend
            .commit_atomic(batch, IndexCommitMode::Buffer)
            .map_err(|error| io::Error::other(format!("pointer commit: {error:?}")))?;
        Ok(())
    }

    fn file_path(&self, file_id: u32) -> std::path::PathBuf {
        self.directory.path().join(format!("D{file_id:06}.data"))
    }

    fn read_record(&self, pointer: ValuePointer) -> TestResult<Vec<u8>> {
        let file = self.directory.open_writable_for_test(pointer.file_id)?;
        let mut record = vec![0_u8; usize::try_from(pointer.record_len)?];
        file.read_exact_at(&mut record, u64::from(pointer.record_offset))?;
        Ok(record)
    }

    fn write_record(&self, pointer: ValuePointer, record: &[u8]) -> TestResult {
        let file = self.directory.open_writable_for_test(pointer.file_id)?;
        file.write_all_at(record, u64::from(pointer.record_offset))?;
        Ok(())
    }
}

fn fjall_options() -> FjallIndexOptions {
    FjallIndexOptions {
        write_buffer_size: 1024 * 1024,
        max_open_files: 64,
        block_cache_size: 1024 * 1024,
        block_size: 4096,
        block_restart_interval: 16,
        max_file_size: 1024 * 1024,
        compression: IndexCompression::None,
    }
}

fn assert_terminal_read_error(harness: &Harness, expected_kind: StorageErrorKind) -> StorageError {
    let error = harness.db.get(&ReadOptions::default(), KEY).unwrap_err();
    assert_eq!(error.kind, expected_kind);
    assert_eq!(error.operation, Operation::Get);
    assert_eq!(error.protocol_stage, ProtocolStage::Read);
    assert!(error.write_outcome.is_none());
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::Poisoned
    );
    let latched = harness.runtime.stats().first_latched_error.unwrap();
    assert_eq!(latched.kind, expected_kind);
    assert_eq!(latched.operation, Operation::Get);
    assert_eq!(latched.protocol_stage, ProtocolStage::Read);

    let rejected = harness.db.get(&ReadOptions::default(), KEY).unwrap_err();
    assert_eq!(rejected.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(rejected.instance_state, Some(InstanceState::Poisoned));
    error
}

fn pointer_case(mutate: impl FnOnce(&mut Vec<u8>), expected_kind: StorageErrorKind) -> TestResult {
    let harness = Harness::new()?;
    harness.put_target()?;
    let mut pointer = harness.encoded_pointer()?;
    mutate(&mut pointer);
    harness.replace_pointer(pointer)?;
    assert_terminal_read_error(&harness, expected_kind);
    Ok(())
}

#[test]
fn malformed_value_pointer_matrix_is_rejected_and_poisoned() -> TestResult {
    pointer_case(
        |pointer| {
            pointer.pop();
        },
        StorageErrorKind::Corruption,
    )?;
    pointer_case(
        |pointer| pointer[0..2].copy_from_slice(&1_u16.to_le_bytes()),
        StorageErrorKind::IncompatibleFormat,
    )?;
    pointer_case(
        |pointer| pointer[2..6].copy_from_slice(&u32::MAX.to_le_bytes()),
        StorageErrorKind::Corruption,
    )?;
    pointer_case(
        |pointer| pointer[6..10].copy_from_slice(&0_u32.to_le_bytes()),
        StorageErrorKind::Corruption,
    )?;
    pointer_case(
        |pointer| pointer[10..14].copy_from_slice(&1_u32.to_le_bytes()),
        StorageErrorKind::Corruption,
    )?;
    pointer_case(
        |pointer| pointer[14..16].copy_from_slice(&60_000_u16.to_le_bytes()),
        StorageErrorKind::Corruption,
    )?;
    Ok(())
}

fn record_case(mutate: impl FnOnce(&mut Vec<u8>), expected_kind: StorageErrorKind) -> TestResult {
    let harness = Harness::new()?;
    harness.put_target()?;
    let pointer = harness.pointer()?;
    let mut record = harness.read_record(pointer)?;
    mutate(&mut record);
    harness.write_record(pointer, &record)?;
    assert_terminal_read_error(&harness, expected_kind);
    Ok(())
}

fn rewrite_header_crc(record: &mut [u8]) {
    let checksum = crc32c(&record[0..35]);
    record[35..39].copy_from_slice(&checksum.to_le_bytes());
}

fn rewrite_record_crc(record: &mut [u8]) {
    let crc_offset = record.len() - 4;
    let checksum = crc32c(&record[0..crc_offset]);
    record[crc_offset..].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn record_magic_version_type_length_header_crc_record_crc_and_key_are_checked() -> TestResult {
    record_case(|record| record[0] ^= 0x01, StorageErrorKind::Corruption)?;
    record_case(
        |record| {
            record[4..6].copy_from_slice(&1_u16.to_le_bytes());
            rewrite_header_crc(record);
            rewrite_record_crc(record);
        },
        StorageErrorKind::IncompatibleFormat,
    )?;
    record_case(
        |record| {
            record[6] = 0xff;
            rewrite_header_crc(record);
            rewrite_record_crc(record);
        },
        StorageErrorKind::Corruption,
    )?;
    record_case(
        |record| {
            let wrong_len = u32::try_from(record.len()).unwrap() - 1;
            record[7..11].copy_from_slice(&wrong_len.to_le_bytes());
            rewrite_header_crc(record);
            rewrite_record_crc(record);
        },
        StorageErrorKind::Corruption,
    )?;
    record_case(
        |record| record[RECORD_HEADER_ENCODED_LEN - 1] ^= 0x01,
        StorageErrorKind::Corruption,
    )?;
    record_case(
        |record| {
            let last = record.len() - 1;
            record[last] ^= 0x01;
        },
        StorageErrorKind::Corruption,
    )?;
    record_case(
        |record| {
            record[51] ^= 0x01;
            rewrite_record_crc(record);
        },
        StorageErrorKind::Corruption,
    )?;
    Ok(())
}

#[test]
fn missing_truncated_and_corrupt_file_header_are_rejected() -> TestResult {
    let missing = Harness::new()?;
    missing.put_target()?;
    let missing_pointer = missing.pointer()?;
    std::fs::remove_file(missing.file_path(missing_pointer.file_id))?;
    assert_terminal_read_error(&missing, StorageErrorKind::Corruption);

    let truncated = Harness::new()?;
    truncated.put_target()?;
    let truncated_pointer = truncated.pointer()?;
    let file = truncated
        .directory
        .open_writable_for_test(truncated_pointer.file_id)?;
    file.set_len(
        u64::from(truncated_pointer.record_offset) + u64::from(truncated_pointer.record_len) - 1,
    )?;
    assert_terminal_read_error(&truncated, StorageErrorKind::Corruption);

    let header = Harness::new()?;
    header.put_target()?;
    let header_pointer = header.pointer()?;
    let file = header
        .directory
        .open_writable_for_test(header_pointer.file_id)?;
    file.write_all_at(&[b'X'], 16)?;
    assert_terminal_read_error(&header, StorageErrorKind::Corruption);
    Ok(())
}

#[test]
fn pointer_value_length_mismatch_is_detected_after_a_valid_record_decode() -> TestResult {
    let harness = Harness::new()?;
    harness.put_target()?;
    let mut pointer = harness.encoded_pointer()?;
    let value_len = u16::from_le_bytes(pointer[14..16].try_into().unwrap());
    pointer[14..16].copy_from_slice(&(value_len - 1).to_le_bytes());
    harness.replace_pointer(pointer)?;
    assert_terminal_read_error(&harness, StorageErrorKind::Corruption);
    Ok(())
}

#[derive(Debug, Default)]
struct EioPositionedRead {
    offsets: Mutex<Vec<u64>>,
}

impl PositionedRead for EioPositionedRead {
    fn read_at(&self, _file: &File, _buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.offsets.lock().unwrap().push(offset);
        Err(io::Error::from_raw_os_error(5))
    }
}

#[test]
fn exact_record_position_eio_returns_io_and_transitions_to_write_stopped() -> TestResult {
    let positioned_read = Arc::new(EioPositionedRead::default());
    let harness = Harness::new_with_positioned_read(Some(positioned_read.clone()))?;
    harness.put_target()?;
    let pointer = harness.pointer()?;
    let error = harness.db.get(&ReadOptions::default(), KEY).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Get);
    assert_eq!(error.protocol_stage, ProtocolStage::Read);
    assert_eq!(error.instance_state, Some(InstanceState::WriteStopped));
    assert_eq!(error.retry_advice, RetryAdvice::FixEnvironmentAndReopen);
    assert_eq!(error.os_code, Some(5));
    assert_eq!(error.vlog_file_id, Some(pointer.file_id));
    assert_eq!(error.vlog_offset, Some(u64::from(pointer.record_offset)));
    assert_eq!(
        *positioned_read.offsets.lock().unwrap(),
        vec![u64::from(pointer.record_offset)],
        "the injected EIO must occur at the KvRecord positional read"
    );
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::WriteStopped
    );
    let latched = harness.runtime.stats().first_latched_error.unwrap();
    assert_eq!(latched.kind, StorageErrorKind::Io);
    assert_eq!(latched.operation, Operation::Get);
    Ok(())
}

#[test]
fn corruption_while_write_stopped_upgrades_to_poisoned_and_leaks_no_key_or_value() -> TestResult {
    let harness = Harness::new()?;
    harness.put_target()?;
    let write_stop = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    harness
        .runtime
        .latch_failure(InstanceState::WriteStopped, &write_stop);

    let mut pointer = harness.encoded_pointer()?;
    pointer.pop();
    harness.replace_pointer(pointer)?;
    let error = harness.db.get(&ReadOptions::default(), KEY).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::Poisoned
    );
    let rendered = format!("{error:?}");
    assert!(!rendered.contains(std::str::from_utf8(KEY)?));
    assert!(!rendered.contains(std::str::from_utf8(VALUE)?));
    assert!(error.message.is_empty());
    assert!(error.source.is_none());
    Ok(())
}
