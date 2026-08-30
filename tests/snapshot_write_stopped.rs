#![allow(dead_code, unused_imports)]

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};
#[path = "../src/snapshot.rs"]
mod snapshot;
pub(crate) use snapshot::Snapshot;
#[path = "../src/cursor.rs"]
mod cursor;
pub(crate) use cursor::{DbIterator, KeyRange, RangeCursor};
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
use db::{Db, ReadRuntime, ReadStateSnapshot, UserIndexReader, ValueReader};
use index::{
    FjallBackend, FjallIndexOptions, IndexBackend, IndexCommitMode, IndexCompression,
    initialization_batch,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, PositionedRead, VLogDirectory};
use vlog::format::{VLogGeometry, ValuePointer};
use vlog::reader::ValueLogReader;
use vlog::writer::ValueLogWriter;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x75; 16];
const FIRST_KEY: &[u8] = b"a";
const FIRST_VALUE: &[u8] = b"one";
const SECOND_KEY: &[u8] = b"b";
const SECOND_VALUE: &[u8] = b"two";

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

#[derive(Debug)]
struct FailFirstPositionedRead {
    fail_next: AtomicBool,
    calls: Mutex<Vec<u64>>,
}

impl FailFirstPositionedRead {
    fn new() -> Self {
        Self {
            fail_next: AtomicBool::new(true),
            calls: Mutex::new(Vec::new()),
        }
    }
}

impl PositionedRead for FailFirstPositionedRead {
    fn read_at(&self, file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.calls
            .lock()
            .expect("calls mutex poisoned")
            .push(offset);
        if self.fail_next.swap(false, Ordering::AcqRel) {
            Err(io::Error::from_raw_os_error(5))
        } else {
            FileExt::read_at(file, buffer, offset)
        }
    }
}

struct Harness {
    _temporary: TempDir,
    backend: Arc<FjallBackend>,
    runtime: Arc<RuntimeControl>,
    coordinator: CommitCoordinator<FjallBackend, FixedUuid>,
    reader: Arc<ValueLogReader>,
    positioned_read: Arc<FailFirstPositionedRead>,
    db: Db,
}

impl Harness {
    fn new() -> TestResult<Self> {
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
            directory,
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            catalog,
            2,
        )?);
        let positioned_read = Arc::new(FailFirstPositionedRead::new());
        let reader = Arc::new(ValueLogReader::new_with_positioned_read(
            files,
            VLogGeometry::PRODUCTION,
            positioned_read.clone(),
        )?);
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
        let db = Db::from_read_components(
            Arc::clone(&runtime),
            Arc::clone(&backend),
            Arc::clone(&reader),
        );
        Ok(Self {
            _temporary: temporary,
            backend,
            runtime,
            coordinator,
            reader,
            positioned_read,
            db,
        })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_put(key, value, false)?)?;
        Ok(())
    }

    fn pointer(&self, key: &[u8]) -> TestResult<ValuePointer> {
        let encoded = self
            .backend
            .get_user(key, None)?
            .ok_or("missing test pointer")?;
        Ok(ValuePointer::decode(&encoded)?)
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

#[test]
fn exact_kv_record_eio_enters_write_stopped_and_all_stage15_reads_continue() -> TestResult {
    let harness = Harness::new()?;
    harness.put(FIRST_KEY, FIRST_VALUE)?;
    harness.put(SECOND_KEY, SECOND_VALUE)?;
    let first_pointer = harness.pointer(FIRST_KEY)?;

    let error = harness
        .db
        .get(&ReadOptions::default(), FIRST_KEY)
        .unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Get);
    assert_eq!(error.protocol_stage, ProtocolStage::Read);
    assert_eq!(error.os_code, Some(5));
    assert_eq!(error.instance_state, Some(InstanceState::WriteStopped));
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::WriteStopped
    );
    assert_eq!(
        *harness
            .positioned_read
            .calls
            .lock()
            .expect("calls mutex poisoned"),
        vec![u64::from(first_pointer.record_offset)],
        "the injected EIO must hit the selected KV_RECORD position"
    );

    assert_eq!(
        harness.db.get(&ReadOptions::default(), FIRST_KEY)?,
        Some(FIRST_VALUE.to_vec())
    );

    let snapshot = harness.db.snapshot()?;
    let snapshot_clone = snapshot.clone();
    drop(snapshot);
    let snapshot_options = ReadOptions {
        snapshot: Some(&snapshot_clone),
    };
    assert_eq!(
        harness.db.get(&snapshot_options, FIRST_KEY)?,
        Some(FIRST_VALUE.to_vec())
    );

    let mut cursor = harness.db.iter(&snapshot_options)?;
    cursor.seek_to_first();
    assert_eq!(cursor.key(), Some(FIRST_KEY));
    assert_eq!(cursor.value(), Some(FIRST_VALUE));
    cursor.next();
    assert_eq!(cursor.key(), Some(SECOND_KEY));
    assert_eq!(cursor.value(), Some(SECOND_VALUE));
    assert!(cursor.status().is_ok());

    let range = harness.db.range(
        &snapshot_options,
        KeyRange {
            start: Some(SECOND_KEY),
            end: None,
        },
        1,
    )?;
    assert_eq!(range.key(), Some(SECOND_KEY));
    assert_eq!(range.value(), Some(SECOND_VALUE));
    assert!(range.status().is_ok());

    drop(snapshot_clone);
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::WriteStopped
    );
    Ok(())
}
