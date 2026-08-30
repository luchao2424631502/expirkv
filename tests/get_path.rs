#![allow(dead_code, unused_imports)]

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

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

use commit::{
    CommitCoordinator, DurableFrontier, DurableVLogEnd, TxUuidSource, preflight_delete,
    preflight_put,
};
use db::{
    Db, ReadRuntime, ReadStateSnapshot, UserIndexIterator, UserIndexReader, UserIndexSnapshot,
    ValueReader,
};
use index::{
    FjallBackend, FjallIndexOptions, IndexBackend, IndexCommitMode, IndexCompression,
    initialization_batch,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{VLogGeometry, ValuePointer};
use vlog::reader::ValueLogReader;
use vlog::writer::ValueLogWriter;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x31; 16];

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

struct RealHarness {
    _temporary: TempDir,
    backend: Arc<FjallBackend>,
    runtime: Arc<RuntimeControl>,
    coordinator: CommitCoordinator<FjallBackend, FixedUuid>,
    geometry: VLogGeometry,
    db: Db,
}

impl RealHarness {
    fn new() -> TestResult<Self> {
        Self::new_with_geometry(VLogGeometry::PRODUCTION, 4)
    }

    fn new_with_geometry(geometry: VLogGeometry, cache_capacity: usize) -> TestResult<Self> {
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
            geometry,
            Arc::clone(&catalog),
        )?;
        let files = Arc::new(FileSet::new(
            directory,
            DATABASE_UUID,
            geometry,
            catalog,
            cache_capacity,
        )?);
        let reader = Arc::new(ValueLogReader::new(files, geometry)?);
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
            geometry,
            db,
        })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_put(key, value, false)?)?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_delete(key, false)?)?;
        Ok(())
    }

    fn pointer(&self, key: &[u8]) -> TestResult<ValuePointer> {
        let encoded = self
            .backend
            .get_user(key, None)?
            .expect("put key must have a ValuePointer");
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

fn state_error(kind: StorageErrorKind) -> StorageError {
    StorageError::codec_error(
        kind,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::DoNotRetry,
    )
}

fn deliberately_mismapped_index_error(kind: StorageErrorKind) -> StorageError {
    StorageError::codec_error(
        kind,
        Operation::Open,
        ProtocolStage::Lifecycle,
        Some(WriteOutcome::CommitUnknown),
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

#[test]
fn real_fjall_vlog_get_handles_missing_empty_boundary_overwrite_and_delete() -> TestResult {
    let harness = RealHarness::new()?;
    let options = ReadOptions::default();
    assert_eq!(harness.db.get(&options, b"missing")?, None);
    harness.delete(b"missing")?;
    assert_eq!(harness.db.get(&options, b"missing")?, None);

    harness.put(b"empty", b"")?;
    assert_eq!(harness.db.get(&options, b"empty")?, Some(Vec::new()));

    let binary_key = [0x00, 0xff, 0x80, b'k'];
    let binary_value = [0xff, 0x00, 0x7f, 0x80];
    harness.put(&binary_key, &binary_value)?;
    assert_eq!(
        harness.db.get(&options, &binary_key)?,
        Some(binary_value.to_vec())
    );

    let boundary_key = vec![b'k'; 60_000];
    harness.put(&boundary_key, b"")?;
    assert_eq!(harness.db.get(&options, &boundary_key)?, Some(Vec::new()));

    harness.put(b"overwritten", b"old")?;
    harness.put(b"overwritten", b"new")?;
    assert_eq!(
        harness.db.get(&options, b"overwritten")?,
        Some(b"new".to_vec())
    );
    harness.delete(b"overwritten")?;
    assert_eq!(harness.db.get(&options, b"overwritten")?, None);
    harness.put(b"overwritten", b"reborn")?;
    assert_eq!(
        harness.db.get(&options, b"overwritten")?,
        Some(b"reborn".to_vec())
    );

    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::Healthy
    );
    assert!(harness.runtime.active_request_for_test().is_none());
    assert_eq!(harness.runtime.queued_write_count_for_test(), 0);
    Ok(())
}

#[test]
fn real_get_reads_first_and_later_pages_across_multiple_files() -> TestResult {
    let geometry = VLogGeometry::test_only(512, 1_024, 6)?;
    let harness = RealHarness::new_with_geometry(geometry, 2)?;
    let options = ReadOptions::default();
    let cases: Vec<(Vec<u8>, Vec<u8>)> = (0_u8..10)
        .map(|index| {
            (
                format!("placed-{index:02}").into_bytes(),
                vec![index.wrapping_mul(17).wrapping_add(3); 100],
            )
        })
        .collect();

    for (key, value) in &cases {
        harness.put(key, value)?;
    }

    let mut placements = Vec::new();
    for (key, value) in cases.iter().rev() {
        let pointer = harness.pointer(key)?;
        let page_no = u64::from(pointer.record_offset) / harness.geometry.page_size;
        let page_end = (page_no + 1) * harness.geometry.page_size;
        assert!(pointer.file_id <= harness.geometry.max_file_id);
        assert!(page_no < harness.geometry.max_file_size / harness.geometry.page_size);
        assert!(
            u64::from(pointer.record_offset) + u64::from(pointer.record_len) <= page_end,
            "a KvRecord must remain wholly inside its target page: {pointer:?}"
        );
        assert_eq!(usize::from(pointer.value_len), value.len());
        assert_eq!(harness.db.get(&options, key)?, Some(value.clone()));
        placements.push((pointer.file_id, page_no));
    }
    placements.reverse();
    assert_eq!(
        placements,
        vec![
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (2, 0),
            (2, 1),
            (3, 0),
            (3, 1),
            (4, 0),
            (4, 1),
        ],
        "the real write path must place readable pointers on both pages of five files"
    );
    assert_eq!(
        harness.runtime.state().instance_state,
        InstanceState::Healthy
    );
    Ok(())
}

struct CountingIndex {
    calls: AtomicUsize,
    response: Mutex<IndexResponse>,
}

struct EmptySnapshot;

impl UserIndexSnapshot for EmptySnapshot {
    fn get_user_pointer(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn iter_user(&self) -> Result<UserIndexIterator> {
        let iterator: UserIndexIterator = Box::new(std::iter::empty());
        Ok(iterator)
    }
}

enum IndexResponse {
    Missing,
    Pointer(Vec<u8>),
    Error(StorageErrorKind),
}

impl CountingIndex {
    fn new(response: IndexResponse) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            response: Mutex::new(response),
        }
    }
}

impl UserIndexReader for CountingIndex {
    fn get_user_pointer(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &*self.response.lock().unwrap() {
            IndexResponse::Missing => Ok(None),
            IndexResponse::Pointer(pointer) => Ok(Some(pointer.clone())),
            IndexResponse::Error(kind) => Err(deliberately_mismapped_index_error(*kind)),
        }
    }

    fn snapshot_view(self: Arc<Self>) -> Result<Arc<dyn UserIndexSnapshot>> {
        let _ = self;
        Ok(Arc::new(EmptySnapshot))
    }
}

struct CountingValues {
    calls: AtomicUsize,
    value: Vec<u8>,
}

impl ValueReader for CountingValues {
    fn read_value(&self, _encoded_pointer: &[u8], _expected_key: &[u8]) -> Result<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.value.clone())
    }
}

fn fake_db(
    response: IndexResponse,
    value: &[u8],
) -> (
    Db,
    Arc<RuntimeControl>,
    Arc<CountingIndex>,
    Arc<CountingValues>,
) {
    let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
    let index = Arc::new(CountingIndex::new(response));
    let values = Arc::new(CountingValues {
        calls: AtomicUsize::new(0),
        value: value.to_vec(),
    });
    let db = Db::from_read_components(
        Arc::clone(&runtime),
        Arc::clone(&index),
        Arc::clone(&values),
    );
    (db, runtime, index, values)
}

#[test]
fn invalid_key_and_snapshot_are_rejected_before_index_access() {
    let (db, _, index, values) = fake_db(IndexResponse::Missing, b"");
    let empty = db.get(&ReadOptions::default(), b"").unwrap_err();
    assert_eq!(empty.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(empty.operation, Operation::Get);
    assert_eq!(empty.protocol_stage, ProtocolStage::Read);
    assert_eq!(empty.instance_state, Some(InstanceState::Healthy));
    assert_eq!(
        empty.retry_advice,
        RetryAdvice::FixRequestAndRetrySameInstance
    );

    let too_long = vec![0_u8; 60_001];
    assert_eq!(
        db.get(&ReadOptions::default(), &too_long).unwrap_err().kind,
        StorageErrorKind::InvalidArgument
    );

    let (foreign, _, _, _) = fake_db(IndexResponse::Missing, b"");
    let snapshot = foreign.snapshot().unwrap();
    let invalid_snapshot = db
        .get(
            &ReadOptions {
                snapshot: Some(&snapshot),
            },
            b"key",
        )
        .unwrap_err();
    assert_eq!(invalid_snapshot.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(invalid_snapshot.operation, Operation::Get);
    assert_eq!(invalid_snapshot.protocol_stage, ProtocolStage::Read);
    assert_eq!(
        invalid_snapshot.instance_state,
        Some(InstanceState::Healthy)
    );
    assert_eq!(index.calls.load(Ordering::SeqCst), 0);
    assert_eq!(values.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn write_stopped_reads_valid_data_but_poisoned_rejects_before_index() {
    let (db, runtime, index, values) = fake_db(IndexResponse::Pointer(vec![1; 16]), b"visible");
    runtime.latch_failure(
        InstanceState::WriteStopped,
        &state_error(StorageErrorKind::Io),
    );
    assert_eq!(
        db.get(&ReadOptions::default(), b"key").unwrap(),
        Some(b"visible".to_vec())
    );
    assert_eq!(runtime.state().instance_state, InstanceState::WriteStopped);
    assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    assert_eq!(values.calls.load(Ordering::SeqCst), 1);

    runtime.latch_failure(
        InstanceState::Poisoned,
        &state_error(StorageErrorKind::Corruption),
    );
    let rejected = db.get(&ReadOptions::default(), b"key").unwrap_err();
    assert_eq!(rejected.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(rejected.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    assert_eq!(values.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn every_index_error_kind_has_canonical_state_and_retry_mapping() {
    let cases = [
        (
            StorageErrorKind::InvalidArgument,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::NotFound,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::Busy,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        ),
        (
            StorageErrorKind::Unsupported,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::ResourceExhausted,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        ),
        (
            StorageErrorKind::CapacityExceeded,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::Io,
            InstanceState::Poisoned,
            RetryAdvice::ReopenAndVerify,
        ),
        (
            StorageErrorKind::Corruption,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            StorageErrorKind::InvalidLayout,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            StorageErrorKind::IncompatibleFormat,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::StorageWriteStopped,
            InstanceState::WriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
        (
            StorageErrorKind::StoragePoisoned,
            InstanceState::Poisoned,
            RetryAdvice::ReopenAndVerify,
        ),
        (
            StorageErrorKind::Unrecoverable,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
    ];

    for (kind, expected_state, expected_retry) in cases {
        let (db, runtime, index, values) = fake_db(IndexResponse::Error(kind), b"");
        let error = db.get(&ReadOptions::default(), b"key").unwrap_err();
        assert_eq!(error.kind, kind, "kind={kind:?}");
        assert_eq!(error.operation, Operation::Get, "kind={kind:?}");
        assert_eq!(error.protocol_stage, ProtocolStage::Read, "kind={kind:?}");
        assert_eq!(error.write_outcome, None, "kind={kind:?}");
        assert_eq!(error.instance_state, Some(expected_state), "kind={kind:?}");
        assert_eq!(error.retry_advice, expected_retry, "kind={kind:?}");
        assert_eq!(
            runtime.state().instance_state,
            expected_state,
            "kind={kind:?}"
        );
        assert_eq!(index.calls.load(Ordering::SeqCst), 1, "kind={kind:?}");
        assert_eq!(values.calls.load(Ordering::SeqCst), 0, "kind={kind:?}");

        let latched = runtime.stats().first_latched_error;
        if expected_state == InstanceState::Healthy {
            assert!(latched.is_none(), "kind={kind:?}");
        } else {
            let latched = latched.expect("non-healthy read error must be latched");
            assert_eq!(latched.kind, kind, "kind={kind:?}");
            assert_eq!(latched.operation, Operation::Get, "kind={kind:?}");
            assert_eq!(latched.protocol_stage, ProtocolStage::Read, "kind={kind:?}");
            assert_eq!(latched.retry_advice, expected_retry, "kind={kind:?}");
        }
    }
}

#[derive(Default)]
struct BlockState {
    entered: bool,
    released: bool,
}

struct TestBlocker {
    state: Mutex<BlockState>,
    changed: Condvar,
}

impl TestBlocker {
    fn new() -> Self {
        Self {
            state: Mutex::new(BlockState::default()),
            changed: Condvar::new(),
        }
    }

    fn enter_and_wait_for_release(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(state_error(StorageErrorKind::Busy));
            }
            let (next, timeout) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timeout.timed_out() && !state.released {
                return Err(state_error(StorageErrorKind::Busy));
            }
        }
        Ok(())
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "Get did not reach the blocking read");
            let (next, timeout) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(
                !timeout.timed_out() || state.entered,
                "Get did not reach the blocking read"
            );
        }
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

struct BlockingIndex {
    blocker: TestBlocker,
    error_kind: Option<StorageErrorKind>,
}

impl BlockingIndex {
    fn wait_until_entered(&self) {
        self.blocker.wait_until_entered();
    }

    fn release(&self) {
        self.blocker.release();
    }
}

impl UserIndexReader for BlockingIndex {
    fn get_user_pointer(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.blocker.enter_and_wait_for_release()?;
        match self.error_kind {
            Some(kind) => Err(state_error(kind)),
            None => Ok(None),
        }
    }
}

struct BlockingValues {
    blocker: TestBlocker,
    calls: AtomicUsize,
    value: Vec<u8>,
}

impl ValueReader for BlockingValues {
    fn read_value(&self, _encoded_pointer: &[u8], _expected_key: &[u8]) -> Result<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = self.value.clone();
        self.blocker.enter_and_wait_for_release()?;
        Ok(value)
    }
}

fn receive_get_result(
    receiver: mpsc::Receiver<Result<Option<Vec<u8>>>>,
) -> Result<Option<Vec<u8>>> {
    receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("Get thread did not return within the test timeout")
}

#[test]
fn epoch_change_to_poisoned_prevents_success_after_index_read() {
    let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
    let index = Arc::new(BlockingIndex {
        blocker: TestBlocker::new(),
        error_kind: None,
    });
    let values = Arc::new(CountingValues {
        calls: AtomicUsize::new(0),
        value: Vec::new(),
    });
    let db = Db::from_read_components(
        Arc::clone(&runtime),
        Arc::clone(&index),
        Arc::clone(&values),
    );

    let (result_sender, result_receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = result_sender.send(db.get(&ReadOptions::default(), b"key"));
    });
    index.wait_until_entered();
    runtime.latch_failure(
        InstanceState::Poisoned,
        &state_error(StorageErrorKind::Corruption),
    );
    index.release();
    let error = receive_get_result(result_receiver).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(values.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn epoch_change_to_poisoned_prevents_success_after_value_read() {
    let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
    let index = Arc::new(CountingIndex::new(IndexResponse::Pointer(vec![1; 16])));
    let values = Arc::new(BlockingValues {
        blocker: TestBlocker::new(),
        calls: AtomicUsize::new(0),
        value: b"must-not-escape".to_vec(),
    });
    let db = Db::from_read_components(
        Arc::clone(&runtime),
        Arc::clone(&index),
        Arc::clone(&values),
    );

    let (result_sender, result_receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = result_sender.send(db.get(&ReadOptions::default(), b"key"));
    });
    values.blocker.wait_until_entered();
    runtime.latch_failure(
        InstanceState::Poisoned,
        &state_error(StorageErrorKind::Corruption),
    );
    values.blocker.release();

    let error = receive_get_result(result_receiver).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    assert_eq!(values.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn retry_mapping_uses_the_final_concurrently_poisoned_state() {
    let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
    let index = Arc::new(BlockingIndex {
        blocker: TestBlocker::new(),
        error_kind: Some(StorageErrorKind::ResourceExhausted),
    });
    let values = Arc::new(CountingValues {
        calls: AtomicUsize::new(0),
        value: Vec::new(),
    });
    let db = Db::from_read_components(
        Arc::clone(&runtime),
        Arc::clone(&index),
        Arc::clone(&values),
    );

    let (result_sender, result_receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = result_sender.send(db.get(&ReadOptions::default(), b"key"));
    });
    index.wait_until_entered();
    runtime.latch_failure(
        InstanceState::Poisoned,
        &state_error(StorageErrorKind::Corruption),
    );
    index.release();
    let error = receive_get_result(result_receiver).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(error.retry_advice, RetryAdvice::ReopenAndVerify);
    assert_eq!(values.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn retryable_index_error_remains_retryable_while_write_stopped() {
    let (db, runtime, index, values) = fake_db(
        IndexResponse::Error(StorageErrorKind::ResourceExhausted),
        b"",
    );
    runtime.latch_failure(
        InstanceState::WriteStopped,
        &state_error(StorageErrorKind::Io),
    );

    let error = db.get(&ReadOptions::default(), b"key").unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
    assert_eq!(error.instance_state, Some(InstanceState::WriteStopped));
    assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
    assert_eq!(runtime.state().instance_state, InstanceState::WriteStopped);
    assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    assert_eq!(values.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn get_uses_neither_write_gate_nor_commit_coordinator() {
    let (db, runtime, index, values) = fake_db(IndexResponse::Pointer(vec![0; 16]), b"value");
    assert!(runtime.active_request_for_test().is_none());
    assert_eq!(runtime.queued_write_count_for_test(), 0);
    assert_eq!(
        db.get(&ReadOptions::default(), b"key").unwrap(),
        Some(b"value".to_vec())
    );
    assert!(runtime.active_request_for_test().is_none());
    assert_eq!(runtime.queued_write_count_for_test(), 0);
    assert_eq!(index.calls.load(Ordering::SeqCst), 1);
    assert_eq!(values.calls.load(Ordering::SeqCst), 1);
}
