#![allow(dead_code, unused_imports)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition as PublicVLogPosition};
#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use batch::WriteBatch;
use commit::{
    CommitCoordinator, DurableFrontier, DurableVLogEnd, TransactionDescriptor, TxMutation,
    TxUuidSource, VLogPos, ValueState, decode_descriptor, decode_head_seq, preflight_batch,
    preflight_delete, preflight_put, prepare_commit,
};
use index::{
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, HEAD_SEQ_KEY, IndexApplyState,
    IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexCompression,
    IndexEntry, IndexMutation, InternalIndexError, InternalIndexSpace, InternalKeyRange,
    initialization_batch,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{VLogGeometry, ValuePointer};
use vlog::reader::ValueLogReader;
use vlog::writer::{ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x71; 16];

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    VLogWrite,
    VLogFileSync,
    VLogDirectorySync,
    IndexCommit(IndexCommitMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteFailure {
    None,
    AlwaysWouldBlock,
    PartialThenEio,
}

struct SpyWriterIo {
    events: Arc<Mutex<Vec<Event>>>,
    failure: WriteFailure,
    write_calls: Mutex<usize>,
}

impl SpyWriterIo {
    fn new(events: Arc<Mutex<Vec<Event>>>, failure: WriteFailure) -> Self {
        Self {
            events,
            failure,
            write_calls: Mutex::new(0),
        }
    }
}

impl WriterIo for SpyWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.events.lock().unwrap().push(Event::VLogWrite);
        let mut calls = self.write_calls.lock().unwrap();
        *calls += 1;
        match self.failure {
            WriteFailure::AlwaysWouldBlock => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            WriteFailure::PartialThenEio if *calls == 1 => {
                let amount = bytes.len().max(2) / 2;
                file.write_at(&bytes[..amount], offset)
            }
            WriteFailure::PartialThenEio => Err(io::Error::from_raw_os_error(5)),
            WriteFailure::None => file.write_at(bytes, offset),
        }
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        self.events.lock().unwrap().push(Event::VLogFileSync);
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        self.events.lock().unwrap().push(Event::VLogDirectorySync);
        directory.sync()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitFailure {
    None,
    NotApplied(StorageErrorKind),
    Unknown(StorageErrorKind),
}

#[derive(Default)]
struct BlockState {
    enabled: bool,
    entered: bool,
    released: bool,
}

#[derive(Default)]
struct FakeData {
    user: BTreeMap<Vec<u8>, Vec<u8>>,
    transaction: BTreeMap<Vec<u8>, Vec<u8>>,
    system: BTreeMap<Vec<u8>, Vec<u8>>,
    calls: Vec<(IndexAtomicBatch, IndexCommitMode)>,
}

struct FakeBackend {
    data: Mutex<FakeData>,
    failure: Mutex<CommitFailure>,
    user_read_failure: Mutex<Option<StorageErrorKind>>,
    block: Mutex<BlockState>,
    changed: Condvar,
    events: Arc<Mutex<Vec<Event>>>,
}

impl FakeBackend {
    fn new(events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            data: Mutex::new(FakeData::default()),
            failure: Mutex::new(CommitFailure::None),
            user_read_failure: Mutex::new(None),
            block: Mutex::new(BlockState::default()),
            changed: Condvar::new(),
            events,
        }
    }

    fn set_failure(&self, failure: CommitFailure) {
        *self.failure.lock().unwrap() = failure;
    }

    fn set_user_read_failure(&self, failure: StorageErrorKind) {
        *self.user_read_failure.lock().unwrap() = Some(failure);
    }

    fn enable_block(&self) {
        *self.block.lock().unwrap() = BlockState {
            enabled: true,
            entered: false,
            released: false,
        };
    }

    fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut block = self.block.lock().unwrap();
        while !block.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "backend commit did not start");
            block = self.changed.wait_timeout(block, remaining).unwrap().0;
        }
    }

    fn release(&self) {
        let mut block = self.block.lock().unwrap();
        block.released = true;
        drop(block);
        self.changed.notify_all();
    }

    fn calls(&self) -> Vec<(IndexAtomicBatch, IndexCommitMode)> {
        self.data.lock().unwrap().calls.clone()
    }

    fn transaction_len(&self) -> usize {
        self.data.lock().unwrap().transaction.len()
    }
}

impl IndexBackend for FakeBackend {
    type Snapshot = BTreeMap<Vec<u8>, Vec<u8>>;
    type UserIterator = std::vec::IntoIter<Result<IndexEntry>>;
    type InternalIterator = std::vec::IntoIter<Result<IndexEntry>>;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        batch.validate_for_commit(mode)?;
        self.events.lock().unwrap().push(Event::IndexCommit(mode));
        self.data.lock().unwrap().calls.push((batch.clone(), mode));

        let mut block = self.block.lock().unwrap();
        if block.enabled {
            block.entered = true;
            self.changed.notify_all();
            while !block.released {
                block = self.changed.wait(block).unwrap();
            }
        }
        drop(block);

        match *self.failure.lock().unwrap() {
            CommitFailure::NotApplied(kind) => {
                return Err(IndexCommitError::not_applied(InternalIndexError::new(
                    kind,
                    (kind == StorageErrorKind::Io).then_some(5),
                )));
            }
            CommitFailure::Unknown(kind) => {
                return Err(IndexCommitError::unknown(InternalIndexError::new(
                    kind,
                    (kind == StorageErrorKind::Io).then_some(5),
                )));
            }
            CommitFailure::None => {}
        }

        let mut data = self.data.lock().unwrap();
        for mutation in batch.into_operations() {
            match mutation {
                IndexMutation::InitializeDatabaseIdentity { encoded_identity } => {
                    data.system
                        .insert(b"database_identity".to_vec(), encoded_identity);
                }
                IndexMutation::PutUser {
                    user_key,
                    encoded_pointer,
                } => {
                    data.user.insert(user_key, encoded_pointer);
                }
                IndexMutation::DeleteUser { user_key } => {
                    data.user.remove(&user_key);
                }
                IndexMutation::PutInternal { space, key, value } => {
                    internal_map_mut(&mut data, space).insert(key, value);
                }
                IndexMutation::DeleteInternal { space, key } => {
                    internal_map_mut(&mut data, space).remove(&key);
                }
            }
        }
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .system
            .get(b"database_identity".as_slice())
            .cloned())
    }

    fn get_user(&self, key: &[u8], _snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        if let Some(kind) = *self.user_read_failure.lock().unwrap() {
            return Err(StorageError::codec_error(
                kind,
                Operation::Get,
                ProtocolStage::Read,
                None,
                RetryAdvice::DoNotRetry,
            ));
        }
        Ok(self.data.lock().unwrap().user.get(key).cloned())
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        Ok(internal_map(&data, space).get(key).cloned())
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        _range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        let data = self.data.lock().unwrap();
        Ok(internal_map(&data, space)
            .iter()
            .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
            .collect::<Vec<_>>()
            .into_iter())
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(self.data.lock().unwrap().user.clone())
    }

    fn iter_user(&self, _snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .user
            .iter()
            .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
            .collect::<Vec<_>>()
            .into_iter())
    }
}

fn internal_map(data: &FakeData, space: InternalIndexSpace) -> &BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &data.transaction,
        InternalIndexSpace::System => &data.system,
    }
}

fn internal_map_mut(
    data: &mut FakeData,
    space: InternalIndexSpace,
) -> &mut BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &mut data.transaction,
        InternalIndexSpace::System => &mut data.system,
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

struct FakeHarness {
    _temporary: TempDir,
    coordinator: Arc<CommitCoordinator<FakeBackend, FixedUuid>>,
    backend: Arc<FakeBackend>,
    runtime: Arc<RuntimeControl>,
    stats: Arc<StatsState>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl FakeHarness {
    fn new(write_failure: WriteFailure) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let events = Arc::new(Mutex::new(Vec::new()));
        let backend = Arc::new(FakeBackend::new(Arc::clone(&events)));
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let writer = ValueLogWriter::empty_with_io(
            directory,
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::new(FileCatalog::new()),
            Arc::new(SpyWriterIo::new(Arc::clone(&events), write_failure)),
        )?;
        let coordinator = Arc::new(CommitCoordinator::new(
            Arc::clone(&runtime),
            Arc::clone(&stats),
            Arc::clone(&backend),
            writer,
            FixedUuid(1),
            0,
            empty_frontier(),
            None,
        )?);
        Ok(Self {
            _temporary: temporary,
            coordinator,
            backend,
            runtime,
            stats,
            events,
        })
    }
}

struct RealCommitHarness {
    _temporary: TempDir,
    backend: Arc<FjallBackend>,
    coordinator: CommitCoordinator<FjallBackend, FixedUuid>,
    stats: Arc<StatsState>,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
    geometry: VLogGeometry,
}

impl RealCommitHarness {
    fn new(geometry: VLogGeometry) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let backend = Arc::new(FjallBackend::create(&index_path, fjall_options())?);
        backend
            .commit_atomic(
                initialization_batch(0, DATABASE_UUID).map_err(|error| {
                    io::Error::other(format!("initial batch error: {:?}", error.kind))
                })?,
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
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let coordinator = CommitCoordinator::new(
            runtime,
            Arc::clone(&stats),
            Arc::clone(&backend),
            writer,
            FixedUuid(0x41),
            0,
            empty_frontier(),
            None,
        )?;
        Ok(Self {
            _temporary: temporary,
            backend,
            coordinator,
            stats,
            directory,
            catalog,
            geometry,
        })
    }

    fn put(&self, key: &[u8], value: &[u8], sync: bool) -> TestResult {
        let write = preflight_put(key, value, sync)?;
        self.coordinator.commit_nonempty(&write)?;
        Ok(())
    }

    fn delete(&self, key: &[u8], sync: bool) -> TestResult {
        let write = preflight_delete(key, sync)?;
        self.coordinator.commit_nonempty(&write)?;
        Ok(())
    }

    fn write(&self, batch: &WriteBatch, sync: bool) -> TestResult {
        let write = preflight_batch(batch, sync)?;
        assert_ne!(
            write.logical_op_count(),
            0,
            "nonempty helper received an empty batch"
        );
        self.coordinator.commit_nonempty(&write)?;
        Ok(())
    }

    fn barrier(&self) -> TestResult {
        self.coordinator.commit_empty_batch(true)?;
        Ok(())
    }

    fn reader(&self, capacity: usize) -> TestResult<(Arc<FileSet>, ValueLogReader)> {
        let files = Arc::new(FileSet::new(
            Arc::clone(&self.directory),
            DATABASE_UUID,
            self.geometry,
            Arc::clone(&self.catalog),
            capacity,
        )?);
        let reader = ValueLogReader::new(Arc::clone(&files), self.geometry)?;
        Ok((files, reader))
    }

    fn descriptor_entry_count(&self) -> TestResult<usize> {
        Ok(self
            .backend
            .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())?
            .collect::<Result<Vec<_>>>()?
            .len())
    }
}

fn assert_terminal_values(
    harness: &RealCommitHarness,
    reader: &ValueLogReader,
    expected: &[(Vec<u8>, Option<Vec<u8>>)],
) -> TestResult {
    let mut actual: BTreeMap<Vec<u8>, Vec<u8>> = harness
        .backend
        .iter_user(None)?
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect();

    for (key, expected_value) in expected {
        match expected_value {
            Some(expected_value) => {
                let encoded_pointer = actual
                    .remove(key.as_slice())
                    .unwrap_or_else(|| panic!("missing expected key {key:?}"));
                let value = reader.read_value(&encoded_pointer, key)?;
                assert_eq!(
                    value.as_slice(),
                    expected_value.as_slice(),
                    "value mismatch for key {key:?}"
                );
            }
            None => {
                assert!(
                    actual.remove(key.as_slice()).is_none(),
                    "deleted key remains indexed: {key:?}"
                );
            }
        }
    }
    assert!(actual.is_empty(), "unexpected indexed keys: {actual:?}");
    Ok(())
}

fn assert_persisted_state(
    harness: &RealCommitHarness,
    expected_head: u64,
    expected_durable: u64,
) -> TestResult {
    let head = harness
        .backend
        .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
        .expect("persisted head_seq");
    assert_eq!(u64::from_le_bytes(head.try_into().unwrap()), expected_head);
    let frontier = harness
        .backend
        .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
        .expect("persisted durable frontier");
    assert_eq!(
        DurableFrontier::decode(&frontier)?.durable_seq,
        expected_durable
    );
    Ok(())
}

fn assert_commit_state(
    harness: &RealCommitHarness,
    expected_head: u64,
    expected_durable: u64,
    expect_dirty: bool,
) {
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, expected_head);
    assert_eq!(state.durable_seq, expected_durable);
    if expected_head == expected_durable {
        assert_eq!(state.head_vlog_end, state.durable_vlog_end);
    }
    let dirty = harness.coordinator.dirty_state_for_test();
    assert_eq!(
        !dirty.dirty_files.is_empty() || !dirty.pending_directory_entries.is_empty(),
        expect_dirty
    );
    let stats = harness.stats.snapshot();
    assert_eq!(stats.head_seq, expected_head);
    assert_eq!(stats.durable_seq, expected_durable);
    assert_eq!(
        stats.durability_lag,
        expected_head.saturating_sub(expected_durable)
    );
}

fn assert_first_latched_matches(runtime: &RuntimeControl, error: &StorageError) {
    let stats = runtime.stats();
    let latched = stats
        .first_latched_error
        .expect("the first terminal write error must be published");
    assert_eq!(latched.kind, error.kind);
    assert_eq!(latched.operation, error.operation);
    assert_eq!(latched.protocol_stage, error.protocol_stage);
    assert_eq!(latched.retry_advice, error.retry_advice);
    assert_eq!(latched.os_code, error.os_code);
    assert_eq!(latched.commit_seq, error.commit_seq);
    assert_eq!(latched.vlog_file_id, error.vlog_file_id);
    assert_eq!(latched.vlog_offset, error.vlog_offset);
}

fn empty_frontier() -> DurableFrontier {
    DurableFrontier {
        durable_seq: 0,
        durable_vlog_end: DurableVLogEnd::Empty,
    }
}

fn batch_has_frontier(batch: &IndexAtomicBatch) -> bool {
    batch.operations().iter().any(|mutation| {
        matches!(
            mutation,
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key,
                ..
            } if key == DURABLE_FRONTIER_KEY
        )
    })
}

fn inspect_nonempty_batch<'a>(
    batch: &'a IndexAtomicBatch,
    distinct_key_count: usize,
    expect_frontier: bool,
) -> TestResult<(
    &'a [IndexMutation],
    TransactionDescriptor,
    Option<DurableFrontier>,
)> {
    let operations = batch.operations();
    let head_index = distinct_key_count
        .checked_mul(2)
        .and_then(|index| index.checked_add(1))
        .expect("small test batch index");
    let expected_len = head_index + 1 + usize::from(expect_frontier);
    assert_eq!(operations.len(), expected_len);

    let (meta_key, meta_value) = match &operations[distinct_key_count] {
        IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key,
            value,
        } => (key.as_slice(), value.as_slice()),
        mutation => panic!("expected TxMeta mutation, got {mutation:?}"),
    };
    let mut encoded_mutations = Vec::new();
    for mutation in &operations[distinct_key_count + 1..head_index] {
        match mutation {
            IndexMutation::PutInternal {
                space: InternalIndexSpace::Transaction,
                key,
                value,
            } => encoded_mutations.push((key.as_slice(), value.as_slice())),
            mutation => panic!("expected TxMutation, got {mutation:?}"),
        }
    }
    assert_eq!(encoded_mutations.len(), distinct_key_count);
    let descriptor = decode_descriptor(meta_key, meta_value, &encoded_mutations)?;

    let encoded_head = match &operations[head_index] {
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key,
            value,
        } if key == HEAD_SEQ_KEY => value,
        mutation => panic!("expected HeadSeq mutation, got {mutation:?}"),
    };
    assert_eq!(decode_head_seq(encoded_head)?, descriptor.meta.commit_seq);

    let frontier = if expect_frontier {
        let encoded_frontier = match &operations[head_index + 1] {
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key,
                value,
            } if key == DURABLE_FRONTIER_KEY => value,
            mutation => panic!("expected DurableFrontier mutation, got {mutation:?}"),
        };
        let frontier = DurableFrontier::decode(encoded_frontier)?;
        assert_eq!(frontier.durable_seq, descriptor.meta.commit_seq);
        assert_eq!(
            frontier.durable_vlog_end,
            DurableVLogEnd::Position(descriptor.meta.vlog_end)
        );
        Some(frontier)
    } else {
        None
    };

    Ok((&operations[..distinct_key_count], descriptor, frontier))
}

fn assert_descriptor_shape(
    descriptor: &TransactionDescriptor,
    commit_seq: u64,
    prev_seq: u64,
    logical_op_count: u64,
    distinct_key_count: u64,
) {
    assert_eq!(descriptor.meta.commit_seq, commit_seq);
    assert_eq!(descriptor.meta.prev_seq, prev_seq);
    assert_eq!(descriptor.meta.logical_op_count, logical_op_count);
    assert_eq!(descriptor.meta.distinct_key_count, distinct_key_count);
    assert_eq!(descriptor.mutations.len(), distinct_key_count as usize);
    assert_ne!(descriptor.meta.tx_uuid.0, [0; 16]);
    assert_ne!(descriptor.meta.vlog_begin, descriptor.meta.vlog_end);
}

#[test]
fn fake_backend_batches_exactly_encode_put_delete_batch_and_sync_shapes() -> TestResult {
    let harness = FakeHarness::new(WriteFailure::None)?;

    let put = preflight_put(b"shared", b"put-value", false)?;
    harness.coordinator.commit_nonempty(&put)?;

    let delete = preflight_delete(b"shared", false)?;
    harness.coordinator.commit_nonempty(&delete)?;

    let mut batch = WriteBatch::new();
    batch.put(b"batch-a", b"first")?;
    batch.delete(b"batch-b")?;
    batch.put(b"batch-a", b"final")?;
    let batch_write = preflight_batch(&batch, false)?;
    harness.coordinator.commit_nonempty(&batch_write)?;

    let sync_put = preflight_put(b"durable", b"sync-value", true)?;
    harness.coordinator.commit_nonempty(&sync_put)?;

    let calls = harness.backend.calls();
    assert_eq!(calls.len(), 4);
    assert_eq!(
        calls.iter().map(|call| call.1).collect::<Vec<_>>(),
        vec![
            IndexCommitMode::Buffer,
            IndexCommitMode::Buffer,
            IndexCommitMode::Buffer,
            IndexCommitMode::SyncAll,
        ]
    );

    let (put_users, put_descriptor, put_frontier) = inspect_nonempty_batch(&calls[0].0, 1, false)?;
    assert_descriptor_shape(&put_descriptor, 1, 0, 1, 1);
    assert!(put_frontier.is_none());
    let put_pointer = match &put_users[0] {
        IndexMutation::PutUser {
            user_key,
            encoded_pointer,
        } if user_key == b"shared" => ValuePointer::decode(encoded_pointer)?,
        mutation => panic!("expected shared PutUser, got {mutation:?}"),
    };
    assert_eq!(
        put_descriptor.mutations,
        vec![TxMutation {
            user_key: b"shared".to_vec(),
            before_state: ValueState::Absent,
            after_state: ValueState::Present(put_pointer),
        }]
    );

    let (delete_users, delete_descriptor, delete_frontier) =
        inspect_nonempty_batch(&calls[1].0, 1, false)?;
    assert_descriptor_shape(&delete_descriptor, 2, 1, 1, 1);
    assert!(delete_frontier.is_none());
    assert!(matches!(
        &delete_users[0],
        IndexMutation::DeleteUser { user_key } if user_key == b"shared"
    ));
    assert_eq!(
        delete_descriptor.mutations,
        vec![TxMutation {
            user_key: b"shared".to_vec(),
            before_state: ValueState::Present(put_pointer),
            after_state: ValueState::Absent,
        }]
    );

    let (batch_users, batch_descriptor, batch_frontier) =
        inspect_nonempty_batch(&calls[2].0, 2, false)?;
    assert_descriptor_shape(&batch_descriptor, 3, 2, 3, 2);
    assert!(batch_frontier.is_none());
    let batch_pointer = match &batch_users[0] {
        IndexMutation::PutUser {
            user_key,
            encoded_pointer,
        } if user_key == b"batch-a" => ValuePointer::decode(encoded_pointer)?,
        mutation => panic!("expected batch-a PutUser, got {mutation:?}"),
    };
    assert!(matches!(
        &batch_users[1],
        IndexMutation::DeleteUser { user_key } if user_key == b"batch-b"
    ));
    assert_eq!(
        batch_descriptor.mutations,
        vec![
            TxMutation {
                user_key: b"batch-a".to_vec(),
                before_state: ValueState::Absent,
                after_state: ValueState::Present(batch_pointer),
            },
            TxMutation {
                user_key: b"batch-b".to_vec(),
                before_state: ValueState::Absent,
                after_state: ValueState::Absent,
            },
        ]
    );

    let (sync_users, sync_descriptor, sync_frontier) =
        inspect_nonempty_batch(&calls[3].0, 1, true)?;
    assert_descriptor_shape(&sync_descriptor, 4, 3, 1, 1);
    assert_eq!(sync_frontier.expect("sync frontier").durable_seq, 4);
    let sync_pointer = match &sync_users[0] {
        IndexMutation::PutUser {
            user_key,
            encoded_pointer,
        } if user_key == b"durable" => ValuePointer::decode(encoded_pointer)?,
        mutation => panic!("expected durable PutUser, got {mutation:?}"),
    };
    assert_eq!(
        sync_descriptor.mutations,
        vec![TxMutation {
            user_key: b"durable".to_vec(),
            before_state: ValueState::Absent,
            after_state: ValueState::Present(sync_pointer),
        }]
    );
    Ok(())
}

#[test]
fn coordinator_rejects_unconverged_reopened_head_and_accepts_stable_state() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let vlog_path = temporary.path().join("vlog");
    std::fs::create_dir(&vlog_path)?;
    let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
    let catalog = Arc::new(FileCatalog::new());
    let backend = Arc::new(FakeBackend::new(Arc::new(Mutex::new(Vec::new()))));
    let mut writer = ValueLogWriter::empty(
        Arc::clone(&directory),
        DATABASE_UUID,
        VLogGeometry::PRODUCTION,
        Arc::clone(&catalog),
    )?;
    let write = preflight_put(b"seed", b"value", false)?;
    let mut uuid_source = FixedUuid(0x51);
    let prepared = prepare_commit(
        &write,
        DATABASE_UUID,
        0,
        writer.position(),
        writer.geometry(),
        backend.as_ref(),
        &mut uuid_source,
    )?;
    writer.append(&prepared.envelope)?;
    let accepted_end = prepared.envelope.vlog_end;
    let synced = writer.sync_through(1, Some(accepted_end))?;
    writer.frontier_succeeded(synced)?;
    drop(writer);

    let reopened = ValueLogWriter::open(
        Arc::clone(&directory),
        DATABASE_UUID,
        VLogGeometry::PRODUCTION,
        Arc::clone(&catalog),
        Some(accepted_end),
    )?;
    assert!(reopened.dirty_state().dirty_files.is_empty());
    assert!(reopened.dirty_state().pending_directory_entries.is_empty());
    let stats = Arc::new(StatsState::new());
    let runtime = RuntimeControl::new(Arc::clone(&stats));
    let error = CommitCoordinator::new(
        Arc::clone(&runtime),
        Arc::clone(&stats),
        Arc::clone(&backend),
        reopened,
        FixedUuid(0x61),
        1,
        empty_frontier(),
        Some(accepted_end),
    )
    .err()
    .expect("H>D must be rejected before the runtime is opened");
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);

    let reopened = ValueLogWriter::open(
        directory,
        DATABASE_UUID,
        VLogGeometry::PRODUCTION,
        catalog,
        Some(accepted_end),
    )?;
    let stable_frontier = DurableFrontier {
        durable_seq: 1,
        durable_vlog_end: DurableVLogEnd::Position(VLogPos {
            file_id: accepted_end.file_id,
            offset: accepted_end.offset,
        }),
    };
    let coordinator = CommitCoordinator::new(
        runtime,
        stats,
        backend,
        reopened,
        FixedUuid(0x71),
        1,
        stable_frontier,
        Some(accepted_end),
    )?;
    assert_eq!(
        coordinator.state_snapshot(),
        commit::CommitStateSnapshot {
            head_seq: 1,
            durable_seq: 1,
            head_vlog_end: Some(accepted_end),
            durable_vlog_end: Some(accepted_end),
        }
    );
    Ok(())
}

#[test]
fn buffer_then_syncall_publish_one_atomic_batch_each_in_protocol_order() -> TestResult {
    let harness = FakeHarness::new(WriteFailure::None)?;

    let buffer = preflight_put(b"buffer", b"one", false)?;
    harness.coordinator.commit_nonempty(&buffer)?;
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 1);
    assert_eq!(state.durable_seq, 0);
    assert!(state.head_vlog_end.is_some());
    assert!(state.durable_vlog_end.is_none());
    let calls = harness.backend.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, IndexCommitMode::Buffer);
    assert!(!batch_has_frontier(&calls[0].0));
    assert_eq!(harness.backend.transaction_len(), 2, "TxMeta + TxMutation");
    assert!(
        !harness
            .coordinator
            .dirty_state_for_test()
            .dirty_files
            .is_empty()
    );
    let events = harness.events.lock().unwrap().clone();
    assert!(matches!(
        events.last(),
        Some(Event::IndexCommit(IndexCommitMode::Buffer))
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::VLogFileSync))
    );

    harness.events.lock().unwrap().clear();
    let durable = preflight_put(b"durable", b"two", true)?;
    harness.coordinator.commit_nonempty(&durable)?;
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 2);
    assert_eq!(state.durable_seq, 2);
    assert_eq!(state.head_vlog_end, state.durable_vlog_end);
    let calls = harness.backend.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].1, IndexCommitMode::SyncAll);
    assert!(batch_has_frontier(&calls[1].0));
    assert!(
        harness
            .coordinator
            .dirty_state_for_test()
            .dirty_files
            .is_empty()
    );
    assert!(
        harness
            .coordinator
            .dirty_state_for_test()
            .pending_directory_entries
            .is_empty()
    );
    let events = harness.events.lock().unwrap().clone();
    let index_position = events
        .iter()
        .position(|event| matches!(event, Event::IndexCommit(IndexCommitMode::SyncAll)))
        .unwrap();
    assert!(
        events[..index_position]
            .iter()
            .any(|event| matches!(event, Event::VLogFileSync))
    );
    assert!(
        events[..index_position]
            .iter()
            .any(|event| matches!(event, Event::VLogDirectorySync))
    );

    let stats = harness.stats.snapshot();
    assert_eq!(stats.head_seq, 2);
    assert_eq!(stats.durable_seq, 2);
    assert_eq!(stats.durability_lag, 0);
    assert!(stats.durable_vlog_end.is_some());
    Ok(())
}

#[test]
fn preappend_and_partial_append_failures_never_call_index() -> TestResult {
    let retryable = FakeHarness::new(WriteFailure::AlwaysWouldBlock)?;
    let write = preflight_put(b"key", b"value", false)?;
    let error = retryable.coordinator.commit_nonempty(&write).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogAppend);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(InstanceState::Healthy));
    assert_eq!(retryable.backend.calls().len(), 0);
    assert_eq!(retryable.coordinator.state_snapshot().head_seq, 0);

    let partial = FakeHarness::new(WriteFailure::PartialThenEio)?;
    let write = preflight_put(b"key", b"value", false)?;
    let error = partial.coordinator.commit_nonempty(&write).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.protocol_stage, ProtocolStage::VLogAppend);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    assert_eq!(error.retry_advice, RetryAdvice::ReopenAndVerify);
    assert_eq!(partial.backend.calls().len(), 0);
    assert_eq!(
        partial.runtime.state().instance_state,
        InstanceState::Poisoned
    );
    assert_first_latched_matches(&partial.runtime, &error);
    let stats = partial.runtime.stats();
    let actual_vlog_bytes = std::fs::metadata(
        partial
            ._temporary
            .path()
            .join("vlog")
            .join(vlog::file_set::vlog_file_name(0)?),
    )?
    .len();
    assert!(actual_vlog_bytes > 0);
    assert_eq!(stats.head_seq, 0);
    assert_eq!(stats.durable_seq, 0);
    assert_eq!(stats.active_vlog_file_id, Some(0));
    assert_eq!(stats.vlog_file_count, 1);
    assert_eq!(stats.vlog_logical_bytes, actual_vlog_bytes);
    Ok(())
}

#[test]
fn coordinator_preserves_preclassified_preflight_retry_in_error_and_latched_summary() -> TestResult
{
    for kind in [
        StorageErrorKind::InvalidArgument,
        StorageErrorKind::NotFound,
        StorageErrorKind::Unsupported,
        StorageErrorKind::CapacityExceeded,
    ] {
        let harness = FakeHarness::new(WriteFailure::None)?;
        harness.backend.set_user_read_failure(kind);
        let write = preflight_put(b"key", b"value", false)?;

        let error = harness.coordinator.commit_nonempty(&write).unwrap_err();

        assert_eq!(error.kind, kind);
        assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
        assert_eq!(error.retry_advice, RetryAdvice::DoNotRetry);
        assert!(harness.events.lock().unwrap().is_empty());
        assert!(harness.backend.calls().is_empty());
        assert_first_latched_matches(&harness.runtime, &error);
    }
    Ok(())
}

#[test]
fn index_failures_cover_every_not_applied_and_commit_unknown_kind() -> TestResult {
    let kinds = [
        StorageErrorKind::InvalidArgument,
        StorageErrorKind::NotFound,
        StorageErrorKind::Busy,
        StorageErrorKind::Unsupported,
        StorageErrorKind::ResourceExhausted,
        StorageErrorKind::CapacityExceeded,
        StorageErrorKind::Io,
        StorageErrorKind::Corruption,
        StorageErrorKind::InvalidLayout,
        StorageErrorKind::IncompatibleFormat,
        StorageErrorKind::StorageWriteStopped,
        StorageErrorKind::StoragePoisoned,
        StorageErrorKind::Unrecoverable,
    ];

    for kind in kinds {
        let harness = FakeHarness::new(WriteFailure::None)?;
        harness.backend.set_failure(CommitFailure::NotApplied(kind));
        let write = preflight_put(b"key", b"value", false)?;
        let error = harness.coordinator.commit_nonempty(&write).unwrap_err();
        let (expected_state, expected_retry) = match kind {
            StorageErrorKind::Io | StorageErrorKind::StoragePoisoned => {
                (InstanceState::Poisoned, RetryAdvice::ReopenAndVerify)
            }
            StorageErrorKind::Corruption
            | StorageErrorKind::InvalidLayout
            | StorageErrorKind::Unrecoverable => {
                (InstanceState::Poisoned, RetryAdvice::RestoreOrRepair)
            }
            StorageErrorKind::IncompatibleFormat => {
                (InstanceState::Poisoned, RetryAdvice::DoNotRetry)
            }
            _ => (
                InstanceState::WriteStopped,
                RetryAdvice::FixEnvironmentAndReopen,
            ),
        };
        assert_eq!(error.kind, kind);
        assert_eq!(error.protocol_stage, ProtocolStage::IndexCommit);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(expected_state));
        assert_eq!(error.retry_advice, expected_retry);
        assert_eq!(error.os_code, (kind == StorageErrorKind::Io).then_some(5));
        assert_eq!(harness.coordinator.state_snapshot().head_seq, 0);
        assert_eq!(harness.backend.calls().len(), 1);
        assert_first_latched_matches(&harness.runtime, &error);
    }

    for kind in kinds {
        let harness = FakeHarness::new(WriteFailure::None)?;
        harness.backend.set_failure(CommitFailure::Unknown(kind));
        let write = preflight_put(b"key", b"value", false)?;
        let error = harness.coordinator.commit_nonempty(&write).unwrap_err();
        assert_eq!(error.kind, kind);
        assert_eq!(error.protocol_stage, ProtocolStage::IndexCommit);
        assert_eq!(error.write_outcome, Some(WriteOutcome::CommitUnknown));
        assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
        assert_eq!(error.retry_advice, RetryAdvice::ReopenAndVerify);
        assert_eq!(error.os_code, (kind == StorageErrorKind::Io).then_some(5));
        assert!(error.commit_seq.is_some());
        assert!(error.tx_uuid.is_some());
        assert_eq!(harness.coordinator.state_snapshot().head_seq, 0);
        assert_eq!(harness.backend.calls().len(), 1);
        assert_first_latched_matches(&harness.runtime, &error);
    }
    Ok(())
}

#[test]
fn active_failure_cancels_a_queued_follower_without_bypass() -> TestResult {
    let harness = FakeHarness::new(WriteFailure::None)?;
    harness.backend.set_failure(CommitFailure::NotApplied(
        StorageErrorKind::ResourceExhausted,
    ));
    harness.backend.enable_block();

    let first_coordinator = Arc::clone(&harness.coordinator);
    let first = thread::spawn(move || {
        let write = preflight_put(b"first", b"1", false).unwrap();
        first_coordinator.commit_nonempty(&write)
    });
    harness.backend.wait_until_entered();

    let follower_coordinator = Arc::clone(&harness.coordinator);
    let follower = thread::spawn(move || {
        let write = preflight_put(b"second", b"2", false).unwrap();
        follower_coordinator.commit_nonempty(&write)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while harness.runtime.queued_write_count_for_test() != 1 {
        assert!(Instant::now() < deadline, "follower was not queued");
        thread::yield_now();
    }
    harness.backend.release();

    let first_error = first.join().unwrap().unwrap_err();
    let follower_error = follower.join().unwrap().unwrap_err();
    assert_eq!(first_error.protocol_stage, ProtocolStage::IndexCommit);
    assert_eq!(first_error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(follower_error.protocol_stage, ProtocolStage::Admission);
    assert_eq!(
        follower_error.write_outcome,
        Some(WriteOutcome::NotCommitted)
    );
    assert_eq!(
        follower_error.instance_state,
        Some(InstanceState::WriteStopped)
    );
    assert_eq!(harness.backend.calls().len(), 1);
    Ok(())
}

#[test]
fn real_buffer_commit_is_readable_before_any_durability_barrier() -> TestResult {
    let harness = RealCommitHarness::new(VLogGeometry::PRODUCTION)?;
    let key = b"buffer-visible";
    let value = b"visible-before-frontier-sync";

    harness.put(key, value, false)?;

    assert_commit_state(&harness, 1, 0, true);
    assert_persisted_state(&harness, 1, 0)?;
    let state_before_read = harness.coordinator.state_snapshot();
    assert!(state_before_read.head_vlog_end.is_some());
    assert!(state_before_read.durable_vlog_end.is_none());

    let encoded_pointer = harness
        .backend
        .get_user(key, None)?
        .expect("Buffer commit must publish its User Index pointer");
    let pointer = ValuePointer::decode(&encoded_pointer)?;
    assert_eq!(usize::from(pointer.value_len), value.len());

    let (_files, reader) = harness.reader(1)?;
    assert_eq!(reader.read_value(&encoded_pointer, key)?, value);

    // Reading must not turn the Buffer commit into a durability barrier.
    assert_eq!(harness.coordinator.state_snapshot(), state_before_read);
    assert_commit_state(&harness, 1, 0, true);
    assert_eq!(harness.descriptor_entry_count()?, 2);
    Ok(())
}

#[test]
fn t1_production_geometry_all_write_shapes_reconcile_through_real_reader() -> TestResult {
    let harness = RealCommitHarness::new(VLogGeometry::PRODUCTION)?;
    let empty_key = b"empty-value";
    let boundary_key = b"boundary-key";
    let boundary_value = vec![0xa5; 60_000 - boundary_key.len()];
    let missing_key = b"missing-key";
    let repeated_key = b"repeated-key";
    let rewrite_key = b"rewrite-key";
    let overwrite_key = b"overwrite-key";

    harness.put(empty_key, b"", false)?;
    harness.put(boundary_key, &boundary_value, false)?;
    harness.delete(missing_key, false)?;

    let mut repeated = WriteBatch::new();
    repeated.put(repeated_key, b"batch-first")?;
    repeated.put(repeated_key, b"batch-second")?;
    repeated.delete(repeated_key)?;
    repeated.put(repeated_key, b"batch-final")?;
    harness.write(&repeated, false)?;

    harness.put(rewrite_key, b"before-delete", false)?;
    harness.delete(rewrite_key, false)?;
    harness.put(rewrite_key, b"after-delete", false)?;
    harness.put(overwrite_key, b"first-version", false)?;
    harness.put(overwrite_key, b"second-version", false)?;

    assert_commit_state(&harness, 9, 0, true);
    harness.barrier()?;
    assert_commit_state(&harness, 9, 9, false);
    assert_persisted_state(&harness, 9, 9)?;

    let expected = vec![
        (empty_key.to_vec(), Some(Vec::new())),
        (boundary_key.to_vec(), Some(boundary_value)),
        (missing_key.to_vec(), None),
        (repeated_key.to_vec(), Some(b"batch-final".to_vec())),
        (rewrite_key.to_vec(), Some(b"after-delete".to_vec())),
        (overwrite_key.to_vec(), Some(b"second-version".to_vec())),
    ];
    let (_files, reader) = harness.reader(4)?;
    assert_terminal_values(&harness, &reader, &expected)?;

    // Nine nonempty transactions, each with one distinct key:
    // one TxMeta + one TxMutation per transaction.
    assert_eq!(harness.descriptor_entry_count()?, 9 * 2);
    Ok(())
}

#[test]
fn t2_small_geometry_twenty_mixed_operations_cross_pages_and_files() -> TestResult {
    let geometry = VLogGeometry::test_only(256, 512, 5)?;
    let harness = RealCommitHarness::new(geometry)?;

    // Twenty separate minimal envelopes cannot fit in the requested six 512-byte
    // files. Keep the requested operation volume and exercise three real compound
    // transactions so the same coordinator path necessarily crosses pages/files.
    let mut first = WriteBatch::new();
    first.put(b"a", b"A1")?;
    first.put(b"b", b"B1")?;
    first.delete(b"c")?;
    first.put(b"d", b"D1")?;
    first.put(b"e", b"E1")?;
    first.delete(b"f")?;
    first.put(b"g", b"G1")?;
    harness.write(&first, false)?;

    let mut second = WriteBatch::new();
    second.put(b"a", b"A2")?;
    second.delete(b"b")?;
    second.put(b"c", b"C2")?;
    second.delete(b"d")?;
    second.put(b"f", b"F2")?;
    second.put(b"h", b"H2")?;
    second.delete(b"g")?;
    harness.write(&second, false)?;

    let mut third = WriteBatch::new();
    third.delete(b"a")?;
    third.put(b"b", b"B3")?;
    third.put(b"d", b"D3")?;
    third.delete(b"e")?;
    third.put(b"g", b"G3")?;
    third.put(b"h", b"H3")?;
    harness.write(&third, false)?;

    let before_barrier = harness.coordinator.state_snapshot();
    assert_eq!(before_barrier.head_seq, 3);
    assert_eq!(before_barrier.durable_seq, 0);
    assert!(
        before_barrier
            .head_vlog_end
            .is_some_and(|end| end.file_id >= 1),
        "the test must cross at least one VLog file boundary"
    );
    harness.barrier()?;
    assert_commit_state(&harness, 3, 3, false);

    let expected = vec![
        (b"a".to_vec(), None),
        (b"b".to_vec(), Some(b"B3".to_vec())),
        (b"c".to_vec(), Some(b"C2".to_vec())),
        (b"d".to_vec(), Some(b"D3".to_vec())),
        (b"e".to_vec(), None),
        (b"f".to_vec(), Some(b"F2".to_vec())),
        (b"g".to_vec(), Some(b"G3".to_vec())),
        (b"h".to_vec(), Some(b"H3".to_vec())),
    ];
    let (_files, reader) = harness.reader(2)?;
    assert_terminal_values(&harness, &reader, &expected)?;
    assert_eq!(harness.descriptor_entry_count()?, 8 + 8 + 7);
    Ok(())
}

#[test]
fn t3_small_cache_reads_ten_large_transactions_across_three_or_more_files() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 4)?;
    let harness = RealCommitHarness::new(geometry)?;
    let cases = [
        (b"large-00".as_slice(), 0x10),
        (b"large-01".as_slice(), 0x21),
        (b"large-02".as_slice(), 0x32),
        (b"large-03".as_slice(), 0x43),
        (b"large-04".as_slice(), 0x54),
        (b"large-05".as_slice(), 0x65),
        (b"large-06".as_slice(), 0x76),
        (b"large-07".as_slice(), 0x87),
        (b"large-08".as_slice(), 0x98),
        (b"large-09".as_slice(), 0xa9),
    ];
    let mut expected = Vec::new();
    for (key, fill) in cases {
        let value = vec![fill; 40_000];
        harness.put(key, &value, false)?;
        expected.push((key.to_vec(), Some(value)));
    }
    harness.barrier()?;
    assert_commit_state(&harness, 10, 10, false);

    let mut file_ids = BTreeSet::new();
    for (key, _) in &expected {
        let encoded_pointer = harness
            .backend
            .get_user(key, None)?
            .expect("large value pointer");
        file_ids.insert(ValuePointer::decode(&encoded_pointer)?.file_id);
    }
    assert!(
        file_ids.len() >= 3,
        "large-value pointers must span at least three files: {file_ids:?}"
    );
    assert!(
        harness
            .coordinator
            .state_snapshot()
            .head_vlog_end
            .is_some_and(|end| end.file_id >= 2)
    );

    let (files, reader) = harness.reader(2)?;
    assert_terminal_values(&harness, &reader, &expected)?;
    assert_eq!(files.cache_len()?, 2);
    let first_key = cases[0].0;
    let first_pointer = harness
        .backend
        .get_user(first_key, None)?
        .expect("first large value pointer");
    let first_file_id = ValuePointer::decode(&first_pointer)?.file_id;
    assert!(
        !files.cache_order()?.contains(&first_file_id),
        "the first file must have been evicted before the reopen check"
    );
    assert_eq!(
        reader.read_value(&first_pointer, first_key)?,
        vec![cases[0].1; 40_000]
    );
    assert!(files.cache_order()?.contains(&first_file_id));
    assert_eq!(files.cache_len()?, 2);
    assert_eq!(harness.descriptor_entry_count()?, 10 * 2);
    Ok(())
}

#[test]
fn t4_two_barriers_advance_exact_prefixes_and_preserve_terminal_values() -> TestResult {
    let harness = RealCommitHarness::new(VLogGeometry::PRODUCTION)?;

    harness.put(b"barrier-a", b"a1", false)?;
    harness.put(b"barrier-b", b"b1", false)?;
    harness.put(b"barrier-c", b"c1", false)?;
    assert_commit_state(&harness, 3, 0, true);

    harness.barrier()?;
    assert_commit_state(&harness, 3, 3, false);
    assert_persisted_state(&harness, 3, 3)?;

    harness.put(b"barrier-a", b"a2", false)?;
    harness.delete(b"barrier-b", false)?;
    assert_commit_state(&harness, 5, 3, true);
    assert_persisted_state(&harness, 5, 3)?;

    harness.barrier()?;
    assert_commit_state(&harness, 5, 5, false);
    assert_persisted_state(&harness, 5, 5)?;

    let expected = vec![
        (b"barrier-a".to_vec(), Some(b"a2".to_vec())),
        (b"barrier-b".to_vec(), None),
        (b"barrier-c".to_vec(), Some(b"c1".to_vec())),
    ];
    let (_files, reader) = harness.reader(2)?;
    assert_terminal_values(&harness, &reader, &expected)?;
    assert_eq!(harness.descriptor_entry_count()?, 5 * 2);
    Ok(())
}

#[test]
fn real_vlog_and_real_fjall_execute_one_sync_compound_commit() -> TestResult {
    let harness = RealCommitHarness::new(VLogGeometry::PRODUCTION)?;
    harness.put(b"real-key", b"real-value", true)?;

    let (_files, reader) = harness.reader(4)?;
    let expected = vec![(b"real-key".to_vec(), Some(b"real-value".to_vec()))];
    assert_terminal_values(&harness, &reader, &expected)?;
    assert_persisted_state(&harness, 1, 1)?;
    assert_eq!(harness.descriptor_entry_count()?, 2);
    Ok(())
}

fn fjall_options() -> FjallIndexOptions {
    FjallIndexOptions {
        write_buffer_size: 4 * 1024 * 1024,
        max_open_files: 1000,
        block_cache_size: 8 * 1024 * 1024,
        block_size: 4 * 1024,
        block_restart_interval: 16,
        max_file_size: 2 * 1024 * 1024,
        compression: IndexCompression::None,
    }
}
