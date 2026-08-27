#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

use commit::{CommitCoordinator, DurableFrontier, DurableVLogEnd, TxUuidSource, preflight_put};
use index::{
    DURABLE_FRONTIER_KEY, IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode,
    IndexEntry, IndexMutation, InternalIndexError, InternalIndexSpace, InternalKeyRange,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, VLogDirectory};
use vlog::format::VLogGeometry;
use vlog::writer::{ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x62; 16];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SyncFailure {
    None,
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Write,
    FileSync,
    DirectorySync,
    Index(IndexCommitMode),
}

struct BarrierWriterIo {
    failure: SyncFailure,
    events: Arc<Mutex<Vec<Event>>>,
}

impl WriterIo for BarrierWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.events.lock().unwrap().push(Event::Write);
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        self.events.lock().unwrap().push(Event::FileSync);
        if self.failure == SyncFailure::File {
            Err(io::Error::from_raw_os_error(5))
        } else {
            file.sync_data()
        }
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        self.events.lock().unwrap().push(Event::DirectorySync);
        if self.failure == SyncFailure::Directory {
            Err(io::Error::from_raw_os_error(5))
        } else {
            directory.sync()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendFailure {
    None,
    NotApplied(StorageErrorKind),
    Unknown(StorageErrorKind),
}

#[derive(Default)]
struct BackendData {
    user: BTreeMap<Vec<u8>, Vec<u8>>,
    transaction: BTreeMap<Vec<u8>, Vec<u8>>,
    system: BTreeMap<Vec<u8>, Vec<u8>>,
    calls: Vec<(IndexAtomicBatch, IndexCommitMode)>,
}

struct BarrierBackend {
    data: Mutex<BackendData>,
    failure: Mutex<BackendFailure>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl BarrierBackend {
    fn new(events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            data: Mutex::new(BackendData::default()),
            failure: Mutex::new(BackendFailure::None),
            events,
        }
    }

    fn set_failure(&self, failure: BackendFailure) {
        *self.failure.lock().unwrap() = failure;
    }

    fn calls(&self) -> Vec<(IndexAtomicBatch, IndexCommitMode)> {
        self.data.lock().unwrap().calls.clone()
    }

    fn transaction_len(&self) -> usize {
        self.data.lock().unwrap().transaction.len()
    }
}

impl IndexBackend for BarrierBackend {
    type Snapshot = BTreeMap<Vec<u8>, Vec<u8>>;
    type UserIterator = std::vec::IntoIter<Result<IndexEntry>>;
    type InternalIterator = std::vec::IntoIter<Result<IndexEntry>>;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        batch.validate_for_commit(mode)?;
        self.events.lock().unwrap().push(Event::Index(mode));
        self.data.lock().unwrap().calls.push((batch.clone(), mode));
        match *self.failure.lock().unwrap() {
            BackendFailure::NotApplied(kind) => {
                return Err(IndexCommitError::not_applied(InternalIndexError::new(
                    kind,
                    (kind == StorageErrorKind::Io).then_some(5),
                )));
            }
            BackendFailure::Unknown(kind) => {
                return Err(IndexCommitError::unknown(InternalIndexError::new(
                    kind,
                    (kind == StorageErrorKind::Io).then_some(5),
                )));
            }
            BackendFailure::None => {}
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
                    map_mut(&mut data, space).insert(key, value);
                }
                IndexMutation::DeleteInternal { space, key } => {
                    map_mut(&mut data, space).remove(&key);
                }
            }
        }
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_user(&self, key: &[u8], _snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        Ok(self.data.lock().unwrap().user.get(key).cloned())
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.lock().unwrap();
        Ok(map(&data, space).get(key).cloned())
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        _range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        let data = self.data.lock().unwrap();
        Ok(map(&data, space)
            .iter()
            .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
            .collect::<Vec<_>>()
            .into_iter())
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(self.data.lock().unwrap().user.clone())
    }

    fn iter_user(&self, _snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        Ok(Vec::new().into_iter())
    }
}

fn map(data: &BackendData, space: InternalIndexSpace) -> &BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &data.transaction,
        InternalIndexSpace::System => &data.system,
    }
}

fn map_mut(data: &mut BackendData, space: InternalIndexSpace) -> &mut BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &mut data.transaction,
        InternalIndexSpace::System => &mut data.system,
    }
}

struct CountingUuid {
    calls: Arc<AtomicUsize>,
}

impl TxUuidSource for CountingUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        output.fill(u8::try_from(call).unwrap_or(0x7f));
        Ok(())
    }
}

struct Harness {
    _temporary: TempDir,
    coordinator: CommitCoordinator<BarrierBackend, CountingUuid>,
    backend: Arc<BarrierBackend>,
    runtime: Arc<RuntimeControl>,
    stats: Arc<StatsState>,
    uuid_calls: Arc<AtomicUsize>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl Harness {
    fn new(sync_failure: SyncFailure) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let events = Arc::new(Mutex::new(Vec::new()));
        let backend = Arc::new(BarrierBackend::new(Arc::clone(&events)));
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let writer = ValueLogWriter::empty_with_io(
            Arc::new(VLogDirectory::open(&vlog_path)?),
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::new(FileCatalog::new()),
            Arc::new(BarrierWriterIo {
                failure: sync_failure,
                events: Arc::clone(&events),
            }),
        )?;
        let uuid_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = CommitCoordinator::new(
            Arc::clone(&runtime),
            Arc::clone(&stats),
            Arc::clone(&backend),
            writer,
            CountingUuid {
                calls: Arc::clone(&uuid_calls),
            },
            0,
            DurableFrontier {
                durable_seq: 0,
                durable_vlog_end: DurableVLogEnd::Empty,
            },
            None,
        )?;
        Ok(Self {
            _temporary: temporary,
            coordinator,
            backend,
            runtime,
            stats,
            uuid_calls,
            events,
        })
    }

    fn buffer_one(&self) -> TestResult {
        let write = preflight_put(b"buffered", b"value", false)?;
        self.coordinator.commit_nonempty(&write)?;
        Ok(())
    }
}

fn is_frontier_only(batch: &IndexAtomicBatch) -> bool {
    matches!(
        batch.operations(),
        [IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key,
            ..
        }] if key == DURABLE_FRONTIER_KEY
    )
}

fn assert_first_latched_matches(runtime: &RuntimeControl, error: &StorageError) {
    let latched = runtime
        .stats()
        .first_latched_error
        .expect("the first terminal barrier error must be published");
    assert_eq!(latched.kind, error.kind);
    assert_eq!(latched.operation, error.operation);
    assert_eq!(latched.protocol_stage, error.protocol_stage);
    assert_eq!(latched.retry_advice, error.retry_advice);
    assert_eq!(latched.os_code, error.os_code);
    assert_eq!(latched.commit_seq, error.commit_seq);
    assert_eq!(latched.vlog_file_id, error.vlog_file_id);
    assert_eq!(latched.vlog_offset, error.vlog_offset);
}

#[test]
fn empty_nonsync_and_already_durable_empty_sync_do_no_io_or_identity_allocation() -> TestResult {
    let harness = Harness::new(SyncFailure::None)?;

    harness.coordinator.commit_empty_batch(false)?;
    harness.coordinator.commit_empty_batch(true)?;

    assert!(harness.events.lock().unwrap().is_empty());
    assert!(harness.backend.calls().is_empty());
    assert_eq!(harness.uuid_calls.load(Ordering::SeqCst), 0);
    assert_eq!(harness.runtime.queued_write_count_for_test(), 0);
    assert!(harness.runtime.active_request_for_test().is_none());
    assert_eq!(harness.coordinator.state_snapshot().head_seq, 0);
    assert_eq!(harness.coordinator.state_snapshot().durable_seq, 0);
    Ok(())
}

#[test]
fn empty_sync_advances_only_frontier_and_never_allocates_a_transaction() -> TestResult {
    let harness = Harness::new(SyncFailure::None)?;
    harness.buffer_one()?;
    assert_eq!(harness.uuid_calls.load(Ordering::SeqCst), 1);
    assert_eq!(harness.backend.transaction_len(), 2);
    harness.events.lock().unwrap().clear();
    let prior_calls = harness.backend.calls().len();

    harness.coordinator.commit_empty_batch(true)?;

    let calls = harness.backend.calls();
    assert_eq!(calls.len(), prior_calls + 1);
    let (batch, mode) = calls.last().unwrap();
    assert_eq!(*mode, IndexCommitMode::SyncAll);
    assert!(is_frontier_only(batch));
    let encoded_frontier = match &batch.operations()[0] {
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key,
            value,
        } if key == DURABLE_FRONTIER_KEY => value,
        mutation => panic!("expected frontier-only mutation, got {mutation:?}"),
    };
    let committed_frontier = DurableFrontier::decode(encoded_frontier)?;
    assert_eq!(committed_frontier.durable_seq, 1);
    assert_eq!(harness.backend.transaction_len(), 2);
    assert_eq!(harness.uuid_calls.load(Ordering::SeqCst), 1);
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 1);
    assert_eq!(state.durable_seq, 1);
    assert_eq!(state.head_vlog_end, state.durable_vlog_end);
    assert_eq!(
        committed_frontier.durable_vlog_end,
        state
            .durable_vlog_end
            .map(|end| commit::DurableVLogEnd::Position(commit::VLogPos {
                file_id: end.file_id,
                offset: end.offset,
            }))
            .expect("nonempty durable end")
    );
    assert!(
        harness
            .coordinator
            .dirty_state_for_test()
            .dirty_files
            .is_empty()
    );
    let events = harness.events.lock().unwrap().clone();
    assert!(matches!(
        events.last(),
        Some(Event::Index(IndexCommitMode::SyncAll))
    ));
    let stats = harness.stats.snapshot();
    assert_eq!(stats.head_seq, 1);
    assert_eq!(stats.durable_seq, 1);
    assert_eq!(stats.durability_lag, 0);

    harness.events.lock().unwrap().clear();
    let call_count = harness.backend.calls().len();
    harness.coordinator.commit_empty_batch(true)?;
    assert!(harness.events.lock().unwrap().is_empty());
    assert_eq!(harness.backend.calls().len(), call_count);
    Ok(())
}

#[test]
fn file_and_directory_sync_failures_poison_without_advancing_frontier() -> TestResult {
    for failure in [SyncFailure::File, SyncFailure::Directory] {
        let harness = Harness::new(failure)?;
        harness.buffer_one()?;
        let calls_before = harness.backend.calls().len();
        let transaction_before = harness.backend.transaction_len();

        let error = harness.coordinator.commit_empty_batch(true).unwrap_err();

        assert_eq!(error.kind, StorageErrorKind::Io);
        assert_eq!(error.operation, Operation::WriteBatch);
        assert_eq!(error.protocol_stage, ProtocolStage::VLogSync);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
        assert_eq!(error.retry_advice, RetryAdvice::ReopenAndVerify);
        assert_eq!(harness.backend.calls().len(), calls_before);
        assert_eq!(harness.backend.transaction_len(), transaction_before);
        let state = harness.coordinator.state_snapshot();
        assert_eq!(state.head_seq, 1);
        assert_eq!(state.durable_seq, 0);
        assert!(
            !harness
                .coordinator
                .dirty_state_for_test()
                .dirty_files
                .is_empty()
        );
        assert_eq!(
            harness.runtime.state().instance_state,
            InstanceState::Poisoned
        );
        assert_first_latched_matches(&harness.runtime, &error);
    }
    Ok(())
}

#[test]
fn every_frontier_commit_unknown_kind_requires_reopen_and_never_guesses_memory_state() -> TestResult
{
    for kind in [
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
    ] {
        let harness = Harness::new(SyncFailure::None)?;
        harness.buffer_one()?;
        harness.backend.set_failure(BackendFailure::Unknown(kind));

        let error = harness.coordinator.commit_empty_batch(true).unwrap_err();

        assert_eq!(error.kind, kind);
        assert_eq!(error.protocol_stage, ProtocolStage::DurableFrontier);
        assert_eq!(error.write_outcome, Some(WriteOutcome::CommitUnknown));
        assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
        assert_eq!(error.retry_advice, RetryAdvice::ReopenAndVerify);
        assert_eq!(error.os_code, (kind == StorageErrorKind::Io).then_some(5));
        assert_eq!(error.tx_uuid, None);
        let state = harness.coordinator.state_snapshot();
        assert_eq!(state.head_seq, 1);
        assert_eq!(state.durable_seq, 0);
        assert!(
            !harness
                .coordinator
                .dirty_state_for_test()
                .dirty_files
                .is_empty()
        );
        assert_first_latched_matches(&harness.runtime, &error);
    }
    Ok(())
}

#[test]
fn every_not_applied_frontier_kind_preserves_prior_ok_and_uses_safe_state() -> TestResult {
    for kind in [
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
    ] {
        let harness = Harness::new(SyncFailure::None)?;
        harness.buffer_one()?;
        harness
            .backend
            .set_failure(BackendFailure::NotApplied(kind));

        let error = harness.coordinator.commit_empty_batch(true).unwrap_err();
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
        assert_eq!(error.protocol_stage, ProtocolStage::DurableFrontier);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(expected_state));
        assert_eq!(error.retry_advice, expected_retry);
        assert_eq!(error.os_code, (kind == StorageErrorKind::Io).then_some(5));
        let state = harness.coordinator.state_snapshot();
        assert_eq!(state.head_seq, 1);
        assert_eq!(state.durable_seq, 0);
        assert_eq!(harness.backend.transaction_len(), 2);
        assert_first_latched_matches(&harness.runtime, &error);
    }
    Ok(())
}
