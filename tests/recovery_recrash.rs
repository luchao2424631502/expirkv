#![allow(dead_code, unused_imports)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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
pub(crate) use batch::WriteBatch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

mod db {
    use std::path::PathBuf;

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct VLogInventoryEntry {
        pub(crate) file_id: u32,
        pub(crate) len: u64,
        pub(crate) path: PathBuf,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub(crate) struct ManagedInventory {
        pub(crate) vlog_files: Vec<VLogInventoryEntry>,
    }
}

#[path = "../src/recovery/mod.rs"]
mod recovery;

use commit::{
    CommitCoordinator, DurableFrontier, DurableVLogEnd, RECOVERY_STATE_KEY, RecoveryPhase,
    RecoveryState, TransactionDescriptor, TxUuidSource, ValueState, decode_head_seq,
    encode_tx_meta_key, preflight_put,
};
use db::ManagedInventory;
use format::FormatMetadataV0;
use index::{
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, HEAD_SEQ_KEY, IndexApplyState,
    IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexCompression,
    IndexEntry, IndexMutation, InternalIndexError, InternalIndexSpace, InternalKeyRange,
    initialization_batch,
};
use lock::RootLock;
use recovery::{analyze_recovery, execute_recovery};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::ValuePointer;
use vlog::reader::ValueLogReader;
use vlog::writer::{ValueLogRecovery, ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x72; 16];
const PROCESS_CRASH_ENV: &str = "RUSTKV_RECOVERY_PROCESS_CRASH_CHILD";
const PROCESS_CRASH_PATH_ENV: &str = "RUSTKV_RECOVERY_PROCESS_CRASH_PATH";
const PROCESS_CRASH_POINT_ENV: &str = "RUSTKV_RECOVERY_PROCESS_CRASH_POINT";
const PROCESS_CRASH_EXIT_CODE: i32 = 89;

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CommitCrash {
    Before(usize),
    After(usize),
}

struct CrashBackend {
    inner: Arc<FjallBackend>,
    crash: CommitCrash,
    commits: AtomicUsize,
}

impl CrashBackend {
    fn new(inner: Arc<FjallBackend>, crash: CommitCrash) -> Self {
        Self {
            inner,
            crash,
            commits: AtomicUsize::new(0),
        }
    }

    fn should_crash(&self, call: usize, before: bool) -> bool {
        matches!(
            (self.crash, before),
            (CommitCrash::Before(expected), true) | (CommitCrash::After(expected), false)
                if call == expected
        )
    }
}

impl IndexBackend for CrashBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        let call = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if self.should_crash(call, true) {
            return Err(IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::Io,
                None,
            )));
        }
        self.inner.commit_atomic(batch, mode)?;
        if self.should_crash(call, false) {
            return Err(IndexCommitError::unknown(InternalIndexError::new(
                StorageErrorKind::Io,
                None,
            )));
        }
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        self.inner.get_database_identity()
    }

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        self.inner.get_user(key, snapshot)
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_internal(space, key)
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        self.inner.scan_internal(space, range)
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        self.inner.snapshot()
    }

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        self.inner.iter_user(snapshot)
    }
}

#[derive(Clone)]
enum IoCrash {
    None,
    Truncate,
    BoundarySync,
    Delete(usize),
    DirectorySync,
    TailRecheck(PathBuf),
}

struct CrashIo {
    crash: IoCrash,
    delete_calls: AtomicUsize,
}

impl CrashIo {
    fn normal() -> Self {
        Self::with(IoCrash::None)
    }

    fn with(crash: IoCrash) -> Self {
        Self {
            crash,
            delete_calls: AtomicUsize::new(0),
        }
    }
}

impl WriterIo for CrashIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        if matches!(self.crash, IoCrash::BoundarySync) {
            return Err(io::Error::other("injected recovery boundary sync crash"));
        }
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        if matches!(self.crash, IoCrash::DirectorySync) {
            return Err(io::Error::other("injected recovery directory sync crash"));
        }
        directory.sync()
    }

    fn truncate_file(&self, file: &File, len: u64) -> io::Result<()> {
        if matches!(self.crash, IoCrash::Truncate) {
            return Err(io::Error::other("injected recovery truncate crash"));
        }
        file.set_len(len)
    }

    fn before_remove_recovery_file(&self, _file_id: u32) -> io::Result<()> {
        let call = self.delete_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if matches!(self.crash, IoCrash::Delete(expected) if call == expected) {
            return Err(io::Error::other("injected recovery delete crash"));
        }
        Ok(())
    }

    fn after_recovery_trim(&self) -> io::Result<()> {
        let IoCrash::TailRecheck(path) = &self.crash else {
            return Ok(());
        };
        let file = OpenOptions::new().write(true).open(path)?;
        let len = file.metadata()?.len();
        file.set_len(len + 1)
    }
}

struct ProcessExitBackend {
    inner: Arc<FjallBackend>,
    exit_after_commit: usize,
    commits: AtomicUsize,
}

impl ProcessExitBackend {
    fn new(inner: Arc<FjallBackend>, exit_after_commit: usize) -> Self {
        Self {
            inner,
            exit_after_commit,
            commits: AtomicUsize::new(0),
        }
    }
}

impl IndexBackend for ProcessExitBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        self.inner.commit_atomic(batch, mode)?;
        let call = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.exit_after_commit {
            std::process::exit(PROCESS_CRASH_EXIT_CODE);
        }
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        self.inner.get_database_identity()
    }

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        self.inner.get_user(key, snapshot)
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_internal(space, key)
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        self.inner.scan_internal(space, range)
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        self.inner.snapshot()
    }

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        self.inner.iter_user(snapshot)
    }
}

struct ProcessExitIo {
    point: String,
}

impl ProcessExitIo {
    fn new(point: String) -> Self {
        Self { point }
    }

    fn exit_if(&self, expected: &str) {
        if self.point == expected {
            std::process::exit(PROCESS_CRASH_EXIT_CODE);
        }
    }
}

impl WriterIo for ProcessExitIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()?;
        self.exit_if("trim-directory");
        Ok(())
    }

    fn truncate_file(&self, file: &File, len: u64) -> io::Result<()> {
        file.set_len(len)?;
        self.exit_if("trim-truncate");
        Ok(())
    }

    fn after_remove_recovery_file(&self, _file_id: u32) -> io::Result<()> {
        self.exit_if("trim-delete");
        Ok(())
    }
}

struct Harness {
    _temporary: Option<TempDir>,
    root: RootLock,
    index_path: PathBuf,
    vlog_path: PathBuf,
    format: FormatMetadataV0,
    backend: Option<Arc<FjallBackend>>,
    coordinator: Option<CommitCoordinator<FjallBackend, FixedUuid>>,
}

impl Harness {
    fn new() -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().to_path_buf();
        Self::create_at(&path, Some(temporary))
    }

    fn create_at(path: &Path, temporary: Option<TempDir>) -> TestResult<Self> {
        let root = RootLock::acquire(path, false)?.expect("exclusive root lock");
        let index_path = path.join("index");
        let vlog_path = path.join("vlog");
        std::fs::create_dir(&index_path)?;
        std::fs::create_dir(&vlog_path)?;
        let format = FormatMetadataV0::new(DATABASE_UUID)?;
        std::fs::write(path.join("FORMAT"), format.encode()?)?;

        let backend = Arc::new(FjallBackend::create_for_open_preparation(
            &index_path,
            fjall_options(),
        )?);
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
            directory,
            DATABASE_UUID,
            vlog::format::VLogGeometry::PRODUCTION,
            catalog,
        )?;
        let stats = Arc::new(StatsState::new());
        let coordinator = CommitCoordinator::new(
            RuntimeControl::new(Arc::clone(&stats)),
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
        Ok(Self {
            _temporary: temporary,
            root,
            index_path,
            vlog_path,
            format,
            backend: Some(backend),
            coordinator: Some(coordinator),
        })
    }

    fn reopen_at(path: &Path) -> TestResult<Self> {
        let root = RootLock::acquire(path, false)?.expect("exclusive root lock");
        let index_path = path.join("index");
        let vlog_path = path.join("vlog");
        let format = FormatMetadataV0::new(DATABASE_UUID)?;
        let backend = Arc::new(FjallBackend::open_existing_for_open_preparation(
            &index_path,
            fjall_options(),
        )?);
        Ok(Self {
            _temporary: None,
            root,
            index_path,
            vlog_path,
            format,
            backend: Some(backend),
            coordinator: None,
        })
    }

    fn backend(&self) -> &Arc<FjallBackend> {
        self.backend.as_ref().expect("backend is open")
    }

    fn put(&self, key: &[u8], value: &[u8], sync: bool) -> TestResult {
        self.coordinator
            .as_ref()
            .expect("coordinator is active")
            .commit_nonempty(&preflight_put(key, value, sync)?)?;
        Ok(())
    }

    fn finish_writes(&mut self) {
        drop(self.coordinator.take());
    }

    fn reopen_backend(&mut self) -> TestResult {
        drop(self.backend.take());
        self.backend = Some(Arc::new(FjallBackend::open_existing_for_open_preparation(
            &self.index_path,
            fjall_options(),
        )?));
        Ok(())
    }

    fn recovery_inputs<B: IndexBackend>(
        &self,
        backend: &B,
        io: Arc<dyn WriterIo>,
    ) -> Result<(recovery::RecoveryPlan, ValueLogReader, ValueLogRecovery)> {
        let inventory = ManagedInventory::inspect(&self.root, &self.format)?;
        let directory = Arc::new(VLogDirectory::open(&self.vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        for entry in &inventory.vlog_files {
            let file = directory
                .open_read_only(entry.file_id)
                .map_err(test_io_error)?;
            catalog.register(entry.file_id, &file)?;
        }
        let files = Arc::new(FileSet::new(
            directory,
            DATABASE_UUID,
            vlog::format::VLogGeometry::PRODUCTION,
            catalog,
            2,
        )?);
        let reader =
            ValueLogReader::new(Arc::clone(&files), vlog::format::VLogGeometry::PRODUCTION)?;
        let plan = analyze_recovery(backend, &self.format, &inventory, &reader)?;
        let recovery = ValueLogRecovery::new_with_io(files, io)?;
        Ok((plan, reader, recovery))
    }

    fn execute_with<B: IndexBackend>(
        &self,
        backend: &B,
        io: Arc<dyn WriterIo>,
    ) -> Result<recovery::RecoveredState> {
        let (plan, reader, vlog) = self.recovery_inputs(backend, io)?;
        execute_recovery(backend, plan, &self.root, &self.format, &reader, vlog)
    }

    fn execute_normal(&self) -> Result<recovery::RecoveredState> {
        self.execute_with(self.backend().as_ref(), Arc::new(CrashIo::normal()))
    }

    fn analyze(&self) -> Result<recovery::RecoveryPlan> {
        let (plan, _reader, recovery) =
            self.recovery_inputs(self.backend().as_ref(), Arc::new(CrashIo::normal()))?;
        drop(recovery);
        Ok(plan)
    }

    fn commit_mutations(&self, mutations: Vec<IndexMutation>) -> TestResult {
        let mut batch = IndexAtomicBatch::try_with_capacity(mutations.len())
            .map_err(|error| io::Error::other(format!("batch allocation: {error:?}")))?;
        for mutation in mutations {
            batch
                .try_push(mutation)
                .map_err(|error| io::Error::other(format!("batch mutation: {error:?}")))?;
        }
        self.backend()
            .commit_atomic(batch, IndexCommitMode::SyncAll)
            .map_err(|error| io::Error::other(format!("mutation commit: {error:?}")))?;
        Ok(())
    }

    fn install_state(&self, state: RecoveryState) -> TestResult {
        self.commit_mutations(vec![IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: RECOVERY_STATE_KEY.to_vec(),
            value: state.encode()?.to_vec(),
        }])
    }

    fn flip_value_byte(&self, descriptor: &TransactionDescriptor) -> TestResult {
        let pointer = descriptor
            .mutations
            .iter()
            .find_map(|mutation| match mutation.after_state {
                ValueState::Present(pointer) => Some(pointer),
                ValueState::Absent => None,
            })
            .expect("Put descriptor");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.vlog_path.join(format!("D{:06}.data", pointer.file_id)))?;
        let offset = u64::from(pointer.record_offset) + 39;
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset)?;
        byte[0] ^= 0x80;
        file.write_all_at(&byte, offset)?;
        Ok(())
    }

    fn state(&self) -> TestResult<Option<RecoveryState>> {
        self.backend()
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .as_deref()
            .map(RecoveryState::decode)
            .transpose()
            .map_err(Into::into)
    }

    fn head_and_frontier(&self) -> TestResult<(u64, DurableFrontier)> {
        let head = self
            .backend()
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .ok_or_else(|| io::Error::other("head missing"))?;
        let frontier = self
            .backend()
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .ok_or_else(|| io::Error::other("frontier missing"))?;
        Ok((decode_head_seq(&head)?, DurableFrontier::decode(&frontier)?))
    }

    fn read_value(&self, key: &[u8]) -> TestResult<Option<Vec<u8>>> {
        let Some(encoded_pointer) = self.backend().get_user(key, None)? else {
            return Ok(None);
        };
        let inventory = ManagedInventory::inspect(&self.root, &self.format)?;
        let directory = Arc::new(VLogDirectory::open(&self.vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        for entry in &inventory.vlog_files {
            let file = directory.open_read_only(entry.file_id)?;
            catalog.register(entry.file_id, &file)?;
        }
        let files = Arc::new(FileSet::new(
            directory,
            DATABASE_UUID,
            vlog::format::VLogGeometry::PRODUCTION,
            catalog,
            2,
        )?);
        let reader = ValueLogReader::new(files, vlog::format::VLogGeometry::PRODUCTION)?;
        Ok(Some(reader.read_value(&encoded_pointer, key)?))
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

fn test_io_error(source: io::Error) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.os_code = source.raw_os_error();
    error
}

fn assert_recovery_failure(error: &StorageError) {
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert!(error.write_outcome.is_none());
    assert!(error.instance_state.is_none());
}

fn prepare_rejected_suffix() -> TestResult<(Harness, recovery::RecoveryPlan)> {
    let mut harness = Harness::new()?;
    harness.put(b"shared", b"stable", true)?;
    harness.put(b"shared", b"accepted", false)?;
    harness.put(b"shared", b"rejected-three", false)?;
    harness.put(b"shared", b"rejected-four", false)?;
    harness.finish_writes();
    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[1])?;
    let plan = harness.analyze()?;
    assert_eq!(
        (
            plan.durable_frontier.durable_seq,
            plan.accepted_seq,
            plan.head_seq
        ),
        (1, 2, 4)
    );
    assert!(plan.needs_undo && plan.needs_promote && plan.needs_trim);
    Ok((harness, plan))
}

#[test]
fn recovery_process_crash_child() -> TestResult {
    if std::env::var_os(PROCESS_CRASH_ENV).is_none() {
        return Ok(());
    }
    let path = PathBuf::from(
        std::env::var_os(PROCESS_CRASH_PATH_ENV).expect("recovery crash path must be provided"),
    );
    let point =
        std::env::var(PROCESS_CRASH_POINT_ENV).expect("recovery crash point must be provided");
    let mut harness = Harness::create_at(&path, None)?;
    harness.put(b"shared", b"stable", true)?;
    harness.put(b"shared", b"accepted", false)?;
    harness.put(b"shared", b"rejected-three", false)?;
    harness.put(b"shared", b"rejected-four", false)?;
    harness.finish_writes();
    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[1])?;
    let corrupted_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(harness.vlog_path.join("D000000.data"))?;
    corrupted_file.sync_data()?;
    File::create(harness.vlog_path.join("D000001.data"))?;
    File::create(harness.vlog_path.join("D000002.data"))?;
    VLogDirectory::open(&harness.vlog_path)?.sync()?;

    if let Some(call) = point.strip_prefix("commit-") {
        let call = call.parse::<usize>()?;
        let backend = ProcessExitBackend::new(Arc::clone(harness.backend()), call);
        let _ = harness.execute_with(&backend, Arc::new(ProcessExitIo::new(String::new())))?;
    } else {
        let _ = harness.execute_with(
            harness.backend().as_ref(),
            Arc::new(ProcessExitIo::new(point)),
        )?;
    }
    panic!("recovery child did not exit at its configured crash point");
}

#[test]
fn real_process_exits_during_recovery_and_every_reopen_converges() -> TestResult {
    for point in [
        "commit-1",
        "commit-2",
        "commit-4",
        "commit-5",
        "trim-truncate",
        "trim-delete",
        "trim-directory",
    ] {
        let temporary = TempDir::new()?;
        let status = Command::new(std::env::current_exe()?)
            .args(["--exact", "recovery_process_crash_child", "--nocapture"])
            .env(PROCESS_CRASH_ENV, "1")
            .env(PROCESS_CRASH_PATH_ENV, temporary.path())
            .env(PROCESS_CRASH_POINT_ENV, point)
            .status()?;
        assert_eq!(
            status.code(),
            Some(PROCESS_CRASH_EXIT_CODE),
            "crash point {point}"
        );

        let harness = Harness::reopen_at(temporary.path())?;
        let recovered = harness.execute_normal()?;
        assert_eq!(recovered.head_seq, 2, "crash point {point}");
        assert_eq!(
            recovered.durable_frontier.durable_seq, 2,
            "crash point {point}"
        );
        drop(recovered.writer);
        assert_eq!(
            harness.read_value(b"shared")?,
            Some(b"accepted".to_vec()),
            "crash point {point}"
        );
        assert!(harness.state()?.is_none(), "crash point {point}");
        let inventory = ManagedInventory::inspect(&harness.root, &harness.format)?;
        assert_eq!(inventory.vlog_files.len(), 1, "crash point {point}");
    }
    Ok(())
}

#[test]
fn recovery_state_commit_before_and_unknown_crashes_converge_on_reopen() -> TestResult {
    for crash in [CommitCrash::Before(1), CommitCrash::After(1)] {
        let (mut harness, plan) = prepare_rejected_suffix()?;
        let crashing = CrashBackend::new(Arc::clone(harness.backend()), crash);
        let error = match harness.execute_with(&crashing, Arc::new(CrashIo::normal())) {
            Ok(_) => panic!("state commit crash must stop Open"),
            Err(error) => error,
        };
        assert_recovery_failure(&error);
        match crash {
            CommitCrash::Before(_) => assert!(harness.state()?.is_none()),
            CommitCrash::After(_) => {
                let state = harness.state()?.expect("atomic state commit applied");
                assert_eq!(state.phase, RecoveryPhase::Undo);
                assert_eq!(state.next_undo_seq, 4);
            }
        }
        assert_eq!(harness.head_and_frontier()?.0, 4);
        drop(crashing);
        harness.reopen_backend()?;
        let recovered = harness.execute_normal()?;
        assert_eq!(recovered.head_seq, 2);
        assert_eq!(recovered.durable_frontier.durable_seq, 2);
        assert_eq!(
            recovered.durable_frontier.durable_vlog_end,
            plan.accepted_end
        );
        assert!(harness.state()?.is_none());
    }
    Ok(())
}

#[test]
fn every_undo_batch_is_atomic_and_reopen_resumes_at_the_persisted_sequence() -> TestResult {
    for (crash, expected_head) in [
        (CommitCrash::Before(1), 4),
        (CommitCrash::After(1), 3),
        (CommitCrash::Before(2), 3),
        (CommitCrash::After(2), 2),
    ] {
        let (mut harness, plan) = prepare_rejected_suffix()?;
        harness.install_state(RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 4,
            target_seq: 2,
            target_vlog_end: plan.accepted_end,
            next_undo_seq: 4,
            trim_required: true,
        })?;
        let expected_pointer = match expected_head {
            4 => plan.descriptors[2].mutations[0].after_state,
            3 => plan.descriptors[1].mutations[0].after_state,
            2 => plan.descriptors[0].mutations[0].after_state,
            _ => unreachable!(),
        };

        let crashing = CrashBackend::new(Arc::clone(harness.backend()), crash);
        let error = match harness.execute_with(&crashing, Arc::new(CrashIo::normal())) {
            Ok(_) => panic!("Undo crash must stop Open"),
            Err(error) => error,
        };
        assert_recovery_failure(&error);
        let state = harness.state()?.expect("Undo state remains");
        assert_eq!(state.phase, RecoveryPhase::Undo);
        assert_eq!(state.next_undo_seq, expected_head);
        assert_eq!(harness.head_and_frontier()?.0, expected_head);
        let ValueState::Present(pointer) = expected_pointer else {
            panic!("test transactions are puts");
        };
        assert_eq!(
            harness.backend().get_user(b"shared", None)?.as_deref(),
            Some(pointer.encode()?.as_slice())
        );
        for seq in (expected_head + 1)..=4 {
            assert!(
                harness
                    .backend()
                    .get_internal(InternalIndexSpace::Transaction, &encode_tx_meta_key(seq)?,)?
                    .is_none()
            );
        }

        drop(crashing);
        harness.reopen_backend()?;
        let recovered = harness.execute_normal()?;
        assert_eq!(recovered.head_seq, 2);
        assert_eq!(recovered.durable_frontier.durable_seq, 2);
        assert!(harness.state()?.is_none());
    }
    Ok(())
}

#[test]
fn frontier_and_finalize_commit_boundaries_resume_without_guessing() -> TestResult {
    for crash in [CommitCrash::Before(1), CommitCrash::After(1)] {
        let mut harness = Harness::new()?;
        harness.put(b"stable", b"one", true)?;
        harness.put(b"accepted", b"two", false)?;
        harness.finish_writes();
        let plan = harness.analyze()?;
        harness.install_state(RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 2,
            target_seq: 2,
            target_vlog_end: plan.accepted_end,
            next_undo_seq: 2,
            trim_required: false,
        })?;

        let crashing = CrashBackend::new(Arc::clone(harness.backend()), crash);
        let error = match harness.execute_with(&crashing, Arc::new(CrashIo::normal())) {
            Ok(_) => panic!("frontier crash must stop Open"),
            Err(error) => error,
        };
        assert_recovery_failure(&error);
        let (head, frontier) = harness.head_and_frontier()?;
        match crash {
            CommitCrash::Before(_) => {
                assert_eq!((head, frontier.durable_seq), (2, 1));
                assert_eq!(
                    harness.state()?.expect("Undo state").phase,
                    RecoveryPhase::Undo
                );
            }
            CommitCrash::After(_) => {
                assert_eq!((head, frontier.durable_seq), (2, 2));
                assert_eq!(
                    harness.state()?.expect("Finalize state").phase,
                    RecoveryPhase::Finalize
                );
            }
        }
        drop(crashing);
        harness.reopen_backend()?;
        assert_eq!(harness.execute_normal()?.durable_frontier.durable_seq, 2);
        assert!(harness.state()?.is_none());
    }

    for crash in [CommitCrash::Before(1), CommitCrash::After(1)] {
        let mut harness = Harness::new()?;
        harness.put(b"stable", b"one", true)?;
        harness.finish_writes();
        let plan = harness.analyze()?;
        harness.install_state(RecoveryState {
            phase: RecoveryPhase::Finalize,
            original_head: 1,
            target_seq: 1,
            target_vlog_end: plan.accepted_end,
            next_undo_seq: 1,
            trim_required: false,
        })?;
        let crashing = CrashBackend::new(Arc::clone(harness.backend()), crash);
        let error = match harness.execute_with(&crashing, Arc::new(CrashIo::normal())) {
            Ok(_) => panic!("Finalize crash must stop Open"),
            Err(error) => error,
        };
        assert_recovery_failure(&error);
        match crash {
            CommitCrash::Before(_) => assert!(harness.state()?.is_some()),
            CommitCrash::After(_) => assert!(harness.state()?.is_none()),
        }
        drop(crashing);
        harness.reopen_backend()?;
        assert_eq!(harness.execute_normal()?.head_seq, 1);
        assert!(harness.state()?.is_none());
    }
    Ok(())
}

#[test]
fn trim_failures_leave_trim_state_and_repeated_open_converges_idempotently() -> TestResult {
    for fault in [
        IoCrash::Truncate,
        IoCrash::BoundarySync,
        IoCrash::Delete(2),
        IoCrash::DirectorySync,
    ] {
        run_trim_recrash_case(fault)?;
    }

    let mut harness = prepared_trim_state()?;
    let plan = harness.analyze()?;
    let DurableVLogEnd::Position(target) = plan.accepted_end else {
        panic!("trim target is nonempty");
    };
    let boundary = harness
        .vlog_path
        .join(format!("D{:06}.data", target.file_id));
    let error = match harness.execute_with(
        harness.backend().as_ref(),
        Arc::new(CrashIo::with(IoCrash::TailRecheck(boundary))),
    ) {
        Ok(_) => panic!("tail recheck mismatch must stop Open"),
        Err(error) => error,
    };
    assert_recovery_failure(&error);
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(
        harness.state()?.expect("Trim state remains").phase,
        RecoveryPhase::Trim
    );
    harness.reopen_backend()?;
    let recovered = harness.execute_normal()?;
    assert_eq!(
        recovered.durable_frontier.durable_vlog_end,
        plan.accepted_end
    );
    assert!(harness.state()?.is_none());
    Ok(())
}

fn prepared_trim_state() -> TestResult<Harness> {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"value", true)?;
    harness.finish_writes();
    let plan = harness.analyze()?;
    let DurableVLogEnd::Position(target) = plan.accepted_end else {
        panic!("stable transaction has an end");
    };
    OpenOptions::new()
        .write(true)
        .open(
            harness
                .vlog_path
                .join(format!("D{:06}.data", target.file_id)),
        )?
        .set_len(target.offset + 17)?;
    File::create(
        harness
            .vlog_path
            .join(format!("D{:06}.data", target.file_id + 1)),
    )?;
    File::create(
        harness
            .vlog_path
            .join(format!("D{:06}.data", target.file_id + 2)),
    )?;
    harness.install_state(RecoveryState {
        phase: RecoveryPhase::Trim,
        original_head: 1,
        target_seq: 1,
        target_vlog_end: plan.accepted_end,
        next_undo_seq: 1,
        trim_required: true,
    })?;
    Ok(harness)
}

fn run_trim_recrash_case(fault: IoCrash) -> TestResult {
    let mut harness = prepared_trim_state()?;
    let target = harness.analyze()?.accepted_end;
    let error =
        match harness.execute_with(harness.backend().as_ref(), Arc::new(CrashIo::with(fault))) {
            Ok(_) => panic!("injected Trim failure must stop Open"),
            Err(error) => error,
        };
    assert_recovery_failure(&error);
    assert_eq!(
        harness.state()?.expect("Trim state remains").phase,
        RecoveryPhase::Trim
    );
    harness.reopen_backend()?;
    let recovered = harness.execute_normal()?;
    assert_eq!(recovered.durable_frontier.durable_vlog_end, target);
    assert!(harness.state()?.is_none());
    let inventory = ManagedInventory::inspect(&harness.root, &harness.format)?;
    let DurableVLogEnd::Position(target) = target else {
        panic!("nonempty target");
    };
    assert_eq!(
        inventory.vlog_files.len(),
        usize::try_from(target.file_id)? + 1
    );
    assert_eq!(
        inventory.vlog_files.last().expect("boundary").len,
        target.offset
    );
    Ok(())
}
