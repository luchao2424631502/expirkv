#![allow(dead_code, unused_imports)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, HEAD_SEQ_KEY, IndexAtomicBatch,
    IndexBackend, IndexCommitMode, IndexCompression, IndexMutation, InternalIndexSpace,
    initialization_batch,
};
use lock::RootLock;
use recovery::{
    analyze_recovery, analyze_recovery_with_test_geometry, execute_recovery,
    execute_recovery_with_test_geometry, fail_next_inventory_inspect_for_test,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{
    DecodedRecord, LayoutPlanner, LogicalOperationRef, PageHeader, VLogFileHeader, VLogGeometry,
    VLogPosition, ValuePointer, decode_record_at, prepare_envelope,
};
use vlog::reader::ValueLogReader;
use vlog::writer::{AppendStateSnapshot, ValueLogRecovery, ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x63; 16];
const BACKPRESSURE_CHILD_MODE: &str = "RUSTKV_RECOVERY_BACKPRESSURE_CHILD_MODE";
const BACKPRESSURE_CHILD_MARKER: &str = "RUSTKV_RECOVERY_BACKPRESSURE_CHILD_MARKER";
const ZERO_WORKER_MODE: &str = "zero-worker";
const ENABLED_WORKER_MODE: &str = "enabled-worker";
const PUBLIC_OPEN_BACKPRESSURE_CHILD: &str = "RUSTKV_PUBLIC_OPEN_BACKPRESSURE_CHILD";
const PUBLIC_OPEN_BACKPRESSURE_PATH: &str = "RUSTKV_PUBLIC_OPEN_BACKPRESSURE_PATH";

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

#[derive(Default)]
struct RecoveryIo {
    file_syncs: AtomicUsize,
    directory_syncs: AtomicUsize,
    truncates: AtomicUsize,
    deletes: AtomicUsize,
}

impl WriterIo for RecoveryIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        self.file_syncs.fetch_add(1, Ordering::SeqCst);
        file.sync_data()
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        self.directory_syncs.fetch_add(1, Ordering::SeqCst);
        directory.sync()
    }

    fn truncate_file(&self, file: &File, len: u64) -> io::Result<()> {
        self.truncates.fetch_add(1, Ordering::SeqCst);
        file.set_len(len)
    }

    fn before_remove_recovery_file(&self, _file_id: u32) -> io::Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Harness {
    _temporary: TempDir,
    root: RootLock,
    index_path: PathBuf,
    vlog_path: PathBuf,
    format: FormatMetadataV0,
    geometry: VLogGeometry,
    backend: Option<Arc<FjallBackend>>,
    coordinator: Option<CommitCoordinator<FjallBackend, FixedUuid>>,
}

impl Harness {
    fn new() -> TestResult<Self> {
        Self::with_geometry(VLogGeometry::PRODUCTION)
    }

    fn with_geometry(geometry: VLogGeometry) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let root = RootLock::acquire(temporary.path(), false)?.expect("exclusive root lock");
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&index_path)?;
        std::fs::create_dir(&vlog_path)?;
        let format = FormatMetadataV0::new(DATABASE_UUID)?;
        std::fs::write(temporary.path().join("FORMAT"), format.encode()?)?;

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
        let writer = ValueLogWriter::empty(directory, DATABASE_UUID, geometry, catalog)?;
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
            geometry,
            backend: Some(backend),
            coordinator: Some(coordinator),
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

    fn reopen_backend_with_workers(&mut self) -> TestResult {
        drop(self.backend.take());
        self.backend = Some(Arc::new(FjallBackend::open_existing(
            &self.index_path,
            fjall_options(),
        )?));
        Ok(())
    }

    fn recovery_inputs(
        &self,
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
            self.geometry,
            catalog,
            2,
        )?);
        let reader = ValueLogReader::new(Arc::clone(&files), self.geometry)?;
        let plan = if self.geometry == VLogGeometry::PRODUCTION {
            analyze_recovery(self.backend().as_ref(), &self.format, &inventory, &reader)?
        } else {
            analyze_recovery_with_test_geometry(
                self.backend().as_ref(),
                &self.format,
                &inventory,
                &reader,
            )?
        };
        let recovery = ValueLogRecovery::new_with_io(files, io)?;
        Ok((plan, reader, recovery))
    }

    fn execute(&self, io: Arc<dyn WriterIo>) -> Result<recovery::RecoveredState> {
        let (plan, reader, vlog) = self.recovery_inputs(io)?;
        self.execute_inputs(plan, &reader, vlog)
    }

    fn execute_inputs(
        &self,
        plan: recovery::RecoveryPlan,
        reader: &ValueLogReader,
        vlog: ValueLogRecovery,
    ) -> Result<recovery::RecoveredState> {
        if self.geometry == VLogGeometry::PRODUCTION {
            execute_recovery(
                self.backend().as_ref(),
                plan,
                &self.root,
                &self.format,
                reader,
                vlog,
            )
        } else {
            execute_recovery_with_test_geometry(
                self.backend().as_ref(),
                plan,
                &self.root,
                &self.format,
                reader,
                vlog,
            )
        }
    }

    fn analyze(&self) -> Result<recovery::RecoveryPlan> {
        let io = Arc::new(RecoveryIo::default());
        let (plan, _reader, recovery) = self.recovery_inputs(io)?;
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

    fn flip_value_byte(&self, descriptor: &TransactionDescriptor) -> TestResult {
        let pointer = descriptor
            .mutations
            .iter()
            .find_map(|mutation| match mutation.after_state {
                ValueState::Present(pointer) => Some(pointer),
                ValueState::Absent => None,
            })
            .expect("Put descriptor");
        let path = self.vlog_path.join(format!("D{:06}.data", pointer.file_id));
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let offset = u64::from(pointer.record_offset) + 39;
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset)?;
        byte[0] ^= 0x80;
        file.write_all_at(&byte, offset)?;
        Ok(())
    }

    fn read_value(&self, key: &[u8]) -> TestResult<Option<Vec<u8>>> {
        let encoded = self.backend().get_user(key, None)?;
        let Some(encoded) = encoded else {
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
            self.geometry,
            catalog,
            2,
        )?);
        let reader = ValueLogReader::new(files, self.geometry)?;
        Ok(Some(reader.read_value(&encoded, key)?))
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

fn expected_position(end: DurableVLogEnd) -> VLogPosition {
    match end {
        DurableVLogEnd::Empty => VLogPosition {
            file_id: 0,
            offset: 0,
        },
        DurableVLogEnd::Position(position) => VLogPosition {
            file_id: position.file_id,
            offset: position.offset,
        },
    }
}

fn encoded_mutation_key(commit_seq: u64, ordinal: u64) -> Vec<u8> {
    let mut key = vec![0_u8; 19];
    key[0..2].copy_from_slice(b"TX");
    key[2..10].copy_from_slice(&commit_seq.to_be_bytes());
    key[10] = 1;
    key[11..19].copy_from_slice(&ordinal.to_be_bytes());
    key
}

fn prepare_and_execute_backpressured_recovery(enable_workers: bool) -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"shared", b"stable", true)?;
    harness.put(b"shared", b"rejected", false)?;
    harness.finish_writes();

    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[0])?;
    let io = Arc::new(RecoveryIo::default());
    let (plan, reader, vlog) = harness.recovery_inputs(io)?;
    assert_eq!((plan.durable_frontier.durable_seq, plan.head_seq), (1, 2));
    assert_eq!(plan.accepted_seq, 1);
    assert!(plan.needs_undo && plan.needs_trim);

    for ordinal in 0_u8..4 {
        let key = [b'p', ordinal];
        harness
            .backend()
            .insert_without_keyspace_durability(None, &key, b"pressure")?;
        assert!(harness.backend().rotate_user_memtable_without_wait()?);
    }
    assert_eq!(harness.backend().user_sealed_memtable_count(), 4);
    assert!(harness.backend().outstanding_flushes() >= 4);

    if enable_workers {
        harness.reopen_backend_with_workers()?;
    }
    assert_eq!(
        harness.backend().background_workers_enabled(),
        enable_workers
    );
    let marker =
        std::env::var_os(BACKPRESSURE_CHILD_MARKER).ok_or("missing backpressure child marker")?;
    std::fs::write(marker, b"execute")?;

    let recovered = harness.execute_inputs(plan, &reader, vlog)?;
    assert_eq!(recovered.head_seq, 1);
    assert_eq!(recovered.durable_frontier.durable_seq, 1);
    drop(recovered.writer);
    assert_eq!(harness.read_value(b"shared")?, Some(b"stable".to_vec()));
    Ok(())
}

fn spawn_backpressure_child(mode: &str, marker: &Path) -> io::Result<Child> {
    Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "recovery_commit_backpressure_requires_worker_handoff",
            "--nocapture",
        ])
        .env(BACKPRESSURE_CHILD_MODE, mode)
        .env(BACKPRESSURE_CHILD_MARKER, marker)
        .spawn()
}

fn wait_for_child_marker(child: &mut Child, marker: &Path, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if marker.exists() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "backpressure child exited as {status} before execute"
            )));
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "backpressure child did not reach execute",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn recovery_commit_backpressure_requires_worker_handoff() -> TestResult {
    if let Some(mode) = std::env::var_os(BACKPRESSURE_CHILD_MODE) {
        return match mode.to_str().ok_or("invalid backpressure child mode")? {
            ZERO_WORKER_MODE => prepare_and_execute_backpressured_recovery(false),
            ENABLED_WORKER_MODE => prepare_and_execute_backpressured_recovery(true),
            _ => Err("unknown backpressure child mode".into()),
        };
    }

    let markers = tempfile::tempdir()?;
    let zero_marker = markers.path().join("zero-worker");
    let mut zero_worker = spawn_backpressure_child(ZERO_WORKER_MODE, &zero_marker)?;
    wait_for_child_marker(&mut zero_worker, &zero_marker, Duration::from_secs(15))?;
    assert!(
        wait_for_child_exit(&mut zero_worker, Duration::from_secs(2))?.is_none(),
        "the zero-worker control unexpectedly escaped Fjall local_backpressure"
    );

    let enabled_marker = markers.path().join("enabled-worker");
    let mut enabled_worker = spawn_backpressure_child(ENABLED_WORKER_MODE, &enabled_marker)?;
    wait_for_child_marker(
        &mut enabled_worker,
        &enabled_marker,
        Duration::from_secs(15),
    )?;
    let status = wait_for_child_exit(&mut enabled_worker, Duration::from_secs(15))?
        .ok_or("worker-enabled recovery did not complete before timeout")?;
    assert!(status.success(), "worker-enabled child failed as {status}");
    Ok(())
}

#[test]
fn public_db_open_recovers_with_four_sealed_system_memtables() -> TestResult {
    if std::env::var_os(PUBLIC_OPEN_BACKPRESSURE_CHILD).is_some() {
        let root = std::env::var_os(PUBLIC_OPEN_BACKPRESSURE_PATH)
            .ok_or("missing public Open backpressure path")?;
        let db = rustkv::Db::open(&rustkv::Options::default(), root)?;
        assert_eq!(
            db.get(&rustkv::ReadOptions::default(), b"stable")?,
            Some(b"prefix".to_vec())
        );
        assert_eq!(
            db.get(&rustkv::ReadOptions::default(), b"tail")?,
            Some(b"accepted".to_vec())
        );
        let stats = db.stats();
        assert_eq!((stats.head_seq, stats.durable_seq), (2, 2));
        assert_eq!(stats.durability_lag, 0);
        return Ok(());
    }

    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("db");
    {
        let options = rustkv::Options {
            create_if_missing: true,
            write_buffer_size: 64 * 1024,
            ..rustkv::Options::default()
        };
        let db = rustkv::Db::open(&options, &root)?;
        db.put(&rustkv::WriteOptions { sync: true }, b"stable", b"prefix")?;
        db.put(&rustkv::WriteOptions::default(), b"tail", b"accepted")?;
    }

    let backend =
        FjallBackend::open_existing_for_open_preparation(&root.join("index"), fjall_options())?;
    let encoded_head = backend
        .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
        .ok_or("missing HeadSeq")?;
    while backend.internal_sealed_memtable_count(InternalIndexSpace::System) < 4 {
        backend.insert_without_keyspace_durability(
            Some(InternalIndexSpace::System),
            HEAD_SEQ_KEY,
            &encoded_head,
        )?;
        assert!(backend.rotate_internal_memtable_without_wait(InternalIndexSpace::System)?);
    }
    assert_eq!(
        backend.internal_sealed_memtable_count(InternalIndexSpace::System),
        4
    );
    assert!(backend.outstanding_flushes() >= 4);
    drop(backend);

    let mut child = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "public_db_open_recovers_with_four_sealed_system_memtables",
            "--nocapture",
        ])
        .env(PUBLIC_OPEN_BACKPRESSURE_CHILD, "1")
        .env(PUBLIC_OPEN_BACKPRESSURE_PATH, &root)
        .spawn()?;
    let status = wait_for_child_exit(&mut child, Duration::from_secs(20))?
        .ok_or("public Db::open remained blocked by Fjall backpressure")?;
    assert!(status.success(), "public Open child failed as {status}");
    Ok(())
}

#[test]
fn reverse_undo_promotes_trims_and_returns_the_unique_writer() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"shared", b"stable", true)?;
    harness.put(b"shared", b"accepted", false)?;
    harness.put(b"shared", b"rejected-three", false)?;
    harness.put(b"shared", b"rejected-four", false)?;
    harness.finish_writes();

    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[1])?;
    let plan = harness.analyze()?;
    assert_eq!(plan.accepted_seq, 2);
    assert_eq!(plan.head_seq, 4);
    assert!(plan.needs_undo && plan.needs_promote && plan.needs_trim);
    let accepted_end = plan.accepted_end;

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(recovered.head_seq, 2);
    assert_eq!(recovered.durable_frontier.durable_seq, 2);
    assert_eq!(recovered.durable_frontier.durable_vlog_end, accepted_end);
    assert_eq!(recovered.writer.position(), expected_position(accepted_end));
    assert!(recovered.writer.dirty_state().dirty_files.is_empty());
    assert!(
        recovered
            .writer
            .dirty_state()
            .pending_directory_entries
            .is_empty()
    );
    assert_eq!(harness.read_value(b"shared")?, Some(b"accepted".to_vec()));

    for seq in [3, 4] {
        assert!(
            harness
                .backend()
                .get_internal(InternalIndexSpace::Transaction, &encode_tx_meta_key(seq)?,)?
                .is_none()
        );
        assert!(
            harness
                .backend()
                .get_internal(
                    InternalIndexSpace::Transaction,
                    &encoded_mutation_key(seq, 0),
                )?
                .is_none()
        );
    }
    assert!(
        harness
            .backend()
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .is_none()
    );
    let inventory = ManagedInventory::inspect(&harness.root, &harness.format)?;
    let last = inventory.vlog_files.last().expect("accepted file remains");
    let DurableVLogEnd::Position(end) = accepted_end else {
        panic!("accepted prefix is nonempty");
    };
    assert_eq!((last.file_id, last.len), (end.file_id, end.offset));
    assert!(io.file_syncs.load(Ordering::SeqCst) >= 2);
    assert_eq!(io.truncates.load(Ordering::SeqCst), 1);
    assert_eq!(io.directory_syncs.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn after_state_mismatch_fails_closed_without_overwriting_user_state() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"key", b"stable", true)?;
    harness.put(b"key", b"rejected", false)?;
    harness.finish_writes();
    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[0])?;
    harness.commit_mutations(vec![IndexMutation::DeleteUser {
        user_key: b"key".to_vec(),
    }])?;

    let error = match harness.execute(Arc::new(RecoveryIo::default())) {
        Ok(_) => panic!("AfterState mismatch must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert!(error.write_outcome.is_none());
    assert!(harness.backend().get_user(b"key", None)?.is_none());
    let (head, frontier) = harness.head_and_frontier()?;
    assert_eq!((head, frontier.durable_seq), (2, 1));
    let state = RecoveryState::decode(
        &harness
            .backend()
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .expect("Undo state must remain"),
    )?;
    assert_eq!(state.phase, RecoveryPhase::Undo);
    assert_eq!(state.next_undo_seq, 2);
    assert!(
        harness
            .backend()
            .get_internal(InternalIndexSpace::Transaction, &encode_tx_meta_key(2)?,)?
            .is_some()
    );
    Ok(())
}

#[test]
fn promotion_only_syncs_the_prefix_without_creating_recovery_state() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"one", true)?;
    harness.put(b"two", b"two", false)?;
    harness.put(b"three", b"three", false)?;
    harness.finish_writes();
    let plan = harness.analyze()?;
    assert!(!plan.needs_undo && plan.needs_promote && !plan.needs_trim);
    let target = plan.accepted_end;

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(recovered.head_seq, 3);
    assert_eq!(recovered.durable_frontier.durable_seq, 3);
    assert_eq!(recovered.writer.position(), expected_position(target));
    assert!(
        harness
            .backend()
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .is_none()
    );
    assert_eq!(harness.read_value(b"two")?, Some(b"two".to_vec()));
    assert_eq!(harness.read_value(b"three")?, Some(b"three".to_vec()));
    assert!(io.file_syncs.load(Ordering::SeqCst) >= 1);
    assert_eq!(io.truncates.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn trim_truncates_boundary_deletes_higher_suffix_and_finalize_only_clears_state() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"value", true)?;
    harness.finish_writes();
    let stable = harness.analyze()?;
    let DurableVLogEnd::Position(end) = stable.accepted_end else {
        panic!("stable end");
    };
    OpenOptions::new()
        .write(true)
        .open(harness.vlog_path.join(format!("D{:06}.data", end.file_id)))?
        .set_len(end.offset + 19)?;
    File::create(
        harness
            .vlog_path
            .join(format!("D{:06}.data", end.file_id + 1)),
    )?;

    let plan = harness.analyze()?;
    assert!(!plan.needs_undo && !plan.needs_promote && plan.needs_trim);
    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(
        recovered.writer.position(),
        expected_position(stable.accepted_end)
    );
    assert_eq!(io.truncates.load(Ordering::SeqCst), 1);
    assert_eq!(io.deletes.load(Ordering::SeqCst), 1);
    assert_eq!(io.directory_syncs.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::metadata(harness.vlog_path.join(format!("D{:06}.data", end.file_id)),)?.len(),
        end.offset
    );
    assert!(
        !harness
            .vlog_path
            .join(format!("D{:06}.data", end.file_id + 1))
            .exists()
    );
    drop(recovered.writer);

    let state = RecoveryState {
        phase: RecoveryPhase::Finalize,
        original_head: 1,
        target_seq: 1,
        target_vlog_end: stable.accepted_end,
        next_undo_seq: 1,
        trim_required: false,
    };
    harness.commit_mutations(vec![IndexMutation::PutInternal {
        space: InternalIndexSpace::System,
        key: RECOVERY_STATE_KEY.to_vec(),
        value: state.encode()?.to_vec(),
    }])?;
    let finalize_io = Arc::new(RecoveryIo::default());
    let finalized = harness.execute(Arc::clone(&finalize_io) as Arc<dyn WriterIo>)?;
    assert_eq!(
        finalized.writer.state_snapshot(),
        AppendStateSnapshot::Open {
            file_id: end.file_id,
            offset: end.offset,
        }
    );
    assert_eq!(finalize_io.file_syncs.load(Ordering::SeqCst), 0);
    assert_eq!(finalize_io.directory_syncs.load(Ordering::SeqCst), 0);
    assert_eq!(finalize_io.truncates.load(Ordering::SeqCst), 0);
    assert_eq!(finalize_io.deletes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn empty_target_deletes_every_vlog_file_and_restores_lazy_empty_append_state() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"orphan", b"value", false)?;
    harness.finish_writes();
    let baseline = harness.analyze()?;
    harness.flip_value_byte(&baseline.descriptors[0])?;
    let plan = harness.analyze()?;
    assert_eq!(plan.accepted_seq, 0);
    assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
    assert!(plan.needs_undo && plan.needs_trim);

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(recovered.head_seq, 0);
    assert_eq!(recovered.durable_frontier.durable_seq, 0);
    assert_eq!(
        recovered.writer.state_snapshot(),
        AppendStateSnapshot::Empty
    );
    assert_eq!(recovered.writer.file_count(), 0);
    assert!(
        ManagedInventory::inspect(&harness.root, &harness.format)?
            .vlog_files
            .is_empty()
    );
    assert!(harness.backend().get_user(b"orphan", None)?.is_none());
    assert_eq!(io.deletes.load(Ordering::SeqCst), 1);
    assert_eq!(io.directory_syncs.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn promotion_syncs_every_file_from_the_old_frontier_through_a_multifile_target() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 5)?;
    let mut harness = Harness::with_geometry(geometry)?;
    let value = vec![0x5a; 40_000];
    harness.put(b"stable", &value, true)?;
    for seq in 2..=8_u8 {
        harness.put(&[b'k', seq], &value, false)?;
    }
    harness.finish_writes();
    let plan = harness.analyze()?;
    let DurableVLogEnd::Position(old_end) = plan.durable_frontier.durable_vlog_end else {
        panic!("stable frontier is nonempty");
    };
    let DurableVLogEnd::Position(target_end) = plan.accepted_end else {
        panic!("accepted target is nonempty");
    };
    assert!(target_end.file_id >= old_end.file_id + 2);
    assert!(!plan.needs_undo && plan.needs_promote && !plan.needs_trim);

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(recovered.head_seq, 8);
    assert_eq!(recovered.durable_frontier.durable_seq, 8);
    assert_eq!(
        io.file_syncs.load(Ordering::SeqCst),
        usize::try_from(target_end.file_id - old_end.file_id + 1)?
    );
    assert_eq!(io.directory_syncs.load(Ordering::SeqCst), 1);
    assert_eq!(harness.read_value(&[b'k', 8])?, Some(value));
    Ok(())
}

#[test]
fn recovery_rejects_reader_and_writer_capabilities_from_different_databases() -> TestResult {
    let mut first = Harness::new()?;
    first.put(b"first", b"value-a", true)?;
    first.finish_writes();
    let (plan, reader, first_recovery) = first.recovery_inputs(Arc::new(RecoveryIo::default()))?;
    drop(first_recovery);

    let mut second = Harness::new()?;
    second.put(b"second", b"value-b", true)?;
    second.finish_writes();
    let (_second_plan, _second_reader, second_recovery) =
        second.recovery_inputs(Arc::new(RecoveryIo::default()))?;
    let second_before = ManagedInventory::inspect(&second.root, &second.format)?;

    let error = match first.execute_inputs(plan, &reader, second_recovery) {
        Ok(_) => panic!("cross-database recovery components must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert!(
        first
            .backend()
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .is_none()
    );
    assert_eq!(
        ManagedInventory::inspect(&second.root, &second.format)?,
        second_before
    );
    assert_eq!(second.read_value(b"second")?, Some(b"value-b".to_vec()));
    Ok(())
}

#[test]
fn recovery_inventory_io_and_illegal_object_errors_keep_open_recovery_context() -> TestResult {
    let mut io_harness = Harness::new()?;
    io_harness.put(b"stable", b"value", true)?;
    io_harness.finish_writes();
    let (plan, reader, recovery) = io_harness.recovery_inputs(Arc::new(RecoveryIo::default()))?;
    fail_next_inventory_inspect_for_test();
    let error = match io_harness.execute_inputs(plan, &reader, recovery) {
        Ok(_) => panic!("injected inventory I/O failure must stop recovery"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert!(error.write_outcome.is_none());
    assert!(error.instance_state.is_none());

    let mut layout_harness = Harness::new()?;
    layout_harness.put(b"stable", b"value", true)?;
    layout_harness.finish_writes();
    let (plan, reader, recovery) =
        layout_harness.recovery_inputs(Arc::new(RecoveryIo::default()))?;
    File::create(layout_harness.vlog_path.join("unexpected-object"))?;
    let error = match layout_harness.execute_inputs(plan, &reader, recovery) {
        Ok(_) => panic!("illegal VLog object must stop recovery"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    assert!(error.write_outcome.is_none());
    assert!(error.instance_state.is_none());
    Ok(())
}

#[test]
fn trim_removes_a_complete_orphan_envelope() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"stable-value", true)?;
    harness.put(b"orphan", b"complete-but-unpublished", false)?;
    harness.finish_writes();
    let before = harness.analyze()?;
    assert_eq!(
        (before.durable_frontier.durable_seq, before.head_seq),
        (1, 2)
    );
    let stable_end = before.durable_frontier.durable_vlog_end;

    harness.commit_mutations(vec![
        IndexMutation::DeleteUser {
            user_key: b"orphan".to_vec(),
        },
        IndexMutation::DeleteInternal {
            space: InternalIndexSpace::Transaction,
            key: encode_tx_meta_key(2)?.to_vec(),
        },
        IndexMutation::DeleteInternal {
            space: InternalIndexSpace::Transaction,
            key: encoded_mutation_key(2, 0),
        },
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: HEAD_SEQ_KEY.to_vec(),
            value: 1_u64.to_le_bytes().to_vec(),
        },
    ])?;

    let plan = harness.analyze()?;
    assert_eq!(plan.accepted_end, stable_end);
    assert!(!plan.needs_undo && !plan.needs_promote && plan.needs_trim);
    let recovered = harness.execute(Arc::new(RecoveryIo::default()))?;
    assert_eq!(recovered.writer.position(), expected_position(stable_end));
    assert!(harness.backend().get_user(b"orphan", None)?.is_none());
    assert_physical_tail(&harness, stable_end)?;
    Ok(())
}

#[test]
fn trim_removes_a_partial_record_suffix() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"value", true)?;
    harness.finish_writes();
    let stable = harness.analyze()?;
    let end = expected_position(stable.accepted_end);
    let mut planner = LayoutPlanner::from_position(harness.geometry, end)?;
    let envelope = prepare_envelope(
        &mut planner,
        DATABASE_UUID,
        2,
        [0x91; 16],
        &[LogicalOperationRef::Put {
            key: b"partial",
            value: b"record",
        }],
    )?;
    let first = envelope
        .chunks
        .first()
        .expect("transaction begins with a record");
    assert_eq!(first.position, end);
    assert!(matches!(
        decode_record_at(&first.bytes, first.position, harness.geometry)?,
        DecodedRecord::TxBegin(_)
    ));
    write_suffix(&harness, first.position, &first.bytes[..17])?;

    let plan = harness.analyze()?;
    assert!(plan.needs_trim && !plan.needs_undo && !plan.needs_promote);
    harness.execute(Arc::new(RecoveryIo::default()))?;
    assert_physical_tail(&harness, stable.accepted_end)?;
    Ok(())
}

#[test]
fn trim_removes_a_partial_page_end_suffix() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"large-start", &vec![0x31; 59_000], true)?;
    let current = harness
        .coordinator
        .as_ref()
        .expect("coordinator")
        .state_snapshot()
        .head_vlog_end
        .expect("first transaction end");
    let target_offset = harness
        .geometry
        .page_size
        .checked_sub(50)
        .expect("page is larger than a PageEnd");
    let mut sizing = LayoutPlanner::from_position(harness.geometry, current)?;
    let empty_filler = prepare_envelope(
        &mut sizing,
        DATABASE_UUID,
        2,
        [0x92; 16],
        &[LogicalOperationRef::Put {
            key: b"tail-fill",
            value: b"",
        }],
    )?;
    let filler_len = usize::try_from(
        target_offset
            .checked_sub(empty_filler.vlog_end.offset)
            .expect("empty filler ends before target"),
    )?;
    assert!(filler_len <= 60_000);
    harness.put(b"tail-fill", &vec![0x32; filler_len], true)?;
    let accepted = harness
        .coordinator
        .as_ref()
        .expect("coordinator")
        .state_snapshot()
        .head_vlog_end
        .expect("second transaction end");
    assert_eq!(accepted.offset, target_offset);
    harness.finish_writes();

    let mut planner = LayoutPlanner::from_position(harness.geometry, accepted)?;
    let next = prepare_envelope(
        &mut planner,
        DATABASE_UUID,
        3,
        [0x93; 16],
        &[LogicalOperationRef::Put {
            key: b"after-tail",
            value: b"x",
        }],
    )?;
    let page_end = next.chunks.first().expect("PageEnd prelude");
    assert_eq!(page_end.position, accepted);
    assert!(matches!(
        decode_record_at(&page_end.bytes, page_end.position, harness.geometry)?,
        DecodedRecord::PageEnd
    ));
    write_suffix(
        &harness,
        page_end.position,
        &page_end.bytes[..page_end.bytes.len() / 2],
    )?;

    let plan = harness.analyze()?;
    assert!(plan.needs_trim && !plan.needs_undo && !plan.needs_promote);
    harness.execute(Arc::new(RecoveryIo::default()))?;
    assert_physical_tail(
        &harness,
        DurableVLogEnd::Position(to_descriptor_pos(accepted)),
    )?;
    Ok(())
}

#[test]
fn trim_removes_a_gapped_high_number_file_with_a_complete_header() -> TestResult {
    let mut harness = Harness::new()?;
    harness.put(b"stable", b"value", true)?;
    harness.finish_writes();
    let stable = harness.analyze()?;
    let DurableVLogEnd::Position(end) = stable.accepted_end else {
        panic!("stable end");
    };
    let high_id = end.file_id.checked_add(7).expect("test file id");
    let high_path = harness.vlog_path.join(format!("D{high_id:06}.data"));
    let high = File::create(&high_path)?;
    high.write_all_at(
        &PageHeader {
            file_id: high_id,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    high.write_all_at(&VLogFileHeader::new(DATABASE_UUID, high_id).encode()?, 16)?;
    high.sync_data()?;

    let plan = harness.analyze()?;
    assert!(plan.needs_trim && !plan.needs_undo && !plan.needs_promote);
    harness.execute(Arc::new(RecoveryIo::default()))?;
    assert!(!high_path.exists());
    assert_physical_tail(&harness, stable.accepted_end)?;
    Ok(())
}

#[test]
fn production_single_and_cross_page_writes_reopen_analyze_recover_and_read_back() -> TestResult {
    let mut harness = Harness::new()?;
    let expected = vec![
        (b"stable".to_vec(), b"base".to_vec(), true),
        (b"single-page".to_vec(), b"short".to_vec(), false),
        (b"large-a".to_vec(), vec![0x41; 40_000], false),
        (b"large-b".to_vec(), vec![0x42; 25_000], false),
    ];
    for (key, value, sync) in &expected {
        harness.put(key, value, *sync)?;
    }
    harness.finish_writes();
    harness.reopen_backend()?;

    let plan = harness.analyze()?;
    assert_eq!((plan.durable_frontier.durable_seq, plan.head_seq), (1, 4));
    assert!(plan.needs_promote && !plan.needs_undo && !plan.needs_trim);
    assert!(
        plan.descriptors
            .iter()
            .any(|descriptor| { envelope_is_single_page(descriptor, harness.geometry.page_size) })
    );
    assert!(plan.descriptors.iter().any(|descriptor| {
        envelope_crosses_page_without_file(descriptor, harness.geometry.page_size)
    }));
    assert!(plan.descriptors.iter().all(|descriptor| {
        descriptor.meta.vlog_begin.file_id == 0 && descriptor.meta.vlog_end.file_id == 0
    }));

    let recovered = harness.execute(Arc::new(RecoveryIo::default()))?;
    assert_eq!(recovered.head_seq, 4);
    drop(recovered.writer);
    for (key, value, _) in expected {
        assert_eq!(harness.read_value(&key)?, Some(value));
    }
    Ok(())
}

#[test]
fn small_geometry_multi_file_writes_reopen_undo_trim_and_read_back() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 8)?;
    let mut harness = Harness::with_geometry(geometry)?;
    let mut expected = vec![(b"stable".to_vec(), b"base".to_vec(), true)];
    expected.push((b"single-page".to_vec(), b"short".to_vec(), false));
    for index in 0_u8..10 {
        expected.push((
            vec![b'm', index],
            vec![0x50_u8.wrapping_add(index); 40_000],
            false,
        ));
    }
    for (key, value, sync) in &expected {
        harness.put(key, value, *sync)?;
    }
    harness.finish_writes();
    let baseline = harness.analyze()?;
    assert_eq!(baseline.head_seq, 12);
    harness.flip_value_byte(&baseline.descriptors[9])?;
    harness.reopen_backend()?;

    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 1);
    assert_eq!(plan.head_seq, u64::try_from(expected.len())?);
    assert_eq!(plan.accepted_seq, 10);
    assert!(plan.needs_promote && plan.needs_undo && plan.needs_trim);
    assert!(
        plan.descriptors
            .iter()
            .any(|descriptor| { envelope_is_single_page(descriptor, geometry.page_size) })
    );
    assert!(
        plan.descriptors.iter().any(|descriptor| {
            envelope_crosses_page_without_file(descriptor, geometry.page_size)
        })
    );
    assert!(plan.descriptors.iter().any(|descriptor| {
        descriptor.meta.vlog_begin.file_id != descriptor.meta.vlog_end.file_id
    }));
    assert!(matches!(
        plan.accepted_end,
        DurableVLogEnd::Position(position) if position.file_id >= 2
    ));

    let recovered = harness.execute(Arc::new(RecoveryIo::default()))?;
    assert_eq!(recovered.head_seq, 10);
    assert_eq!(recovered.durable_frontier.durable_seq, 10);
    drop(recovered.writer);
    for (index, (key, value, _)) in expected.into_iter().enumerate() {
        if index < 10 {
            assert_eq!(harness.read_value(&key)?, Some(value));
        } else {
            assert_eq!(harness.read_value(&key)?, None);
        }
    }
    Ok(())
}

fn write_suffix(harness: &Harness, position: VLogPosition, bytes: &[u8]) -> TestResult {
    let path = harness
        .vlog_path
        .join(format!("D{:06}.data", position.file_id));
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.write_all_at(bytes, position.offset)?;
    file.sync_data()?;
    Ok(())
}

fn assert_physical_tail(harness: &Harness, expected: DurableVLogEnd) -> TestResult {
    let inventory = ManagedInventory::inspect(&harness.root, &harness.format)?;
    match expected {
        DurableVLogEnd::Empty => assert!(inventory.vlog_files.is_empty()),
        DurableVLogEnd::Position(end) => {
            let last = inventory.vlog_files.last().expect("retained boundary file");
            assert_eq!((last.file_id, last.len), (end.file_id, end.offset));
        }
    }
    Ok(())
}

fn to_descriptor_pos(position: VLogPosition) -> commit::VLogPos {
    commit::VLogPos {
        file_id: position.file_id,
        offset: position.offset,
    }
}

fn envelope_is_single_page(descriptor: &TransactionDescriptor, page_size: u64) -> bool {
    descriptor.meta.vlog_begin.file_id == descriptor.meta.vlog_end.file_id
        && descriptor.meta.vlog_begin.offset / page_size
            == descriptor.meta.vlog_end.offset.saturating_sub(1) / page_size
}

fn envelope_crosses_page_without_file(descriptor: &TransactionDescriptor, page_size: u64) -> bool {
    descriptor.meta.vlog_begin.file_id == descriptor.meta.vlog_end.file_id
        && descriptor.meta.vlog_begin.offset / page_size
            != descriptor.meta.vlog_end.offset.saturating_sub(1) / page_size
}
