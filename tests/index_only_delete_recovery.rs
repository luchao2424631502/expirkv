#![allow(dead_code, unused_imports)]

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
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
    RecoveryState, TransactionDescriptor, TxMutation, TxUuidSource, ValueState, decode_head_seq,
    preflight_put,
};
use db::ManagedInventory;
use format::FormatMetadataV0;
use index::{
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, HEAD_SEQ_KEY, IndexAtomicBatch,
    IndexBackend, IndexCommitMode, IndexCompression, IndexMutation, InternalIndexSpace,
    initialization_batch,
};
use lock::RootLock;
use recovery::{analyze_recovery, execute_recovery};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{VLogGeometry, ValuePointer};
use vlog::reader::ValueLogReader;
use vlog::writer::{ValueLogRecovery, ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x91; 16];

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

impl RecoveryIo {
    fn snapshot(&self) -> (usize, usize, usize, usize) {
        (
            self.file_syncs.load(Ordering::SeqCst),
            self.directory_syncs.load(Ordering::SeqCst),
            self.truncates.load(Ordering::SeqCst),
            self.deletes.load(Ordering::SeqCst),
        )
    }
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
    backend: Arc<FjallBackend>,
    coordinator: Option<CommitCoordinator<FjallBackend, FixedUuid>>,
    vlog_path: PathBuf,
    format: FormatMetadataV0,
}

impl Harness {
    fn new() -> TestResult<Self> {
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
        let writer = ValueLogWriter::empty(
            directory,
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::new(FileCatalog::new()),
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
                durable_vlog_seq: 0,
                durable_vlog_end: DurableVLogEnd::Empty,
            },
            0,
            None,
        )?;

        Ok(Self {
            _temporary: temporary,
            root,
            backend,
            coordinator: Some(coordinator),
            vlog_path,
            format,
        })
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

    fn restart_empty_writer_at(&mut self, head_seq: u64) -> TestResult {
        assert!(self.coordinator.is_none());
        assert_eq!(std::fs::read_dir(&self.vlog_path)?.count(), 0);
        let directory = Arc::new(VLogDirectory::open(&self.vlog_path)?);
        let writer = ValueLogWriter::empty(
            directory,
            DATABASE_UUID,
            VLogGeometry::PRODUCTION,
            Arc::new(FileCatalog::new()),
        )?;
        let stats = Arc::new(StatsState::new());
        self.coordinator = Some(CommitCoordinator::new(
            RuntimeControl::new(Arc::clone(&stats)),
            stats,
            Arc::clone(&self.backend),
            writer,
            FixedUuid(0x40),
            head_seq,
            DurableFrontier {
                // The current coordinator contract only accepts a recovered
                // runtime frontier. This test-only writer is used solely to
                // append transaction `head_seq + 1`; the on-disk frontier is
                // deliberately left at zero for Open analysis.
                durable_seq: head_seq,
                durable_vlog_seq: 0,
                durable_vlog_end: DurableVLogEnd::Empty,
            },
            0,
            None,
        )?);
        Ok(())
    }

    fn install_index_only(
        &self,
        commit_seq: u64,
        logical_op_count: u64,
        mutations: Vec<TxMutation>,
    ) -> TestResult {
        let encoded = TransactionDescriptor::encode_index_only_delete_for_test(
            commit_seq,
            [u8::try_from(commit_seq).unwrap_or(0xfe); 16],
            logical_op_count,
            mutations.clone(),
        )?;
        let capacity = mutations
            .len()
            .checked_add(encoded.mutations.len())
            .and_then(|count| count.checked_add(2))
            .ok_or_else(|| io::Error::other("test batch capacity overflow"))?;
        let mut batch = IndexAtomicBatch::try_with_capacity(capacity)
            .map_err(|error| io::Error::other(format!("test batch allocation: {error:?}")))?;
        for mutation in mutations {
            assert_eq!(mutation.after_state, ValueState::Absent);
            batch
                .try_push(IndexMutation::DeleteUser {
                    user_key: mutation.user_key,
                })
                .map_err(|error| io::Error::other(format!("delete user: {error:?}")))?;
        }
        batch
            .try_push(IndexMutation::PutInternal {
                space: InternalIndexSpace::Transaction,
                key: encoded.meta_key.to_vec(),
                value: encoded.meta_value.to_vec(),
            })
            .map_err(|error| io::Error::other(format!("put meta: {error:?}")))?;
        for mutation in encoded.mutations {
            batch
                .try_push(IndexMutation::PutInternal {
                    space: InternalIndexSpace::Transaction,
                    key: mutation.key.to_vec(),
                    value: mutation.value,
                })
                .map_err(|error| io::Error::other(format!("put mutation: {error:?}")))?;
        }
        batch
            .try_push(IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: HEAD_SEQ_KEY.to_vec(),
                value: commit_seq.to_le_bytes().to_vec(),
            })
            .map_err(|error| io::Error::other(format!("put head: {error:?}")))?;
        self.backend
            .commit_atomic(batch, IndexCommitMode::SyncAll)
            .map_err(|error| io::Error::other(format!("index-only commit: {error:?}")))?;
        Ok(())
    }

    fn install_state(&self, state: RecoveryState) -> TestResult {
        let mut batch = IndexAtomicBatch::try_with_capacity(1)
            .map_err(|error| io::Error::other(format!("state batch: {error:?}")))?;
        batch
            .try_push(IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: RECOVERY_STATE_KEY.to_vec(),
                value: state.encode()?.to_vec(),
            })
            .map_err(|error| io::Error::other(format!("put state: {error:?}")))?;
        self.backend
            .commit_atomic(batch, IndexCommitMode::SyncAll)
            .map_err(|error| io::Error::other(format!("state commit: {error:?}")))?;
        Ok(())
    }

    fn pointer(&self, key: &[u8]) -> TestResult<ValuePointer> {
        let encoded = self
            .backend
            .get_user(key, None)?
            .ok_or_else(|| io::Error::other("pointer missing"))?;
        Ok(ValuePointer::decode(&encoded)?)
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
            VLogGeometry::PRODUCTION,
            catalog,
            2,
        )?);
        let reader = ValueLogReader::new(Arc::clone(&files), VLogGeometry::PRODUCTION)?;
        let plan = analyze_recovery(self.backend.as_ref(), &self.format, &inventory, &reader)?;
        let recovery = ValueLogRecovery::new_with_io(files, io)?;
        Ok((plan, reader, recovery))
    }

    fn execute(&self, io: Arc<dyn WriterIo>) -> Result<recovery::RecoveredState> {
        let (plan, reader, vlog) = self.recovery_inputs(io)?;
        execute_recovery(
            self.backend.as_ref(),
            plan,
            &self.root,
            &self.format,
            &reader,
            vlog,
        )
    }

    fn head_and_frontier(&self) -> TestResult<(u64, DurableFrontier)> {
        let head = self
            .backend
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .ok_or_else(|| io::Error::other("head missing"))?;
        let frontier = self
            .backend
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .ok_or_else(|| io::Error::other("frontier missing"))?;
        Ok((decode_head_seq(&head)?, DurableFrontier::decode(&frontier)?))
    }

    fn truncate_vlog_tail_byte(&self) -> TestResult {
        let path = self.vlog_path.join("D000000.data");
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        let len = file.metadata()?.len();
        file.set_len(
            len.checked_sub(1)
                .ok_or_else(|| io::Error::other("empty VLog"))?,
        )?;
        Ok(())
    }

    fn into_public_open(mut self) -> (TempDir, PathBuf) {
        self.finish_writes();
        let path = self._temporary.path().to_path_buf();
        let Self {
            _temporary,
            root,
            backend,
            coordinator,
            ..
        } = self;
        drop(coordinator);
        drop(backend);
        drop(root);
        (_temporary, path)
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

fn absent_delete(key: &[u8]) -> TxMutation {
    TxMutation {
        user_key: key.to_vec(),
        before_state: ValueState::Absent,
        after_state: ValueState::Absent,
    }
}

#[test]
fn pure_index_only_prefix_promotes_d_without_vlog_io_and_reopens_with_empty_end() -> TestResult {
    let mut harness = Harness::new()?;
    harness.finish_writes();
    harness.install_index_only(1, 1, vec![absent_delete(b"missing-a")])?;
    harness.install_index_only(2, 2, vec![absent_delete(b"missing-b")])?;

    let io = Arc::new(RecoveryIo::default());
    let (plan, reader, vlog) = harness.recovery_inputs(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(plan.accepted_seq, 2);
    assert_eq!(plan.accepted_vlog_seq, 0);
    assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
    assert!(!plan.needs_undo && plan.needs_promote && !plan.needs_trim);
    let recovered = execute_recovery(
        harness.backend.as_ref(),
        plan,
        &harness.root,
        &harness.format,
        &reader,
        vlog,
    )?;
    assert_eq!(recovered.head_seq, 2);
    assert_eq!(
        recovered.durable_frontier,
        DurableFrontier {
            durable_seq: 2,
            durable_vlog_seq: 0,
            durable_vlog_end: DurableVLogEnd::Empty,
        }
    );
    assert_eq!(io.snapshot(), (0, 0, 0, 0));
    assert_eq!(std::fs::read_dir(&harness.vlog_path)?.count(), 0);

    drop(recovered.writer);
    let reopened_io = Arc::new(RecoveryIo::default());
    let reopened = harness.execute(Arc::clone(&reopened_io) as Arc<dyn WriterIo>)?;
    assert_eq!(reopened.head_seq, 2);
    assert_eq!(reopened.durable_frontier.durable_vlog_seq, 0);
    assert_eq!(
        reopened.durable_frontier.durable_vlog_end,
        DurableVLogEnd::Empty
    );
    assert_eq!(reopened_io.snapshot(), (0, 0, 0, 0));
    Ok(())
}

#[test]
fn public_open_recovers_a_pure_index_only_database_with_no_vlog_files() -> TestResult {
    let mut harness = Harness::new()?;
    harness.finish_writes();
    harness.install_index_only(1, 1, vec![absent_delete(b"missing")])?;
    let (_temporary, path) = harness.into_public_open();

    {
        let db = rustkv::Db::open(&rustkv::Options::default(), &path)?;
        assert_eq!(db.get(&rustkv::ReadOptions::default(), b"missing")?, None);
        let stats = db.stats();
        assert_eq!((stats.head_seq, stats.durable_seq), (1, 1));
        assert!(stats.durable_vlog_end.is_none());
    }
    let reopened = rustkv::Db::open(&rustkv::Options::default(), &path)?;
    let stats = reopened.stats();
    assert_eq!((stats.head_seq, stats.durable_seq), (1, 1));
    assert!(stats.durable_vlog_end.is_none());
    assert_eq!(std::fs::read_dir(path.join("vlog"))?.count(), 0);
    Ok(())
}

#[test]
fn trailing_index_only_delete_advances_logical_frontier_but_keeps_last_vlog_boundary() -> TestResult
{
    let mut harness = Harness::new()?;
    harness.put(b"victim", b"durable-value", true)?;
    let before = harness.pointer(b"victim")?;
    harness.finish_writes();
    let (_, stable) = harness.head_and_frontier()?;
    harness.install_index_only(
        2,
        1,
        vec![TxMutation {
            user_key: b"victim".to_vec(),
            before_state: ValueState::Present(before),
            after_state: ValueState::Absent,
        }],
    )?;

    let io = Arc::new(RecoveryIo::default());
    let (plan, reader, vlog) = harness.recovery_inputs(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(plan.accepted_seq, 2);
    assert_eq!(plan.accepted_vlog_seq, 1);
    assert_eq!(plan.accepted_end, stable.durable_vlog_end);
    assert!(!plan.needs_undo && plan.needs_promote && !plan.needs_trim);
    let recovered = execute_recovery(
        harness.backend.as_ref(),
        plan,
        &harness.root,
        &harness.format,
        &reader,
        vlog,
    )?;
    assert_eq!(recovered.durable_frontier.durable_seq, 2);
    assert_eq!(recovered.durable_frontier.durable_vlog_seq, 1);
    assert_eq!(
        recovered.durable_frontier.durable_vlog_end,
        stable.durable_vlog_end
    );
    assert!(harness.backend.get_user(b"victim", None)?.is_none());
    assert_eq!(io.snapshot(), (0, 0, 0, 0));
    Ok(())
}

#[test]
fn interleaved_index_only_vlog_index_only_prefix_advances_c_and_cf_independently() -> TestResult {
    let mut harness = Harness::new()?;
    harness.finish_writes();
    harness.install_index_only(1, 1, vec![absent_delete(b"missing-before")])?;
    harness.restart_empty_writer_at(1)?;
    harness.put(b"present", b"value", false)?;
    harness.finish_writes();
    harness.install_index_only(3, 1, vec![absent_delete(b"missing-after")])?;

    let io = Arc::new(RecoveryIo::default());
    let (plan, reader, vlog) = harness.recovery_inputs(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(plan.accepted_seq, 3);
    assert_eq!(plan.accepted_vlog_seq, 2);
    assert_eq!(plan.published_end, plan.accepted_end);
    assert!(!plan.needs_undo && plan.needs_promote && !plan.needs_trim);
    let target_end = plan.accepted_end;
    let recovered = execute_recovery(
        harness.backend.as_ref(),
        plan,
        &harness.root,
        &harness.format,
        &reader,
        vlog,
    )?;
    assert_eq!(
        recovered.durable_frontier,
        DurableFrontier {
            durable_seq: 3,
            durable_vlog_seq: 2,
            durable_vlog_end: target_end,
        }
    );
    assert!(io.file_syncs.load(Ordering::SeqCst) >= 1);
    assert!(harness.backend.get_user(b"present", None)?.is_some());
    Ok(())
}

#[test]
fn index_only_descriptor_cannot_cross_an_invalid_vlog_hole_and_is_undone_in_reverse() -> TestResult
{
    let mut harness = Harness::new()?;
    harness.put(b"victim", b"unstable-value", false)?;
    let before = harness.pointer(b"victim")?;
    harness.finish_writes();
    harness.install_index_only(
        2,
        1,
        vec![TxMutation {
            user_key: b"victim".to_vec(),
            before_state: ValueState::Present(before),
            after_state: ValueState::Absent,
        }],
    )?;
    harness.truncate_vlog_tail_byte()?;

    let io = Arc::new(RecoveryIo::default());
    let (plan, reader, vlog) = harness.recovery_inputs(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(plan.accepted_seq, 0);
    assert_eq!(plan.accepted_vlog_seq, 0);
    assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
    assert!(plan.needs_undo && !plan.needs_promote && plan.needs_trim);
    let recovered = execute_recovery(
        harness.backend.as_ref(),
        plan,
        &harness.root,
        &harness.format,
        &reader,
        vlog,
    )?;
    assert_eq!(recovered.head_seq, 0);
    assert_eq!(recovered.durable_frontier.durable_seq, 0);
    assert!(harness.backend.get_user(b"victim", None)?.is_none());
    assert_eq!(std::fs::read_dir(&harness.vlog_path)?.count(), 0);
    assert_eq!(harness.head_and_frontier()?.0, 0);
    Ok(())
}

#[test]
fn fixed_recovery_state_reenters_an_index_only_target_without_vlog_io() -> TestResult {
    let mut harness = Harness::new()?;
    harness.finish_writes();
    harness.install_index_only(1, 1, vec![absent_delete(b"missing")])?;
    harness.install_state(RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 1,
        target_seq: 1,
        target_vlog_seq: 0,
        target_vlog_end: DurableVLogEnd::Empty,
        next_undo_seq: 1,
        trim_required: false,
    })?;

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(
        recovered.durable_frontier,
        DurableFrontier {
            durable_seq: 1,
            durable_vlog_seq: 0,
            durable_vlog_end: DurableVLogEnd::Empty,
        }
    );
    assert!(
        harness
            .backend
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .is_none()
    );
    assert_eq!(io.snapshot(), (0, 0, 0, 0));
    Ok(())
}

#[test]
fn fixed_mixed_target_with_trailing_index_only_trims_to_its_last_vlog_end() -> TestResult {
    let mut harness = Harness::new()?;
    harness.finish_writes();
    harness.install_index_only(1, 1, vec![absent_delete(b"missing-before")])?;
    harness.restart_empty_writer_at(1)?;
    harness.put(b"present", b"value", false)?;
    harness.finish_writes();
    harness.install_index_only(3, 1, vec![absent_delete(b"missing-after")])?;

    let (target, reader, recovery) = harness.recovery_inputs(Arc::new(RecoveryIo::default()))?;
    drop(recovery);
    drop(reader);
    let DurableVLogEnd::Position(end) = target.accepted_end else {
        panic!("mixed target must have a VLog end");
    };
    assert_eq!((target.accepted_seq, target.accepted_vlog_seq), (3, 2));
    let path = harness.vlog_path.join(format!("D{:06}.data", end.file_id));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    file.sync_all()?;
    file.set_len(end.offset + 17)?;
    file.sync_all()?;
    harness.install_state(RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 3,
        target_seq: 3,
        target_vlog_seq: 2,
        target_vlog_end: target.accepted_end,
        next_undo_seq: 3,
        trim_required: true,
    })?;

    let io = Arc::new(RecoveryIo::default());
    let recovered = harness.execute(Arc::clone(&io) as Arc<dyn WriterIo>)?;
    assert_eq!(
        recovered.durable_frontier,
        DurableFrontier {
            durable_seq: 3,
            durable_vlog_seq: 2,
            durable_vlog_end: target.accepted_end,
        }
    );
    assert_eq!(std::fs::metadata(path)?.len(), end.offset);
    assert_eq!(io.truncates.load(Ordering::SeqCst), 1);
    assert!(
        harness
            .backend
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)?
            .is_none()
    );
    Ok(())
}
