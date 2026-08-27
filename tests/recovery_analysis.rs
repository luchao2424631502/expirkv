#![allow(dead_code, unused_imports)]

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    RecoveryState, TransactionDescriptor, TxUuidSource, ValueState, encode_tx_meta_key,
    preflight_batch, preflight_put,
};
use db::{ManagedInventory, VLogInventoryEntry};
use format::FormatMetadataV0;
use index::{
    FjallBackend, FjallIndexOptions, IndexAtomicBatch, IndexBackend, IndexCommitError,
    IndexCommitMode, IndexCompression, IndexEntry, IndexMutation, InternalIndexSpace,
    InternalKeyRange, initialization_batch,
};
use recovery::{
    PhysicalTail, RecoveryPlan, analyze_recovery, analyze_recovery_with_test_geometry,
    unstable_descriptor_range,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{VLogGeometry, VLogPosition, ValuePointer};
use vlog::reader::{EnvelopeValueState, ValueLogReader};
use vlog::writer::{ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x52; 16];

#[derive(Default)]
struct CountingWriterIo {
    writes: AtomicUsize,
    file_syncs: AtomicUsize,
    directory_syncs: AtomicUsize,
}

impl CountingWriterIo {
    fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.writes.load(Ordering::SeqCst),
            self.file_syncs.load(Ordering::SeqCst),
            self.directory_syncs.load(Ordering::SeqCst),
        )
    }
}

impl WriterIo for CountingWriterIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        self.writes.fetch_add(1, Ordering::SeqCst);
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
}

struct CountingBackend {
    inner: FjallBackend,
    commits: AtomicUsize,
    calls: Mutex<Vec<&'static str>>,
    scan_ranges: Mutex<Vec<InternalKeyRange>>,
    corrupt_identity: AtomicBool,
}

impl CountingBackend {
    fn new(inner: FjallBackend) -> Self {
        Self {
            inner,
            commits: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            scan_ranges: Mutex::new(Vec::new()),
            corrupt_identity: AtomicBool::new(false),
        }
    }

    fn commit_count(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
        self.scan_ranges.lock().unwrap().clear();
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }

    fn scan_ranges(&self) -> Vec<InternalKeyRange> {
        self.scan_ranges.lock().unwrap().clone()
    }
}

impl IndexBackend for CountingBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner.commit_atomic(batch, mode)
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        self.calls.lock().unwrap().push("identity");
        let mut identity = self.inner.get_database_identity()?;
        if self.corrupt_identity.load(Ordering::SeqCst) {
            if let Some(encoded) = identity.as_mut() {
                encoded[0] ^= 0xff;
            }
        }
        Ok(identity)
    }

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        self.calls.lock().unwrap().push("user");
        self.inner.get_user(key, snapshot)
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.calls.lock().unwrap().push("internal");
        self.inner.get_internal(space, key)
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        self.calls.lock().unwrap().push("scan");
        self.scan_ranges.lock().unwrap().push(range.clone());
        self.inner.scan_internal(space, range)
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        self.inner.snapshot()
    }

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        self.inner.iter_user(snapshot)
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
    backend: Arc<CountingBackend>,
    coordinator: CommitCoordinator<CountingBackend, FixedUuid>,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
    vlog_path: PathBuf,
    geometry: VLogGeometry,
    format: FormatMetadataV0,
    writer_io: Arc<CountingWriterIo>,
}

impl Harness {
    fn new() -> TestResult<Self> {
        Self::with_geometry(VLogGeometry::PRODUCTION)
    }

    fn with_geometry(geometry: VLogGeometry) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let backend = Arc::new(CountingBackend::new(FjallBackend::create(
            &index_path,
            fjall_options(),
        )?));
        backend
            .commit_atomic(
                initialization_batch(0, DATABASE_UUID)
                    .map_err(|error| io::Error::other(format!("initial batch: {error:?}")))?,
                IndexCommitMode::SyncAll,
            )
            .map_err(|error| io::Error::other(format!("initial commit: {error:?}")))?;

        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        let writer_io = Arc::new(CountingWriterIo::default());
        let writer = ValueLogWriter::empty_with_io(
            Arc::clone(&directory),
            DATABASE_UUID,
            geometry,
            Arc::clone(&catalog),
            Arc::clone(&writer_io) as Arc<dyn WriterIo>,
        )?;
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let coordinator = CommitCoordinator::new(
            runtime,
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
            backend,
            coordinator,
            directory,
            catalog,
            vlog_path,
            geometry,
            format: FormatMetadataV0::new(DATABASE_UUID)?,
            writer_io,
        })
    }

    fn put(&self, key: &[u8], value: &[u8], sync: bool) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_put(key, value, sync)?)?;
        Ok(())
    }

    fn batch(&self, operations: &[(&[u8], &[u8])], sync: bool) -> TestResult {
        let mut batch = WriteBatch::new();
        for (key, value) in operations {
            batch.put(key, value)?;
        }
        self.commit_batch(&batch, sync)
    }

    fn commit_batch(&self, batch: &WriteBatch, sync: bool) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_batch(batch, sync)?)?;
        Ok(())
    }

    fn inventory(&self) -> TestResult<ManagedInventory> {
        inventory_from(&self.vlog_path)
    }

    fn reader(&self) -> Result<ValueLogReader> {
        let files = Arc::new(FileSet::new(
            Arc::clone(&self.directory),
            DATABASE_UUID,
            self.geometry,
            Arc::clone(&self.catalog),
            4,
        )?);
        ValueLogReader::new(files, self.geometry)
    }

    fn analyze(&self) -> Result<RecoveryPlan> {
        let inventory = self.inventory().map_err(|_| test_io_error())?;
        let reader = self.reader()?;
        analyze_recovery(self.backend.as_ref(), &self.format, &inventory, &reader)
    }

    fn analyze_with_test_geometry(&self) -> Result<RecoveryPlan> {
        let inventory = self.inventory().map_err(|_| test_io_error())?;
        let reader = self.reader()?;
        analyze_recovery_with_test_geometry(
            self.backend.as_ref(),
            &self.format,
            &inventory,
            &reader,
        )
    }

    fn descriptor_entries(&self, commit_seq: u64) -> TestResult<Vec<IndexEntry>> {
        let mut entries = self
            .backend
            .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())?
            .collect::<Result<Vec<_>>>()?;
        entries.retain(|entry| {
            entry
                .key
                .get(2..10)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_be_bytes)
                == Some(commit_seq)
        });
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(entries)
    }

    fn commit_index_mutations(&self, mutations: Vec<IndexMutation>) -> TestResult {
        let mut batch = IndexAtomicBatch::try_with_capacity(mutations.len())
            .map_err(|error| io::Error::other(format!("test batch allocation: {error:?}")))?;
        for mutation in mutations {
            batch
                .try_push(mutation)
                .map_err(|error| io::Error::other(format!("test batch mutation: {error:?}")))?;
        }
        self.backend
            .commit_atomic(batch, IndexCommitMode::SyncAll)
            .map_err(|error| io::Error::other(format!("test mutation commit: {error:?}")))?;
        Ok(())
    }

    fn install_recovery_state(&self, state: RecoveryState) -> TestResult {
        self.commit_index_mutations(vec![IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: RECOVERY_STATE_KEY.to_vec(),
            value: state.encode()?.to_vec(),
        }])
    }

    fn flip_value_record_byte(&self, descriptor: &TransactionDescriptor) -> TestResult {
        let pointer = descriptor
            .mutations
            .iter()
            .find_map(|mutation| match mutation.after_state {
                ValueState::Present(pointer) => Some(pointer),
                ValueState::Absent => None,
            })
            .expect("test transaction must contain a Put");
        self.flip_pointer_byte(pointer)
    }

    fn flip_pointer_byte(&self, pointer: ValuePointer) -> TestResult {
        let path = self.vlog_path.join(format!("D{:06}.data", pointer.file_id));
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let offset = u64::from(pointer.record_offset) + 39;
        let mut byte = [0_u8; 1];
        file.read_exact_at(&mut byte, offset)?;
        byte[0] ^= 0x80;
        file.write_all_at(&byte, offset)?;
        Ok(())
    }

    fn truncate_vlog_to(&self, end: DurableVLogEnd) -> TestResult {
        match end {
            DurableVLogEnd::Empty => {
                for entry in std::fs::read_dir(&self.vlog_path)? {
                    let entry = entry?;
                    if entry.file_type()?.is_file() {
                        std::fs::remove_file(entry.path())?;
                    }
                }
            }
            DurableVLogEnd::Position(position) => {
                for entry in std::fs::read_dir(&self.vlog_path)? {
                    let entry = entry?;
                    let name = entry.file_name();
                    let name = name.to_str().expect("test VLog name is UTF-8");
                    let file_id = name[1..7].parse::<u32>()?;
                    if file_id > position.file_id {
                        std::fs::remove_file(entry.path())?;
                    }
                }
                OpenOptions::new()
                    .write(true)
                    .open(
                        self.vlog_path
                            .join(format!("D{:06}.data", position.file_id)),
                    )?
                    .set_len(position.offset)?;
            }
        }
        Ok(())
    }

    fn extend_suffix_after(&self, end: DurableVLogEnd, extra_len: u64) -> TestResult {
        let DurableVLogEnd::Position(position) = end else {
            return Err(io::Error::other("test suffix requires a nonempty VLog").into());
        };
        let extended_len = position
            .offset
            .checked_add(extra_len)
            .ok_or_else(|| io::Error::other("test suffix length overflow"))?;
        assert!(extended_len <= self.geometry.max_file_size);
        OpenOptions::new()
            .write(true)
            .open(
                self.vlog_path
                    .join(format!("D{:06}.data", position.file_id)),
            )?
            .set_len(extended_len)?;
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

fn inventory_from(vlog_path: &Path) -> TestResult<ManagedInventory> {
    let mut vlog_files = Vec::new();
    for entry in std::fs::read_dir(vlog_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().expect("test file name is UTF-8");
        let file_id = name[1..7].parse::<u32>()?;
        vlog_files.push(VLogInventoryEntry {
            file_id,
            len: entry.metadata()?.len(),
            path: entry.path(),
        });
    }
    vlog_files.sort_by_key(|entry| entry.file_id);
    Ok(ManagedInventory { vlog_files })
}

fn test_io_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    )
}

fn assert_corruption(result: Result<RecoveryPlan>) {
    let error = result.expect_err("recovery analysis must fail closed");
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
}

fn assert_actions(plan: &RecoveryPlan, undo: bool, promote: bool, trim: bool) {
    assert_eq!(plan.needs_undo, undo, "needs_undo");
    assert_eq!(plan.needs_promote, promote, "needs_promote");
    assert_eq!(plan.needs_trim, trim, "needs_trim");
}

#[test]
fn d_equals_h_is_noop_identity_is_first_and_analysis_performs_no_writes() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable-a", b"one", false)?;
    harness.put(b"stable-b", b"two", true)?;

    harness.backend.clear_calls();
    let commits_before = harness.backend.commit_count();
    let writes_before = harness.writer_io.snapshot();
    let files_before = inventory_from(&harness.vlog_path)?;
    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 2);
    assert_eq!(plan.head_seq, 2);
    assert_eq!(plan.accepted_seq, 2);
    assert!(!plan.needs_undo);
    assert!(!plan.needs_promote);
    assert!(!plan.needs_trim);
    assert_eq!(harness.backend.calls().first(), Some(&"identity"));
    assert_eq!(
        harness.backend.scan_ranges(),
        vec![InternalKeyRange {
            start_inclusive: Some(encode_tx_meta_key(3)?[..10].to_vec()),
            end_exclusive: None,
        }]
    );
    assert_eq!(harness.backend.commit_count(), commits_before);
    assert_eq!(harness.writer_io.snapshot(), writes_before);
    assert_eq!(inventory_from(&harness.vlog_path)?, files_before);

    harness.backend.clear_calls();
    harness
        .backend
        .corrupt_identity
        .store(true, Ordering::SeqCst);
    assert_corruption(harness.analyze());
    assert_eq!(harness.backend.calls(), vec!["identity"]);
    assert_eq!(harness.backend.commit_count(), commits_before);
    assert_eq!(harness.writer_io.snapshot(), writes_before);
    Ok(())
}

#[test]
fn empty_database_d_equals_h_zero_produces_an_owned_noop_plan() -> TestResult {
    let harness = Harness::new()?;
    harness.backend.clear_calls();
    let commits_before = harness.backend.commit_count();
    let writes_before = harness.writer_io.snapshot();

    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 0);
    assert_eq!(
        plan.durable_frontier.durable_vlog_end,
        DurableVLogEnd::Empty
    );
    assert_eq!(plan.head_seq, 0);
    assert_eq!(plan.accepted_seq, 0);
    assert_eq!(plan.published_end, DurableVLogEnd::Empty);
    assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
    assert_eq!(plan.physical_tail, PhysicalTail::Empty);
    assert!(plan.descriptors.is_empty());
    assert_eq!(plan.recovery_state, None);
    assert_actions(&plan, false, false, false);
    assert_eq!(harness.backend.calls().first(), Some(&"identity"));
    assert_eq!(
        harness.backend.scan_ranges(),
        vec![InternalKeyRange {
            start_inclusive: Some(encode_tx_meta_key(1)?[..10].to_vec()),
            end_exclusive: None,
        }]
    );
    assert_eq!(harness.backend.commit_count(), commits_before);
    assert_eq!(harness.writer_io.snapshot(), writes_before);
    assert!(harness.inventory()?.vlog_files.is_empty());
    Ok(())
}

#[test]
fn valid_unstable_prefix_requires_only_promotion() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"unstable-a", b"v2", false)?;
    harness.put(b"unstable-b", b"v3", false)?;
    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 1);
    assert_eq!(plan.accepted_seq, 3);
    assert_eq!(plan.head_seq, 3);
    assert!(plan.needs_promote);
    assert!(!plan.needs_undo);
    assert!(!plan.needs_trim);
    assert_eq!(plan.accepted_end, plan.published_end);
    Ok(())
}

#[test]
fn recovery_action_flags_cover_every_reachable_combination() -> TestResult {
    // trim only: stable metadata ends at E, but an unreferenced physical suffix
    // remains after E.
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        let baseline = harness.analyze()?;
        harness.extend_suffix_after(baseline.accepted_end, 1)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.accepted_seq, 1);
        assert_actions(&plan, false, false, true);
    }

    // promote + trim: the whole published transaction is accepted, followed
    // by bytes which no Descriptor owns.
    {
        let harness = Harness::new()?;
        harness.put(b"accepted", b"v1", false)?;
        let baseline = harness.analyze()?;
        harness.extend_suffix_after(baseline.accepted_end, 1)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.accepted_seq, 1);
        assert_actions(&plan, false, true, true);
    }

    // undo only: all bytes after the stable boundary disappeared, while the
    // unstable Descriptor and published HeadSeq survived in Fjall.
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        harness.put(b"lost", b"v2", false)?;
        let baseline = harness.analyze()?;
        harness.truncate_vlog_to(baseline.durable_frontier.durable_vlog_end)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.accepted_seq, plan.durable_frontier.durable_seq);
        assert_actions(&plan, true, false, false);
    }

    // promote + undo without trim: transaction 2 remains complete, while all
    // physical bytes belonging to transaction 3 disappeared.
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        harness.put(b"accepted", b"v2", false)?;
        harness.put(b"lost", b"v3", false)?;
        let baseline = harness.analyze()?;
        let accepted_end = DurableVLogEnd::Position(baseline.descriptors[0].meta.vlog_end);
        harness.truncate_vlog_to(accepted_end)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.accepted_seq, 2);
        assert_eq!(plan.accepted_end, accepted_end);
        assert_actions(&plan, true, true, false);
    }

    // The remaining four combinations are covered by the dedicated tests:
    // none, promote only, undo+trim, and promote+undo+trim.
    Ok(())
}

#[test]
fn invalid_middle_envelope_stops_c_even_when_later_envelope_is_valid() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"accepted", b"v2", false)?;
    harness.put(b"broken", b"v3", false)?;
    harness.put(b"later-valid", b"v4", false)?;
    let baseline = harness.analyze()?;
    let broken = baseline.descriptors[1].clone();
    harness.flip_value_record_byte(&broken)?;

    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 1);
    assert_eq!(plan.accepted_seq, 2);
    assert_eq!(plan.head_seq, 4);
    assert!(plan.needs_promote);
    assert!(plan.needs_undo);
    assert!(plan.needs_trim);
    assert_ne!(plan.accepted_end, plan.published_end);
    Ok(())
}

#[test]
fn invalid_first_unstable_envelope_yields_c_equal_d_with_undo_and_trim() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"broken", b"v2", false)?;
    harness.put(b"later", b"v3", false)?;
    let baseline = harness.analyze()?;
    harness.flip_value_record_byte(&baseline.descriptors[0])?;

    let plan = harness.analyze()?;
    assert_eq!(plan.accepted_seq, plan.durable_frontier.durable_seq);
    assert!(!plan.needs_promote);
    assert!(plan.needs_undo);
    assert!(plan.needs_trim);
    Ok(())
}

#[test]
fn descriptor_scan_skips_stable_history_but_validates_the_entire_head_suffix() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"unstable", b"v2", false)?;

    // This noncanonical key sorts inside transaction 1's stable key range. It
    // must not be interpreted while recovery validates only transaction 2.
    let mut malformed_stable_key = encode_tx_meta_key(1)?.to_vec();
    malformed_stable_key.push(0xff);
    harness.commit_index_mutations(vec![IndexMutation::PutInternal {
        space: InternalIndexSpace::Transaction,
        key: malformed_stable_key,
        value: vec![1],
    }])?;
    harness.backend.clear_calls();

    let plan = harness.analyze()?;
    assert_eq!(plan.durable_frontier.durable_seq, 1);
    assert_eq!(plan.head_seq, 2);
    assert_eq!(plan.accepted_seq, 2);
    assert_eq!(
        harness.backend.scan_ranges(),
        vec![InternalKeyRange {
            start_inclusive: Some(encode_tx_meta_key(2)?[..10].to_vec()),
            end_exclusive: None,
        }]
    );

    // Tightening the lower bound must not weaken validation inside the actual
    // unstable range.
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"unstable", b"v2", false)?;
    let mut malformed_unstable_key = encode_tx_meta_key(2)?.to_vec();
    malformed_unstable_key.push(0xff);
    harness.commit_index_mutations(vec![IndexMutation::PutInternal {
        space: InternalIndexSpace::Transaction,
        key: malformed_unstable_key,
        value: vec![1],
    }])?;
    assert_corruption(harness.analyze());

    // The first unstable sequence's short prefix sorts before its canonical
    // TxMeta key, but it is still part of the suffix that must fail closed.
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"unstable", b"v2", false)?;
    harness.commit_index_mutations(vec![IndexMutation::PutInternal {
        space: InternalIndexSpace::Transaction,
        key: encode_tx_meta_key(2)?[..10].to_vec(),
        value: vec![1],
    }])?;
    assert_corruption(harness.analyze());
    Ok(())
}

#[test]
fn unstable_descriptor_range_covers_empty_normal_invalid_and_max_head_boundaries() -> TestResult {
    assert_eq!(
        unstable_descriptor_range(7, 7)?,
        Some(InternalKeyRange {
            start_inclusive: Some(encode_tx_meta_key(8)?[..10].to_vec()),
            end_exclusive: None,
        })
    );
    assert_eq!(
        unstable_descriptor_range(7, 9)?,
        Some(InternalKeyRange {
            start_inclusive: Some(encode_tx_meta_key(8)?[..10].to_vec()),
            end_exclusive: None,
        })
    );
    let max_range = unstable_descriptor_range(u64::MAX - 1, u64::MAX)?
        .expect("the final representable transaction remains scannable");
    assert_eq!(
        max_range.start_inclusive,
        Some(encode_tx_meta_key(u64::MAX)?[..10].to_vec())
    );
    assert_eq!(max_range.end_exclusive, None);
    assert_eq!(unstable_descriptor_range(u64::MAX, u64::MAX)?, None);
    let error = unstable_descriptor_range(9, 8).expect_err("D > H must fail closed");
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Recovery);
    Ok(())
}

#[test]
fn transaction_metadata_beyond_head_fails_closed_with_and_without_an_unstable_prefix() -> TestResult
{
    for stable_head in [true, false] {
        let harness = Harness::new()?;
        harness.put(b"one", b"v1", stable_head)?;
        if !stable_head {
            harness.put(b"two", b"v2", false)?;
        }
        let head_seq = if stable_head { 1 } else { 2 };
        harness.commit_index_mutations(vec![IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key: encode_tx_meta_key(head_seq + 1)?.to_vec(),
            value: vec![1],
        }])?;

        assert_corruption(harness.analyze());
    }
    Ok(())
}

#[test]
fn later_descriptor_corruption_is_found_before_any_envelope_prefix_is_selected() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"first-broken-envelope", b"v1", false)?;
    harness.put(b"middle", b"v2", false)?;
    harness.put(b"last-broken-descriptor", b"v3", false)?;
    let baseline = harness.analyze()?;
    harness.flip_value_record_byte(&baseline.descriptors[0])?;

    let mut entries = harness.descriptor_entries(3)?;
    let meta = entries
        .iter_mut()
        .find(|entry| entry.key.len() == 11)
        .expect("TxMeta");
    meta.value[82] ^= 1;
    harness.commit_index_mutations(vec![IndexMutation::PutInternal {
        space: InternalIndexSpace::Transaction,
        key: meta.key.clone(),
        value: meta.value.clone(),
    }])?;

    assert_corruption(harness.analyze());
    Ok(())
}

#[test]
fn missing_unstable_file_allows_published_end_beyond_physical_tail() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 4)?;
    let harness = Harness::with_geometry(geometry)?;
    let value = vec![0x6a; 40_000];
    for seq in 1..=7_u8 {
        harness.put(&[b'k', seq], &value, false)?;
    }
    let baseline = harness.analyze_with_test_geometry()?;
    let published = baseline.published_end;
    let DurableVLogEnd::Position(published) = published else {
        panic!("nonempty workload must publish a position");
    };
    assert!(published.file_id >= 1);
    std::fs::remove_file(
        harness
            .vlog_path
            .join(format!("D{:06}.data", published.file_id)),
    )?;

    let plan = harness.analyze_with_test_geometry()?;
    assert!(plan.accepted_seq < plan.head_seq);
    assert!(plan.needs_undo);
    assert!(plan.needs_trim);
    let PhysicalTail::Position(tail) = plan.physical_tail else {
        panic!("an earlier physical file must remain");
    };
    assert!(published.file_id > tail.file_id);
    Ok(())
}

#[test]
fn existing_recovery_state_keeps_its_target_instead_of_recomputing_c() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"one", b"v1", false)?;
    harness.put(b"two", b"v2", false)?;
    harness.put(b"three", b"v3", false)?;
    let baseline = harness.analyze()?;
    assert_eq!(baseline.accepted_seq, 3);
    let target_end = DurableVLogEnd::Position(baseline.descriptors[0].meta.vlog_end);
    let state = RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 3,
        target_seq: 1,
        target_vlog_end: target_end,
        next_undo_seq: 3,
        trim_required: true,
    };
    harness.install_recovery_state(state)?;

    let plan = harness.analyze()?;
    assert_eq!(plan.recovery_state, Some(state));
    assert_eq!(plan.accepted_seq, 1);
    assert_eq!(plan.accepted_end, target_end);
    assert!(plan.needs_promote);
    assert!(plan.needs_undo);
    assert!(plan.needs_trim);
    Ok(())
}

#[test]
fn undo_state_allows_a_previously_required_suffix_to_be_already_gone() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"rejected", b"v2", false)?;
    let baseline = harness.analyze()?;
    let target_end = baseline.durable_frontier.durable_vlog_end;
    let state = RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 2,
        target_seq: 1,
        target_vlog_end: target_end,
        next_undo_seq: 2,
        trim_required: true,
    };
    harness.install_recovery_state(state)?;
    harness.truncate_vlog_to(target_end)?;

    let plan = harness.analyze()?;
    assert_eq!(plan.recovery_state, Some(state));
    assert_eq!(plan.accepted_seq, 1);
    assert_eq!(plan.accepted_end, target_end);
    assert_actions(&plan, true, false, false);
    Ok(())
}

#[test]
fn undo_state_rejects_an_unplanned_new_physical_suffix() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"rejected", b"v2", false)?;
    let baseline = harness.analyze()?;
    let state = RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 2,
        target_seq: 1,
        target_vlog_end: baseline.durable_frontier.durable_vlog_end,
        next_undo_seq: 2,
        trim_required: false,
    };
    harness.install_recovery_state(state)?;
    assert_corruption(harness.analyze());
    Ok(())
}

#[test]
fn undo_state_without_trim_continues_when_the_physical_tail_matches_target() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"stable", b"v1", true)?;
    harness.put(b"lost", b"v2", false)?;
    let baseline = harness.analyze()?;
    let target_end = baseline.durable_frontier.durable_vlog_end;
    harness.truncate_vlog_to(target_end)?;
    let state = RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 2,
        target_seq: 1,
        target_vlog_end: target_end,
        next_undo_seq: 2,
        trim_required: false,
    };
    harness.install_recovery_state(state)?;

    let plan = harness.analyze()?;
    assert_eq!(plan.recovery_state, Some(state));
    assert_actions(&plan, true, false, false);
    Ok(())
}

#[test]
fn trim_and_finalize_states_cover_complete_and_incomplete_physical_cleanup() -> TestResult {
    // Trim remains valid both before cleanup and after the physical operation
    // has already completed but its next SyncAll state transition has not.
    for suffix_present in [true, false] {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        let baseline = harness.analyze()?;
        if suffix_present {
            harness.extend_suffix_after(baseline.accepted_end, 1)?;
        }
        let state = RecoveryState {
            phase: RecoveryPhase::Trim,
            original_head: 1,
            target_seq: 1,
            target_vlog_end: baseline.accepted_end,
            next_undo_seq: 1,
            trim_required: true,
        };
        harness.install_recovery_state(state)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.recovery_state, Some(state));
        assert_actions(&plan, false, false, suffix_present);
    }

    // Finalize is valid only after no physical suffix remains.
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        let baseline = harness.analyze()?;
        let state = RecoveryState {
            phase: RecoveryPhase::Finalize,
            original_head: 1,
            target_seq: 1,
            target_vlog_end: baseline.accepted_end,
            next_undo_seq: 1,
            trim_required: false,
        };
        harness.install_recovery_state(state)?;
        let plan = harness.analyze()?;
        assert_eq!(plan.recovery_state, Some(state));
        assert_actions(&plan, false, false, false);
    }
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        let baseline = harness.analyze()?;
        harness.extend_suffix_after(baseline.accepted_end, 1)?;
        let state = RecoveryState {
            phase: RecoveryPhase::Finalize,
            original_head: 1,
            target_seq: 1,
            target_vlog_end: baseline.accepted_end,
            next_undo_seq: 1,
            trim_required: false,
        };
        harness.install_recovery_state(state)?;
        assert_corruption(harness.analyze());
    }
    Ok(())
}

#[test]
fn empty_target_reentry_covers_undo_trim_and_finalize_phases() -> TestResult {
    // Undo to Empty accepts the three idempotent combinations and rejects an
    // unplanned suffix introduced after a no-trim decision was persisted.
    for (trim_required, suffix_present, succeeds) in [
        (true, true, true),
        (true, false, true),
        (false, false, true),
        (false, true, false),
    ] {
        let harness = Harness::new()?;
        harness.put(b"unstable", b"value", false)?;
        if !suffix_present {
            harness.truncate_vlog_to(DurableVLogEnd::Empty)?;
        }
        let state = RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 1,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 1,
            trim_required,
        };
        harness.install_recovery_state(state)?;

        if succeeds {
            let plan = harness.analyze()?;
            assert_eq!(plan.recovery_state, Some(state));
            assert_eq!(plan.durable_frontier.durable_seq, 0);
            assert_eq!(plan.head_seq, 1);
            assert_eq!(plan.accepted_seq, 0);
            assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
            assert_actions(&plan, true, false, suffix_present);
        } else {
            assert_corruption(harness.analyze());
        }
    }

    // Trim to Empty is valid both before physical deletion and after the
    // deletion completed but before RecoveryState was removed.
    for suffix_present in [true, false] {
        let harness = Harness::new()?;
        if suffix_present {
            File::create(harness.vlog_path.join("D000000.data"))?;
        }
        let state = RecoveryState {
            phase: RecoveryPhase::Trim,
            original_head: 1,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 0,
            trim_required: true,
        };
        harness.install_recovery_state(state)?;

        let plan = harness.analyze()?;
        assert_eq!(plan.recovery_state, Some(state));
        assert_eq!(plan.head_seq, 0);
        assert_eq!(plan.accepted_seq, 0);
        assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
        assert_actions(&plan, false, false, suffix_present);
    }

    // Finalize to Empty is valid only after the physical suffix is gone.
    for suffix_present in [false, true] {
        let harness = Harness::new()?;
        if suffix_present {
            File::create(harness.vlog_path.join("D000000.data"))?;
        }
        let state = RecoveryState {
            phase: RecoveryPhase::Finalize,
            original_head: 1,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 0,
            trim_required: false,
        };
        harness.install_recovery_state(state)?;

        if suffix_present {
            assert_corruption(harness.analyze());
        } else {
            let plan = harness.analyze()?;
            assert_eq!(plan.recovery_state, Some(state));
            assert_eq!(plan.head_seq, 0);
            assert_eq!(plan.accepted_seq, 0);
            assert_eq!(plan.accepted_end, DurableVLogEnd::Empty);
            assert_actions(&plan, false, false, false);
        }
    }
    Ok(())
}

#[test]
fn existing_recovery_state_rejects_phase_metadata_and_target_mismatches() -> TestResult {
    // A fixed target may never fall behind the already durable sequence.
    {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        harness.install_recovery_state(RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 1,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 1,
            trim_required: true,
        })?;
        assert_corruption(harness.analyze());
    }

    // Undo must resume exactly at head_seq == next_undo_seq.
    {
        let harness = Harness::new()?;
        harness.put(b"unstable", b"v1", false)?;
        harness.install_recovery_state(RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 1,
            target_seq: 0,
            target_vlog_end: DurableVLogEnd::Empty,
            next_undo_seq: 0,
            trim_required: true,
        })?;
        assert_corruption(harness.analyze());
    }

    // Trim and Finalize both require H == D == target and E == target_end.
    for phase in [RecoveryPhase::Trim, RecoveryPhase::Finalize] {
        let harness = Harness::new()?;
        harness.put(b"stable", b"v1", true)?;
        let baseline = harness.analyze()?;
        let DurableVLogEnd::Position(end) = baseline.accepted_end else {
            panic!("stable transaction must have an end");
        };
        harness.install_recovery_state(RecoveryState {
            phase,
            original_head: 2,
            target_seq: 2,
            target_vlog_end: DurableVLogEnd::Position(end),
            next_undo_seq: 2,
            trim_required: phase == RecoveryPhase::Trim,
        })?;
        assert_corruption(harness.analyze());
    }

    // A fixed sequence is insufficient: the exact persisted target end must
    // equal the corresponding Descriptor end.
    {
        let harness = Harness::new()?;
        harness.put(b"unstable", b"v1", false)?;
        let baseline = harness.analyze()?;
        let DurableVLogEnd::Position(end) = baseline.accepted_end else {
            panic!("transaction must have an end");
        };
        let wrong_end = DurableVLogEnd::Position(commit::VLogPos {
            file_id: end.file_id,
            offset: end.offset - 1,
        });
        harness.install_recovery_state(RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: 2,
            target_seq: 1,
            target_vlog_end: wrong_end,
            next_undo_seq: 1,
            trim_required: true,
        })?;
        assert_corruption(harness.analyze());
    }
    Ok(())
}

#[test]
fn fixed_recovery_target_fails_closed_when_its_accepted_envelope_is_damaged() -> TestResult {
    let harness = Harness::new()?;
    harness.put(b"accepted", b"v1", false)?;
    harness.put(b"rejected", b"v2", false)?;
    let baseline = harness.analyze()?;
    let target_end = DurableVLogEnd::Position(baseline.descriptors[0].meta.vlog_end);
    harness.install_recovery_state(RecoveryState {
        phase: RecoveryPhase::Undo,
        original_head: 2,
        target_seq: 1,
        target_vlog_end: target_end,
        next_undo_seq: 2,
        trim_required: true,
    })?;
    harness.flip_value_record_byte(&baseline.descriptors[0])?;
    assert_corruption(harness.analyze());
    Ok(())
}

#[test]
fn recovery_reader_reconciles_repeated_puts_and_deletes_across_pages_and_files() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 4)?;
    let harness = Harness::with_geometry(geometry)?;
    let large_a = vec![0x41; 50_000];
    let large_b = vec![0x42; 50_000];
    let large_d = vec![0x44; 40_000];
    let mut batch = WriteBatch::new();
    batch.put(b"a", &large_a)?;
    batch.put(b"b", &large_b)?;
    batch.delete(b"a")?;
    batch.put(b"c", b"")?;
    batch.put(b"d", &large_d)?;
    batch.delete(b"b")?;
    batch.put(b"a", b"final-a")?;
    harness.commit_batch(&batch, false)?;

    let plan = harness.analyze_with_test_geometry()?;
    assert_eq!(plan.accepted_seq, 1);
    assert_actions(&plan, false, true, false);
    let descriptor = &plan.descriptors[0];
    assert!(descriptor.meta.vlog_end.file_id >= 1);
    let reader = harness.reader()?;
    let envelope = reader.read_recovery_envelope(
        VLogPosition {
            file_id: descriptor.meta.vlog_begin.file_id,
            offset: descriptor.meta.vlog_begin.offset,
        },
        VLogPosition {
            file_id: descriptor.meta.vlog_end.file_id,
            offset: descriptor.meta.vlog_end.offset,
        },
        Some(descriptor.meta.envelope_crc32c),
    )?;
    assert_eq!(
        envelope
            .final_states
            .iter()
            .map(|state| state.user_key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice()
        ]
    );

    let EnvelopeValueState::Present(a_pointer) = envelope.final_states[0].state else {
        panic!("a must end Present");
    };
    assert_eq!(envelope.final_states[1].state, EnvelopeValueState::Absent);
    let EnvelopeValueState::Present(c_pointer) = envelope.final_states[2].state else {
        panic!("c must end Present");
    };
    let EnvelopeValueState::Present(d_pointer) = envelope.final_states[3].state else {
        panic!("d must end Present");
    };
    assert_eq!(reader.read_pointer(a_pointer, b"a")?, b"final-a");
    assert_eq!(reader.read_pointer(c_pointer, b"c")?, b"");
    assert_eq!(reader.read_pointer(d_pointer, b"d")?, large_d);
    assert_eq!(
        descriptor
            .mutations
            .iter()
            .map(|mutation| mutation.user_key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice()
        ]
    );
    assert_eq!(
        descriptor.mutations[0].after_state,
        ValueState::Present(a_pointer)
    );
    assert_eq!(descriptor.mutations[1].after_state, ValueState::Absent);
    assert_eq!(
        descriptor.mutations[2].after_state,
        ValueState::Present(c_pointer)
    );
    assert_eq!(
        descriptor.mutations[3].after_state,
        ValueState::Present(d_pointer)
    );
    Ok(())
}

#[test]
fn stable_boundary_reverse_read_round_trips_one_transaction_across_pages_and_files() -> TestResult {
    let geometry = VLogGeometry::test_only(65_536, 131_072, 4)?;
    let harness = Harness::with_geometry(geometry)?;
    let large_a = vec![0x41; 50_000];
    let large_b = vec![0x42; 50_000];
    let large_c = vec![0x43; 50_000];
    let mut batch = WriteBatch::new();
    batch.put(b"a", &large_a)?;
    batch.put(b"b", &large_b)?;
    batch.put(b"c", &large_c)?;
    batch.delete(b"b")?;
    batch.put(b"a", b"final-a")?;
    harness.commit_batch(&batch, true)?;

    // The formal analyzer is bound to the frozen 64 KiB / 4 GiB geometry and
    // must never accept this 128 KiB file-roll simulation as a production DB.
    harness.backend.clear_calls();
    assert_corruption(harness.analyze());
    assert_eq!(harness.backend.calls(), vec!["identity"]);

    // The test geometry remains useful for the Reader algorithm itself: obtain
    // the real persisted frontier without routing the simulated file limit
    // through production recovery topology validation.
    let encoded_frontier = harness
        .backend
        .get_internal(InternalIndexSpace::System, index::DURABLE_FRONTIER_KEY)?
        .expect("durable frontier");
    let frontier = DurableFrontier::decode(&encoded_frontier)?;
    assert_eq!(frontier.durable_seq, 1);
    let DurableVLogEnd::Position(stable_end) = frontier.durable_vlog_end else {
        panic!("the stable transaction must have a physical end");
    };
    assert!(stable_end.file_id >= 1, "transaction must cross a file");
    let inventory = harness.inventory()?;
    assert!(inventory.vlog_files.len() >= 2);
    assert_eq!(inventory.vlog_files[0].len, geometry.max_file_size);

    let reader = harness.reader()?;
    let envelope = reader.read_stable_envelope_from_end(VLogPosition {
        file_id: stable_end.file_id,
        offset: stable_end.offset,
    })?;
    assert_eq!(envelope.scanned.commit_seq, 1);
    assert_ne!(envelope.scanned.tx_uuid, [0; 16]);
    assert_eq!(
        envelope.scanned.vlog_begin,
        VLogPosition {
            file_id: 0,
            offset: 0,
        }
    );
    assert_eq!(
        envelope.scanned.vlog_end,
        VLogPosition {
            file_id: stable_end.file_id,
            offset: stable_end.offset,
        }
    );
    assert_eq!(envelope.scanned.logical_op_count, 5);
    assert_eq!(envelope.scanned.distinct_key_count, 3);
    assert_eq!(envelope.scanned.kv_record_count, 4);
    assert_eq!(envelope.scanned.delete_record_count, 1);
    assert_eq!(
        envelope
            .final_states
            .iter()
            .map(|state| state.user_key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );

    let EnvelopeValueState::Present(a_pointer) = envelope.final_states[0].state else {
        panic!("a must end Present");
    };
    assert_eq!(envelope.final_states[1].state, EnvelopeValueState::Absent);
    let EnvelopeValueState::Present(c_pointer) = envelope.final_states[2].state else {
        panic!("c must end Present");
    };
    assert_eq!(reader.read_pointer(a_pointer, b"a")?, b"final-a");
    assert_eq!(reader.read_pointer(c_pointer, b"c")?, large_c);
    Ok(())
}

#[test]
fn stable_boundary_missing_truncated_bad_crc_or_bad_uuid_fails_closed() -> TestResult {
    for fault in ["missing", "truncated", "crc", "uuid"] {
        let harness = Harness::new()?;
        harness.put(b"stable", b"durable", true)?;
        let baseline = harness.analyze()?;
        let DurableVLogEnd::Position(end) = baseline.durable_frontier.durable_vlog_end else {
            panic!("stable transaction must have a boundary");
        };
        let path = harness.vlog_path.join(format!("D{:06}.data", end.file_id));
        match fault {
            "missing" => std::fs::remove_file(path)?,
            "truncated" => {
                OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(end.offset - 1)?;
            }
            "crc" => {
                let encoded = harness
                    .backend
                    .get_user(b"stable", None)?
                    .expect("stable user pointer");
                harness.flip_pointer_byte(ValuePointer::decode(&encoded)?)?;
            }
            "uuid" => {
                let file = OpenOptions::new().read(true).write(true).open(path)?;
                let mut byte = [0_u8; 1];
                file.read_exact_at(&mut byte, 28)?;
                byte[0] ^= 1;
                file.write_all_at(&byte, 28)?;
            }
            _ => unreachable!(),
        }
        assert_corruption(harness.analyze());
    }
    Ok(())
}

#[test]
fn unstable_descriptor_missing_gap_partial_crc_and_unexplainable_state_fail_closed() -> TestResult {
    for fault in ["missing", "gap", "partial", "crc", "state"] {
        let harness = Harness::new()?;
        harness.batch(&[(b"a", b"one"), (b"b", b"two")], false)?;
        harness.put(b"c", b"three", false)?;
        harness.put(b"d", b"four", false)?;
        let seq = if fault == "gap" { 2 } else { 1 };
        let mut entries = harness.descriptor_entries(seq)?;
        let meta_index = entries
            .iter()
            .position(|entry| entry.key.len() == 11)
            .expect("TxMeta");
        let mutation_index = entries
            .iter()
            .position(|entry| entry.key.len() == 19)
            .expect("TxMutation");
        let mutations = match fault {
            "missing" | "gap" => vec![IndexMutation::DeleteInternal {
                space: InternalIndexSpace::Transaction,
                key: entries[meta_index].key.clone(),
            }],
            "partial" => vec![IndexMutation::DeleteInternal {
                space: InternalIndexSpace::Transaction,
                key: entries[mutation_index].key.clone(),
            }],
            "crc" => {
                entries[meta_index].value[82] ^= 1;
                vec![IndexMutation::PutInternal {
                    space: InternalIndexSpace::Transaction,
                    key: entries[meta_index].key.clone(),
                    value: entries[meta_index].value.clone(),
                }]
            }
            "state" => {
                let value = &mut entries[mutation_index].value;
                let key_len = usize::from(u16::from_le_bytes([value[0], value[1]]));
                value[2 + key_len] = 2;
                rewrite_descriptor_crc(&mut entries);
                entries
                    .into_iter()
                    .map(|entry| IndexMutation::PutInternal {
                        space: InternalIndexSpace::Transaction,
                        key: entry.key,
                        value: entry.value,
                    })
                    .collect()
            }
            _ => unreachable!(),
        };
        harness.commit_index_mutations(mutations)?;
        assert_corruption(harness.analyze());
    }
    Ok(())
}

fn rewrite_descriptor_crc(entries: &mut [IndexEntry]) {
    use crc32c::{crc32c, crc32c_append};

    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let meta_index = entries
        .iter()
        .position(|entry| entry.key.len() == 11)
        .expect("TxMeta");
    let mut crc = crc32c(b"RKDESC0");
    crc = crc32c_append(crc, &entries[meta_index].value[..82]);
    for entry in entries.iter().filter(|entry| entry.key.len() == 19) {
        crc = crc32c_append(crc, &entry.key);
        crc = crc32c_append(crc, &(entry.value.len() as u32).to_le_bytes());
        crc = crc32c_append(crc, &entry.value);
    }
    entries[meta_index].value[82..86].copy_from_slice(&crc.to_le_bytes());
}
