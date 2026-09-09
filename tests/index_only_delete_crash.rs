#![allow(dead_code, unused_imports)]

use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::fs::FileExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
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

use commit::{
    CommitCoordinator, DurableFrontier, DurableVLogEnd, TransactionDescriptor, TransactionKind,
    TxUuidSource, VLogPos, ValueState, decode_descriptor, decode_head_seq, decode_tx_meta_key,
    decode_tx_mutation_key, encode_tx_meta_key, preflight_batch, preflight_delete, preflight_put,
};
use format::{FORMAT_FILE_NAME, FormatMetadataV0};
use index::{
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, HEAD_SEQ_KEY, IndexApplyState,
    IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexCompression,
    IndexEntry, IndexMutation, InternalIndexError, InternalIndexSpace, InternalKeyRange,
};
use lock::RootLock;
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{VLogGeometry, VLogPosition};
use vlog::reader::ValueLogReader;
use vlog::writer::{ValueLogWriter, WriterIo};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CHILD_ENV: &str = "RUSTKV_INDEX_ONLY_CRASH_CHILD";
const CHILD_PATH_ENV: &str = "RUSTKV_INDEX_ONLY_CRASH_PATH";
const CHILD_CASE_ENV: &str = "RUSTKV_INDEX_ONLY_CRASH_CASE";
const POINT_PREFIX: &str = "RUSTKV_INDEX_ONLY_CRASH_POINT";
const TARGET_KEY: &[u8] = b"target";
const MISSING_KEY: &[u8] = b"missing";
const DIRTY_KEY: &[u8] = b"dirty-put";
const TARGET_VALUE: &[u8] = b"stable-target-value";
const DIRTY_VALUE: &[u8] = b"accepted-dirty-value";
const SIGKILL: i32 = 9;
#[cfg(target_os = "linux")]
const SIGSTOP: i32 = 19;
#[cfg(target_os = "macos")]
const SIGSTOP: i32 = 17;
const POINT_TIMEOUT: Duration = Duration::from_secs(20);

unsafe extern "C" {
    fn getpid() -> i32;
    fn kill(pid: i32, signal: i32) -> i32;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultAction {
    StopBeforeFjall,
    ReturnNotApplied,
    ReturnUnknownAfterApply,
    Success,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CrashCase {
    name: &'static str,
    sync: bool,
    dirty_put: bool,
    stop_before_sync: bool,
    action: FaultAction,
}

const CRASH_CASES: &[CrashCase] = &[
    CrashCase {
        name: "buffer-before-fjall",
        sync: false,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::StopBeforeFjall,
    },
    CrashCase {
        name: "buffer-definitely-not-applied",
        sync: false,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::ReturnNotApplied,
    },
    CrashCase {
        name: "buffer-possibly-applied",
        sync: false,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::ReturnUnknownAfterApply,
    },
    CrashCase {
        name: "buffer-success",
        sync: false,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::Success,
    },
    CrashCase {
        name: "clean-sync-before-fjall",
        sync: true,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::StopBeforeFjall,
    },
    CrashCase {
        name: "clean-sync-possibly-applied",
        sync: true,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::ReturnUnknownAfterApply,
    },
    CrashCase {
        name: "clean-sync-success",
        sync: true,
        dirty_put: false,
        stop_before_sync: false,
        action: FaultAction::Success,
    },
    CrashCase {
        name: "dirty-before-vlog-sync",
        sync: true,
        dirty_put: true,
        stop_before_sync: true,
        action: FaultAction::Success,
    },
    CrashCase {
        name: "dirty-after-vlog-sync-before-fjall",
        sync: true,
        dirty_put: true,
        stop_before_sync: false,
        action: FaultAction::StopBeforeFjall,
    },
    CrashCase {
        name: "dirty-syncall-definitely-not-applied",
        sync: true,
        dirty_put: true,
        stop_before_sync: false,
        action: FaultAction::ReturnNotApplied,
    },
    CrashCase {
        name: "dirty-syncall-possibly-applied",
        sync: true,
        dirty_put: true,
        stop_before_sync: false,
        action: FaultAction::ReturnUnknownAfterApply,
    },
    CrashCase {
        name: "dirty-syncall-success",
        sync: true,
        dirty_put: true,
        stop_before_sync: false,
        action: FaultAction::Success,
    },
];

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

struct CrashPointBackend {
    inner: Arc<FjallBackend>,
    target_mode: IndexCommitMode,
    action: FaultAction,
    root: PathBuf,
    case_name: &'static str,
    before_delete_vlog: Arc<Mutex<String>>,
}

impl IndexBackend for CrashPointBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        if mode != self.target_mode {
            return self.inner.commit_atomic(batch, mode);
        }
        match self.action {
            FaultAction::StopBeforeFjall => {
                stop_process(
                    self.case_name,
                    &self
                        .before_delete_vlog
                        .lock()
                        .expect("crash evidence mutex poisoned"),
                    &self.root,
                );
            }
            FaultAction::ReturnNotApplied => Err(IndexCommitError::not_applied(
                InternalIndexError::new(StorageErrorKind::Io, Some(5)),
            )),
            FaultAction::ReturnUnknownAfterApply => {
                self.inner.commit_atomic(batch, mode)?;
                Err(IndexCommitError::unknown(InternalIndexError::new(
                    StorageErrorKind::Io,
                    Some(5),
                )))
            }
            FaultAction::Success => self.inner.commit_atomic(batch, mode),
        }
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

struct StopBeforeVLogSyncIo {
    root: PathBuf,
    case_name: &'static str,
    before_delete_vlog: Arc<Mutex<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VisibilityPhase {
    Waiting,
    BeforeApply,
    AfterApply,
    Returned,
}

struct VisibilityState {
    phase: VisibilityPhase,
    release_before_apply: bool,
    release_after_apply: bool,
}

struct VisibilityBackend {
    inner: Arc<FjallBackend>,
    state: Mutex<VisibilityState>,
    changed: Condvar,
}

impl VisibilityBackend {
    fn new(inner: Arc<FjallBackend>) -> Self {
        Self {
            inner,
            state: Mutex::new(VisibilityState {
                phase: VisibilityPhase::Waiting,
                release_before_apply: false,
                release_after_apply: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait_for_phase(&self, expected: VisibilityPhase) -> TestResult {
        let deadline = Instant::now() + POINT_TIMEOUT;
        let mut state = self.state.lock().map_err(|_| "visibility mutex poisoned")?;
        while state.phase != expected {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::other(format!(
                    "timed out waiting for visibility phase {expected:?}; current={:?}",
                    state.phase
                ))
                .into());
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "visibility mutex poisoned while waiting")?;
            state = next;
            if wait.timed_out() && state.phase != expected {
                return Err(io::Error::other(format!(
                    "timed out waiting for visibility phase {expected:?}; current={:?}",
                    state.phase
                ))
                .into());
            }
        }
        Ok(())
    }

    fn release_before_apply(&self) {
        let mut state = self.state.lock().expect("visibility mutex poisoned");
        state.release_before_apply = true;
        drop(state);
        self.changed.notify_all();
    }

    fn release_after_apply(&self) {
        let mut state = self.state.lock().expect("visibility mutex poisoned");
        state.release_after_apply = true;
        drop(state);
        self.changed.notify_all();
    }
}

impl IndexBackend for VisibilityBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        {
            let mut state = self.state.lock().expect("visibility mutex poisoned");
            assert_eq!(state.phase, VisibilityPhase::Waiting);
            state.phase = VisibilityPhase::BeforeApply;
            self.changed.notify_all();
            let deadline = Instant::now() + POINT_TIMEOUT;
            while !state.release_before_apply {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "release-before-apply timed out");
                let (next, wait) = self.changed.wait_timeout(state, remaining).unwrap();
                state = next;
                assert!(
                    !wait.timed_out() || state.release_before_apply,
                    "release-before-apply timed out"
                );
            }
        }

        self.inner.commit_atomic(batch, mode)?;

        let mut state = self.state.lock().expect("visibility mutex poisoned");
        state.phase = VisibilityPhase::AfterApply;
        self.changed.notify_all();
        let deadline = Instant::now() + POINT_TIMEOUT;
        while !state.release_after_apply {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "release-after-apply timed out");
            let (next, wait) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(
                !wait.timed_out() || state.release_after_apply,
                "release-after-apply timed out"
            );
        }
        state.phase = VisibilityPhase::Returned;
        drop(state);
        self.changed.notify_all();
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

#[derive(Default)]
struct ScriptedState {
    calls: Vec<(IndexAtomicBatch, IndexCommitMode)>,
    released_through: usize,
}

struct ScriptedBackend {
    inner: Arc<FjallBackend>,
    state: Mutex<ScriptedState>,
    changed: Condvar,
}

impl ScriptedBackend {
    fn new(inner: Arc<FjallBackend>) -> Self {
        Self {
            inner,
            state: Mutex::new(ScriptedState::default()),
            changed: Condvar::new(),
        }
    }

    fn wait_for_call(&self, call: usize) -> TestResult<(IndexAtomicBatch, IndexCommitMode)> {
        assert!(call > 0);
        let deadline = Instant::now() + POINT_TIMEOUT;
        let mut state = self.state.lock().map_err(|_| "script mutex poisoned")?;
        while state.calls.len() < call {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::other(format!(
                    "timed out waiting for backend call {call}; observed={}",
                    state.calls.len()
                ))
                .into());
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "script mutex poisoned while waiting")?;
            state = next;
            if wait.timed_out() && state.calls.len() < call {
                return Err(io::Error::other(format!(
                    "timed out waiting for backend call {call}; observed={}",
                    state.calls.len()
                ))
                .into());
            }
        }
        Ok(state.calls[call - 1].clone())
    }

    fn release(&self, call: usize) {
        let mut state = self.state.lock().expect("script mutex poisoned");
        assert!(call <= state.calls.len());
        state.released_through = state.released_through.max(call);
        drop(state);
        self.changed.notify_all();
    }

    fn calls(&self) -> Vec<(IndexAtomicBatch, IndexCommitMode)> {
        self.state
            .lock()
            .expect("script mutex poisoned")
            .calls
            .clone()
    }
}

impl IndexBackend for ScriptedBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        let call = {
            let mut state = self.state.lock().expect("script mutex poisoned");
            state.calls.push((batch.clone(), mode));
            let call = state.calls.len();
            self.changed.notify_all();
            let deadline = Instant::now() + POINT_TIMEOUT;
            while state.released_through < call {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "backend call {call} release timed out"
                );
                let (next, wait) = self.changed.wait_timeout(state, remaining).unwrap();
                state = next;
                assert!(
                    !wait.timed_out() || state.released_through >= call,
                    "backend call {call} release timed out"
                );
            }
            call
        };
        debug_assert!(call > 0);
        self.inner.commit_atomic(batch, mode)
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

impl WriterIo for StopBeforeVLogSyncIo {
    fn write_at(&self, file: &File, bytes: &[u8], offset: u64) -> io::Result<usize> {
        file.write_at(bytes, offset)
    }

    fn sync_file(&self, _file: &File) -> io::Result<()> {
        stop_process(
            self.case_name,
            &self
                .before_delete_vlog
                .lock()
                .expect("crash evidence mutex poisoned"),
            &self.root,
        )
    }

    fn sync_directory(&self, directory: &VLogDirectory) -> io::Result<()> {
        directory.sync()
    }
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

fn public_create_options() -> rustkv::Options {
    rustkv::Options {
        create_if_missing: true,
        ..rustkv::Options::default()
    }
}

fn descriptor_end(end: DurableVLogEnd) -> Option<VLogPosition> {
    match end {
        DurableVLogEnd::Empty => None,
        DurableVLogEnd::Position(position) => Some(VLogPosition {
            file_id: position.file_id,
            offset: position.offset,
        }),
    }
}

fn read_format(root: &Path) -> TestResult<FormatMetadataV0> {
    Ok(FormatMetadataV0::decode(&std::fs::read(
        root.join(FORMAT_FILE_NAME),
    )?)?)
}

fn vlog_inventory(root: &Path) -> TestResult<Vec<(u32, u64)>> {
    let path = root.join("vlog");
    let mut inventory = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| io::Error::other("non-UTF8 VLog file name"))?;
        let Some(id) = name
            .strip_prefix('D')
            .and_then(|rest| rest.strip_suffix(".data"))
            .and_then(|digits| digits.parse::<u32>().ok())
        else {
            return Err(io::Error::other(format!("unexpected VLog entry {name:?}")).into());
        };
        inventory.push((id, entry.metadata()?.len()));
    }
    inventory.sort_unstable();
    Ok(inventory)
}

fn encode_inventory(inventory: &[(u32, u64)]) -> String {
    if inventory.is_empty() {
        return "empty".to_owned();
    }
    inventory
        .iter()
        .map(|(file_id, len)| format!("{file_id}:{len}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn inventory_end(inventory: &[(u32, u64)]) -> DurableVLogEnd {
    inventory
        .last()
        .map_or(DurableVLogEnd::Empty, |(file_id, offset)| {
            DurableVLogEnd::Position(VLogPos {
                file_id: *file_id,
                offset: *offset,
            })
        })
}

fn open_vlog_writer(
    root: &Path,
    format: &FormatMetadataV0,
    frontier: DurableFrontier,
    io: Option<Arc<dyn WriterIo>>,
) -> TestResult<ValueLogWriter> {
    let directory = Arc::new(VLogDirectory::open(&root.join("vlog"))?);
    let catalog = Arc::new(FileCatalog::new());
    for (file_id, _) in vlog_inventory(root)? {
        let file = directory.open_read_only(file_id)?;
        catalog.register(file_id, &file)?;
    }
    let accepted_end = descriptor_end(frontier.durable_vlog_end);
    Ok(match io {
        Some(io) => ValueLogWriter::open_with_io(
            directory,
            format.database_uuid,
            VLogGeometry::PRODUCTION,
            catalog,
            accepted_end,
            io,
        )?,
        None => ValueLogWriter::open(
            directory,
            format.database_uuid,
            VLogGeometry::PRODUCTION,
            catalog,
            accepted_end,
        )?,
    })
}

fn stop_process(case_name: &str, before_delete_vlog: &str, root: &Path) -> ! {
    let after = encode_inventory(&vlog_inventory(root).expect("VLog inventory at crash point"));
    println!("{POINT_PREFIX} case={case_name};before={before_delete_vlog};after={after}");
    io::stdout().flush().expect("flush crash-point evidence");
    // SAFETY: getpid returns this process, and SIGSTOP only suspends it until
    // the parent sends SIGKILL. Neither call dereferences Rust memory.
    let stopped = unsafe { kill(getpid(), SIGSTOP) } == 0;
    if !stopped {
        std::process::exit(98);
    }
    std::process::exit(99)
}

fn child_case(name: &str) -> CrashCase {
    *CRASH_CASES
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("unknown child crash case {name}"))
}

#[test]
fn index_only_delete_process_child() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_none() {
        return Ok(());
    }
    let root_path = PathBuf::from(
        std::env::var_os(CHILD_PATH_ENV).expect("child database path must be provided"),
    );
    let case =
        child_case(&std::env::var(CHILD_CASE_ENV).expect("child crash case must be provided"));
    let _root = RootLock::acquire(&root_path, false)?.expect("child exclusive root lock");
    let format = read_format(&root_path)?;
    let inner = Arc::new(FjallBackend::open_existing(
        &root_path.join("index"),
        fjall_options(),
    )?);
    let head = decode_head_seq(
        &inner
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("head metadata"),
    )?;
    let frontier = DurableFrontier::decode(
        &inner
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("durable frontier metadata"),
    )?;
    assert_eq!((head, frontier.durable_seq), (1, 1));
    let crash_evidence = Arc::new(Mutex::new(String::new()));
    let writer_io = case.stop_before_sync.then(|| {
        Arc::new(StopBeforeVLogSyncIo {
            root: root_path.clone(),
            case_name: case.name,
            before_delete_vlog: Arc::clone(&crash_evidence),
        }) as Arc<dyn WriterIo>
    });
    let writer = open_vlog_writer(&root_path, &format, frontier, writer_io)?;
    let stats = Arc::new(StatsState::new());
    let runtime = RuntimeControl::new(Arc::clone(&stats));

    let initial_vlog = encode_inventory(&vlog_inventory(&root_path)?);
    let backend = Arc::new(CrashPointBackend {
        inner,
        target_mode: if case.sync {
            IndexCommitMode::SyncAll
        } else {
            IndexCommitMode::Buffer
        },
        action: case.action,
        root: root_path.clone(),
        case_name: case.name,
        before_delete_vlog: Arc::clone(&crash_evidence),
    });
    let coordinator = CommitCoordinator::new(
        runtime,
        stats,
        Arc::clone(&backend),
        writer,
        FixedUuid(0xa0),
        head,
        frontier,
        frontier.durable_vlog_seq,
        descriptor_end(frontier.durable_vlog_end),
    )?;

    if case.dirty_put {
        coordinator.commit_nonempty(&preflight_put(DIRTY_KEY, DIRTY_VALUE, false)?)?;
    }
    let before_delete_vlog = encode_inventory(&vlog_inventory(&root_path)?);
    assert_eq!(case.dirty_put, before_delete_vlog != initial_vlog);
    *crash_evidence
        .lock()
        .expect("crash evidence mutex poisoned") = before_delete_vlog.clone();

    let mut delete_batch = WriteBatch::new();
    delete_batch.delete(TARGET_KEY)?;
    delete_batch.delete(MISSING_KEY)?;
    delete_batch.delete(TARGET_KEY)?;
    let result = coordinator.commit_nonempty(&preflight_batch(&delete_batch, case.sync)?);
    match case.action {
        FaultAction::ReturnNotApplied => {
            let error = result.expect_err("not-applied fault must fail the write");
            assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        }
        FaultAction::ReturnUnknownAfterApply => {
            let error = result.expect_err("unknown fault must fail the write");
            assert_eq!(error.write_outcome, Some(WriteOutcome::CommitUnknown));
        }
        FaultAction::Success => result?,
        FaultAction::StopBeforeFjall => unreachable!("backend stops before returning"),
    }
    stop_process(case.name, &before_delete_vlog, &root_path)
}

#[derive(Debug)]
struct DiskState {
    head: u64,
    frontier: DurableFrontier,
    target: Option<Vec<u8>>,
    missing: Option<Vec<u8>>,
    dirty: Option<Vec<u8>>,
    target_descriptor: Option<TransactionDescriptor>,
}

fn open_reader(
    root: &Path,
    format: &FormatMetadataV0,
) -> TestResult<(Arc<FileSet>, ValueLogReader)> {
    let directory = Arc::new(VLogDirectory::open(&root.join("vlog"))?);
    let catalog = Arc::new(FileCatalog::new());
    for (file_id, _) in vlog_inventory(root)? {
        let file = directory.open_read_only(file_id)?;
        catalog.register(file_id, &file)?;
    }
    let files = Arc::new(FileSet::new(
        directory,
        format.database_uuid,
        VLogGeometry::PRODUCTION,
        catalog,
        2,
    )?);
    let reader = ValueLogReader::new(Arc::clone(&files), VLogGeometry::PRODUCTION)?;
    Ok((files, reader))
}

fn read_real_value(
    backend: &FjallBackend,
    reader: &ValueLogReader,
    key: &[u8],
) -> TestResult<Option<Vec<u8>>> {
    backend
        .get_user(key, None)?
        .map(|pointer| reader.read_value(&pointer, key))
        .transpose()
        .map_err(Into::into)
}

fn read_real_value_at_snapshot(
    backend: &FjallBackend,
    reader: &ValueLogReader,
    key: &[u8],
    snapshot: &<FjallBackend as IndexBackend>::Snapshot,
) -> TestResult<Option<Vec<u8>>> {
    backend
        .get_user(key, Some(snapshot))?
        .map(|pointer| reader.read_value(&pointer, key))
        .transpose()
        .map_err(Into::into)
}

fn wait_for_queued_writes(runtime: &RuntimeControl, expected: usize) -> TestResult {
    let deadline = Instant::now() + POINT_TIMEOUT;
    while runtime.queued_write_count_for_test() != expected {
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for {expected} queued writes; observed={}",
                runtime.queued_write_count_for_test()
            ))
            .into());
        }
        thread::yield_now();
    }
    Ok(())
}

fn assert_commit_batch_shape(
    batch: &IndexAtomicBatch,
    mode: IndexCommitMode,
    expected_mode: IndexCommitMode,
    expected_puts: usize,
    expected_deletes: usize,
    expected_frontier: bool,
) {
    assert_eq!(mode, expected_mode);
    let put_count = batch
        .operations()
        .iter()
        .filter(|mutation| matches!(mutation, IndexMutation::PutUser { .. }))
        .count();
    let delete_count = batch
        .operations()
        .iter()
        .filter(|mutation| matches!(mutation, IndexMutation::DeleteUser { .. }))
        .count();
    let has_frontier = batch.operations().iter().any(|mutation| {
        matches!(
            mutation,
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key,
                ..
            } if key.as_slice() == DURABLE_FRONTIER_KEY
        )
    });
    assert_eq!(put_count, expected_puts);
    assert_eq!(delete_count, expected_deletes);
    assert_eq!(has_frontier, expected_frontier);
}

fn read_descriptor(
    backend: &FjallBackend,
    commit_seq: u64,
) -> TestResult<Option<TransactionDescriptor>> {
    let meta_key = encode_tx_meta_key(commit_seq)?;
    let entries = backend
        .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())?
        .collect::<Result<Vec<_>>>()?;
    let mut meta_value = None;
    let mut mutation_entries = Vec::new();
    for entry in entries {
        if let Ok(seq) = decode_tx_meta_key(&entry.key) {
            if seq == commit_seq {
                meta_value = Some(entry.value);
            }
            continue;
        }
        if let Ok((seq, ordinal)) = decode_tx_mutation_key(&entry.key) {
            if seq == commit_seq {
                mutation_entries.push((ordinal, entry.key, entry.value));
            }
            continue;
        }
        return Err(io::Error::other(format!(
            "malformed transaction key during atomicity inspection: {:?}",
            entry.key
        ))
        .into());
    }
    mutation_entries.sort_by_key(|(ordinal, _, _)| *ordinal);
    let Some(meta_value) = meta_value else {
        if mutation_entries.is_empty() {
            return Ok(None);
        }
        return Err(io::Error::other(format!(
            "orphan TxMutation entries for commit_seq={commit_seq}"
        ))
        .into());
    };
    let borrowed = mutation_entries
        .iter()
        .map(|(_, key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    Ok(Some(decode_descriptor(&meta_key, &meta_value, &borrowed)?))
}

#[test]
fn atomicity_inspector_rejects_orphan_mutations_and_malformed_transaction_keys() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }
    let temporary = TempDir::new()?;

    let orphan_root = temporary.path().join("orphan-mutation");
    drop(rustkv::Db::open(&public_create_options(), &orphan_root)?);
    let orphan_lock = RootLock::acquire(&orphan_root, false)?.expect("orphan root lock");
    let orphan_backend = FjallBackend::open_existing_for_open_preparation(
        &orphan_root.join("index"),
        fjall_options(),
    )?;
    let mut mutation_key = [0_u8; 19];
    mutation_key[0..2].copy_from_slice(b"TX");
    mutation_key[2..10].copy_from_slice(&1_u64.to_be_bytes());
    mutation_key[10] = 1;
    orphan_backend.insert_for_test_sync_all(
        Some(InternalIndexSpace::Transaction),
        &mutation_key,
        b"orphan",
    )?;
    let orphan = read_descriptor(&orphan_backend, 1)
        .expect_err("orphan TxMutation must fail atomicity inspection");
    assert!(orphan.to_string().contains("orphan TxMutation"));
    drop(orphan_backend);
    drop(orphan_lock);

    let malformed_root = temporary.path().join("malformed-key");
    drop(rustkv::Db::open(&public_create_options(), &malformed_root)?);
    let _malformed_lock = RootLock::acquire(&malformed_root, false)?.expect("malformed root lock");
    let malformed_backend = FjallBackend::open_existing_for_open_preparation(
        &malformed_root.join("index"),
        fjall_options(),
    )?;
    malformed_backend.insert_for_test_sync_all(
        Some(InternalIndexSpace::Transaction),
        b"malformed-transaction-key",
        b"value",
    )?;
    let malformed = read_descriptor(&malformed_backend, 1)
        .expect_err("malformed transaction key must fail atomicity inspection");
    assert!(malformed.to_string().contains("malformed transaction key"));
    Ok(())
}

#[test]
fn pure_delete_batch_is_atomically_visible_through_real_fjall_snapshots() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }
    const KEY_COUNT: usize = 32;
    const VALUE: &[u8] = b"value-read-through-the-real-vlog-reader";

    let temporary = TempDir::new()?;
    let root_path = temporary.path().join("atomic-delete-visibility");
    let keys = (0..KEY_COUNT)
        .map(|index| format!("present-{index:04}").into_bytes())
        .collect::<Vec<_>>();
    {
        let db = rustkv::Db::open(&public_create_options(), &root_path)?;
        let mut seed = rustkv::WriteBatch::new();
        for key in &keys {
            seed.put(key, VALUE)?;
        }
        db.write(&rustkv::WriteOptions { sync: true }, &seed)?;
    }

    let _root = RootLock::acquire(&root_path, false)?.expect("exclusive root lock");
    let format = read_format(&root_path)?;
    let inner = Arc::new(FjallBackend::open_existing(
        &root_path.join("index"),
        fjall_options(),
    )?);
    let head = decode_head_seq(
        &inner
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("head metadata"),
    )?;
    let frontier = DurableFrontier::decode(
        &inner
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("durable frontier metadata"),
    )?;
    assert_eq!(
        (head, frontier.durable_seq, frontier.durable_vlog_seq),
        (1, 1, 1)
    );
    let writer = open_vlog_writer(&root_path, &format, frontier, None)?;
    let stats = Arc::new(StatsState::new());
    let runtime = RuntimeControl::new(Arc::clone(&stats));
    let backend = Arc::new(VisibilityBackend::new(Arc::clone(&inner)));
    let coordinator = Arc::new(CommitCoordinator::new(
        runtime,
        stats,
        Arc::clone(&backend),
        writer,
        FixedUuid(0xb0),
        head,
        frontier,
        frontier.durable_vlog_seq,
        descriptor_end(frontier.durable_vlog_end),
    )?);
    let initial_state = coordinator.state_snapshot();
    let initial_vlog = vlog_inventory(&root_path)?;
    let (_files, reader) = open_reader(&root_path, &format)?;

    let write_coordinator = Arc::clone(&coordinator);
    let delete_keys = keys.clone();
    let write = thread::spawn(move || -> Result<()> {
        let mut batch = WriteBatch::new();
        for key in &delete_keys {
            batch.delete(key)?;
        }
        let validated = preflight_batch(&batch, false)?;
        write_coordinator.commit_nonempty(&validated)
    });

    backend.wait_for_phase(VisibilityPhase::BeforeApply)?;
    let before = inner.snapshot()?;
    for key in &keys {
        assert_eq!(
            read_real_value_at_snapshot(&inner, &reader, key, &before)?.as_deref(),
            Some(VALUE)
        );
    }
    assert_eq!(coordinator.state_snapshot(), initial_state);
    assert_eq!(vlog_inventory(&root_path)?, initial_vlog);

    backend.release_before_apply();
    backend.wait_for_phase(VisibilityPhase::AfterApply)?;
    let after = inner.snapshot()?;
    for key in &keys {
        assert_eq!(
            read_real_value_at_snapshot(&inner, &reader, key, &after)?,
            None
        );
    }
    let descriptor = read_descriptor(&inner, 2)?.expect("Delete descriptor applied with user keys");
    assert_eq!(
        descriptor.meta.transaction_kind,
        TransactionKind::IndexOnlyDelete
    );
    assert_eq!(descriptor.meta.logical_op_count, KEY_COUNT as u64);
    assert_eq!(descriptor.meta.distinct_key_count, KEY_COUNT as u64);
    assert_eq!(descriptor.mutations.len(), KEY_COUNT);
    assert_eq!(coordinator.state_snapshot(), initial_state);
    assert_eq!(vlog_inventory(&root_path)?, initial_vlog);

    backend.release_after_apply();
    write
        .join()
        .map_err(|_| io::Error::other("atomic Delete writer panicked"))??;
    backend.wait_for_phase(VisibilityPhase::Returned)?;
    let committed = coordinator.state_snapshot();
    assert_eq!((committed.head_seq, committed.durable_seq), (2, 1));
    assert_eq!(
        (committed.head_vlog_seq, committed.durable_vlog_seq),
        (1, 1)
    );
    assert_eq!(committed.head_vlog_end, initial_state.head_vlog_end);
    assert_eq!(committed.durable_vlog_end, initial_state.durable_vlog_end);
    assert_eq!(vlog_inventory(&root_path)?, initial_vlog);
    Ok(())
}

#[test]
fn queued_put_delete_and_barriers_advance_exact_logical_and_vlog_prefixes() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }
    let temporary = TempDir::new()?;
    let root_path = temporary.path().join("scripted-prefixes");
    drop(rustkv::Db::open(&public_create_options(), &root_path)?);

    let _root = RootLock::acquire(&root_path, false)?.expect("exclusive root lock");
    let format = read_format(&root_path)?;
    let inner = Arc::new(FjallBackend::open_existing(
        &root_path.join("index"),
        fjall_options(),
    )?);
    let head = decode_head_seq(
        &inner
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("head metadata"),
    )?;
    let frontier = DurableFrontier::decode(
        &inner
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("durable frontier metadata"),
    )?;
    assert_eq!(
        (head, frontier.durable_seq, frontier.durable_vlog_seq),
        (0, 0, 0)
    );
    let writer = open_vlog_writer(&root_path, &format, frontier, None)?;
    let stats = Arc::new(StatsState::new());
    let runtime = RuntimeControl::new(Arc::clone(&stats));
    let backend = Arc::new(ScriptedBackend::new(Arc::clone(&inner)));
    let coordinator = Arc::new(CommitCoordinator::new(
        Arc::clone(&runtime),
        stats,
        Arc::clone(&backend),
        writer,
        FixedUuid(0xc0),
        head,
        frontier,
        frontier.durable_vlog_seq,
        descriptor_end(frontier.durable_vlog_end),
    )?);

    let first_put_coordinator = Arc::clone(&coordinator);
    let first_put = thread::spawn(move || -> Result<()> {
        let write = preflight_put(b"first", b"first-value", false)?;
        first_put_coordinator.commit_nonempty(&write)
    });
    let (first_batch, first_mode) = backend.wait_for_call(1)?;
    assert_commit_batch_shape(
        &first_batch,
        first_mode,
        IndexCommitMode::Buffer,
        1,
        0,
        false,
    );
    let after_first_put_append = vlog_inventory(&root_path)?;
    assert!(!after_first_put_append.is_empty());

    let first_delete_coordinator = Arc::clone(&coordinator);
    let first_delete = thread::spawn(move || -> Result<()> {
        let write = preflight_delete(b"first", false)?;
        first_delete_coordinator.commit_nonempty(&write)
    });
    wait_for_queued_writes(&runtime, 1)?;
    backend.release(1);
    let (first_delete_batch, first_delete_mode) = backend.wait_for_call(2)?;
    assert_commit_batch_shape(
        &first_delete_batch,
        first_delete_mode,
        IndexCommitMode::Buffer,
        0,
        1,
        false,
    );
    first_put
        .join()
        .map_err(|_| io::Error::other("first Put writer panicked"))??;
    let after_first_put = coordinator.state_snapshot();
    assert_eq!(
        (after_first_put.head_seq, after_first_put.durable_seq),
        (1, 0)
    );
    assert_eq!(
        (
            after_first_put.head_vlog_seq,
            after_first_put.durable_vlog_seq
        ),
        (1, 0)
    );
    assert!(after_first_put.head_vlog_end.is_some());
    assert_eq!(after_first_put.durable_vlog_end, None);

    let first_barrier_coordinator = Arc::clone(&coordinator);
    let first_barrier = thread::spawn(move || first_barrier_coordinator.commit_empty_batch(true));
    wait_for_queued_writes(&runtime, 1)?;
    backend.release(2);
    let (frontier_batch, frontier_mode) = backend.wait_for_call(3)?;
    assert_commit_batch_shape(
        &frontier_batch,
        frontier_mode,
        IndexCommitMode::SyncAll,
        0,
        0,
        true,
    );
    first_delete
        .join()
        .map_err(|_| io::Error::other("first Delete writer panicked"))??;
    let before_first_barrier = coordinator.state_snapshot();
    assert_eq!(
        (
            before_first_barrier.head_seq,
            before_first_barrier.durable_seq
        ),
        (2, 0)
    );
    assert_eq!(
        (
            before_first_barrier.head_vlog_seq,
            before_first_barrier.durable_vlog_seq
        ),
        (1, 0)
    );
    assert_eq!(
        before_first_barrier.head_vlog_end,
        after_first_put.head_vlog_end
    );
    assert_eq!(before_first_barrier.durable_vlog_end, None);

    backend.release(3);
    first_barrier
        .join()
        .map_err(|_| io::Error::other("first barrier writer panicked"))??;
    let first_durable_prefix = coordinator.state_snapshot();
    assert_eq!(
        (
            first_durable_prefix.head_seq,
            first_durable_prefix.durable_seq
        ),
        (2, 2)
    );
    assert_eq!(
        (
            first_durable_prefix.head_vlog_seq,
            first_durable_prefix.durable_vlog_seq
        ),
        (1, 1)
    );
    assert_eq!(
        first_durable_prefix.head_vlog_end,
        after_first_put.head_vlog_end
    );
    assert_eq!(
        first_durable_prefix.durable_vlog_end,
        after_first_put.head_vlog_end
    );
    assert_eq!(vlog_inventory(&root_path)?, after_first_put_append);
    let persisted_head = decode_head_seq(
        &inner
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("persisted head after first barrier"),
    )?;
    let persisted_frontier = DurableFrontier::decode(
        &inner
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("persisted frontier after first barrier"),
    )?;
    assert_eq!(persisted_head, 2);
    assert_eq!(persisted_frontier.durable_seq, 2);
    assert_eq!(persisted_frontier.durable_vlog_seq, 1);
    assert_eq!(
        descriptor_end(persisted_frontier.durable_vlog_end),
        after_first_put.head_vlog_end
    );

    let second_put_coordinator = Arc::clone(&coordinator);
    let second_put = thread::spawn(move || -> Result<()> {
        let write = preflight_put(b"second", b"second-value", false)?;
        second_put_coordinator.commit_nonempty(&write)
    });
    let (second_put_batch, second_put_mode) = backend.wait_for_call(4)?;
    assert_commit_batch_shape(
        &second_put_batch,
        second_put_mode,
        IndexCommitMode::Buffer,
        1,
        0,
        false,
    );
    let after_second_put_append = vlog_inventory(&root_path)?;
    assert_ne!(after_second_put_append, after_first_put_append);

    let sync_delete_coordinator = Arc::clone(&coordinator);
    let sync_delete = thread::spawn(move || -> Result<()> {
        let write = preflight_delete(b"second", true)?;
        sync_delete_coordinator.commit_nonempty(&write)
    });
    wait_for_queued_writes(&runtime, 1)?;
    backend.release(4);
    let (sync_delete_batch, sync_delete_mode) = backend.wait_for_call(5)?;
    assert_commit_batch_shape(
        &sync_delete_batch,
        sync_delete_mode,
        IndexCommitMode::SyncAll,
        0,
        1,
        true,
    );
    second_put
        .join()
        .map_err(|_| io::Error::other("second Put writer panicked"))??;
    let before_sync_delete = coordinator.state_snapshot();
    assert_eq!(
        (before_sync_delete.head_seq, before_sync_delete.durable_seq),
        (3, 2)
    );
    assert_eq!(
        (
            before_sync_delete.head_vlog_seq,
            before_sync_delete.durable_vlog_seq
        ),
        (3, 1)
    );
    assert_ne!(
        before_sync_delete.head_vlog_end,
        after_first_put.head_vlog_end
    );
    assert_eq!(
        before_sync_delete.durable_vlog_end,
        after_first_put.head_vlog_end
    );

    backend.release(5);
    sync_delete
        .join()
        .map_err(|_| io::Error::other("sync Delete writer panicked"))??;
    let final_prefix = coordinator.state_snapshot();
    assert_eq!((final_prefix.head_seq, final_prefix.durable_seq), (4, 4));
    assert_eq!(
        (final_prefix.head_vlog_seq, final_prefix.durable_vlog_seq),
        (3, 3)
    );
    assert_eq!(final_prefix.head_vlog_end, before_sync_delete.head_vlog_end);
    assert_eq!(
        final_prefix.durable_vlog_end,
        before_sync_delete.head_vlog_end
    );
    assert_eq!(vlog_inventory(&root_path)?, after_second_put_append);
    let persisted_head = decode_head_seq(
        &inner
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("persisted final head"),
    )?;
    let persisted_frontier = DurableFrontier::decode(
        &inner
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("persisted final frontier"),
    )?;
    assert_eq!(persisted_head, 4);
    assert_eq!(persisted_frontier.durable_seq, 4);
    assert_eq!(persisted_frontier.durable_vlog_seq, 3);
    assert_eq!(
        descriptor_end(persisted_frontier.durable_vlog_end),
        before_sync_delete.head_vlog_end
    );

    let calls_before_noop_barrier = backend.calls().len();
    coordinator.commit_empty_batch(true)?;
    assert_eq!(backend.calls().len(), calls_before_noop_barrier);
    assert_eq!(coordinator.state_snapshot(), final_prefix);

    for (commit_seq, expected_kind) in [
        (1, TransactionKind::VLogEnvelope),
        (2, TransactionKind::IndexOnlyDelete),
        (3, TransactionKind::VLogEnvelope),
        (4, TransactionKind::IndexOnlyDelete),
    ] {
        let descriptor = read_descriptor(&inner, commit_seq)?.expect("committed descriptor");
        assert_eq!(descriptor.meta.transaction_kind, expected_kind);
    }
    let (_files, reader) = open_reader(&root_path, &format)?;
    assert_eq!(read_real_value(&inner, &reader, b"first")?, None);
    assert_eq!(read_real_value(&inner, &reader, b"second")?, None);
    Ok(())
}

fn inspect_disk(root_path: &Path, target_seq: Option<u64>) -> TestResult<DiskState> {
    let root = RootLock::acquire(root_path, false)?.expect("inspection root lock");
    let format = read_format(root_path)?;
    let backend = FjallBackend::open_existing_for_open_preparation(
        &root_path.join("index"),
        fjall_options(),
    )?;
    let head = decode_head_seq(
        &backend
            .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)?
            .expect("head metadata"),
    )?;
    let frontier = DurableFrontier::decode(
        &backend
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("durable frontier metadata"),
    )?;
    let (_files, reader) = open_reader(root_path, &format)?;
    let state = DiskState {
        head,
        frontier,
        target: read_real_value(&backend, &reader, TARGET_KEY)?,
        missing: read_real_value(&backend, &reader, MISSING_KEY)?,
        dirty: read_real_value(&backend, &reader, DIRTY_KEY)?,
        target_descriptor: target_seq
            .map(|commit_seq| read_descriptor(&backend, commit_seq))
            .transpose()?
            .flatten(),
    };
    drop(backend);
    drop(root);
    Ok(state)
}

fn assert_index_only_descriptor(descriptor: &TransactionDescriptor, commit_seq: u64) -> TestResult {
    assert_eq!(
        descriptor.meta.transaction_kind,
        TransactionKind::IndexOnlyDelete
    );
    assert_eq!(descriptor.meta.commit_seq, commit_seq);
    assert_eq!(descriptor.meta.prev_seq, commit_seq - 1);
    assert_eq!(descriptor.meta.logical_op_count, 3);
    assert_eq!(descriptor.meta.distinct_key_count, 2);
    assert_eq!(
        descriptor.meta.vlog_begin,
        VLogPos {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(
        descriptor.meta.vlog_end,
        VLogPos {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(descriptor.meta.envelope_crc32c, 0);
    assert_eq!(descriptor.mutations.len(), 2);
    assert_eq!(descriptor.mutations[0].user_key, TARGET_KEY);
    assert!(matches!(
        descriptor.mutations[0].before_state,
        ValueState::Present(_)
    ));
    assert_eq!(descriptor.mutations[0].after_state, ValueState::Absent);
    assert_eq!(descriptor.mutations[1].user_key, MISSING_KEY);
    assert_eq!(descriptor.mutations[1].before_state, ValueState::Absent);
    assert_eq!(descriptor.mutations[1].after_state, ValueState::Absent);
    Ok(())
}

fn create_seeded_database(root: &Path) -> TestResult<Vec<(u32, u64)>> {
    let db = rustkv::Db::open(&public_create_options(), root)?;
    db.put(
        &rustkv::WriteOptions { sync: true },
        TARGET_KEY,
        TARGET_VALUE,
    )?;
    drop(db);
    vlog_inventory(root)
}

fn spawn_child(case: CrashCase, root: &Path) -> TestResult<Child> {
    Ok(Command::new(std::env::current_exe()?)
        .args(["--exact", "index_only_delete_process_child", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(CHILD_PATH_ENV, root)
        .env(CHILD_CASE_ENV, case.name)
        .stdout(Stdio::piped())
        .spawn()?)
}

fn kill_at_crash_point(case: CrashCase, root: &Path) -> TestResult<(String, String)> {
    let mut child = spawn_child(case, root)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("child stdout missing"))?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout)
            .lines()
            .map_while(std::result::Result::ok)
        {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + POINT_TIMEOUT;
    let evidence = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::other(format!("timeout waiting for {}", case.name)).into());
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) if line.starts_with(POINT_PREFIX) => break line,
            Ok(_) => {}
            Err(error) => {
                let status = child.wait()?;
                return Err(io::Error::other(format!(
                    "child {} exited as {status} before crash point: {error}",
                    case.name
                ))
                .into());
            }
        }
    };
    child.kill()?;
    let status = child.wait()?;
    assert_eq!(status.signal(), Some(SIGKILL), "case={}", case.name);

    let fields = evidence
        .split_once(";before=")
        .and_then(|(_, rest)| rest.split_once(";after="))
        .ok_or_else(|| io::Error::other(format!("malformed crash evidence: {evidence}")))?;
    Ok((fields.0.to_owned(), fields.1.to_owned()))
}

fn expected_before_recovery(case: CrashCase) -> (u64, u64, u64, bool) {
    let applied = matches!(
        case.action,
        FaultAction::ReturnUnknownAfterApply | FaultAction::Success
    ) && !case.stop_before_sync;
    if case.dirty_put {
        if applied {
            (3, 3, 2, false)
        } else {
            (2, 1, 1, true)
        }
    } else if applied && case.sync {
        (2, 2, 1, false)
    } else if applied {
        (2, 1, 1, false)
    } else {
        (1, 1, 1, true)
    }
}

fn expected_after_recovery(case: CrashCase) -> (u64, u64, u64, bool) {
    let (head, _, _, target_present) = expected_before_recovery(case);
    let vlog_seq = if case.dirty_put { 2 } else { 1 };
    (head, head, vlog_seq, target_present)
}

fn assert_state(state: &DiskState, expected: (u64, u64, u64, bool), case: CrashCase) -> TestResult {
    let (head, durable, vlog_seq, target_present) = expected;
    assert_eq!(state.head, head, "case={}", case.name);
    assert_eq!(state.frontier.durable_seq, durable, "case={}", case.name);
    assert_eq!(
        state.frontier.durable_vlog_seq, vlog_seq,
        "case={}",
        case.name
    );
    assert_eq!(
        state.target.as_deref(),
        target_present.then_some(TARGET_VALUE),
        "case={}",
        case.name
    );
    assert_eq!(state.missing, None, "case={}", case.name);
    assert_eq!(
        state.dirty.as_deref(),
        case.dirty_put.then_some(DIRTY_VALUE),
        "case={}",
        case.name
    );
    Ok(())
}

#[test]
fn sigkill_matrix_preserves_atomic_index_only_transactions_and_reopen_converges() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }
    for case in CRASH_CASES.iter().copied() {
        let temporary = TempDir::new()?;
        let root = temporary.path().join(case.name);
        let initial_vlog = create_seeded_database(&root)?;
        let initial_vlog_encoded = encode_inventory(&initial_vlog);
        let (before_delete, at_crash) = kill_at_crash_point(case, &root)?;
        assert_eq!(before_delete, at_crash, "Delete grew VLog in {}", case.name);
        if case.dirty_put {
            assert_ne!(before_delete, initial_vlog_encoded, "case={}", case.name);
        } else {
            assert_eq!(before_delete, initial_vlog_encoded, "case={}", case.name);
        }
        let crash_vlog = vlog_inventory(&root)?;
        assert_eq!(
            encode_inventory(&crash_vlog),
            at_crash,
            "parent/child physical VLog observation differs in {}",
            case.name
        );

        let target_seq = if case.dirty_put { 3 } else { 2 };
        let before = inspect_disk(&root, Some(target_seq))?;
        assert_state(&before, expected_before_recovery(case), case)?;
        let before_end = if case.dirty_put && before.frontier.durable_vlog_seq == 2 {
            inventory_end(&crash_vlog)
        } else {
            inventory_end(&initial_vlog)
        };
        assert_eq!(
            before.frontier.durable_vlog_end, before_end,
            "case={}",
            case.name
        );
        let transaction_applied = before.head == target_seq;
        assert_eq!(
            before.target_descriptor.is_some(),
            transaction_applied,
            "descriptor/head atomicity in {}",
            case.name
        );
        if let Some(descriptor) = &before.target_descriptor {
            assert_index_only_descriptor(descriptor, target_seq)?;
        }

        for reopen in 0..2 {
            let db = rustkv::Db::open(&rustkv::Options::default(), &root)?;
            assert_eq!(
                db.get(&rustkv::ReadOptions::default(), TARGET_KEY)?
                    .as_deref(),
                expected_after_recovery(case).3.then_some(TARGET_VALUE),
                "case={} reopen={reopen}",
                case.name
            );
            assert_eq!(
                db.get(&rustkv::ReadOptions::default(), MISSING_KEY)?,
                None,
                "case={} reopen={reopen}",
                case.name
            );
            assert_eq!(
                db.get(&rustkv::ReadOptions::default(), DIRTY_KEY)?
                    .as_deref(),
                case.dirty_put.then_some(DIRTY_VALUE),
                "case={} reopen={reopen}",
                case.name
            );
            let expected = expected_after_recovery(case);
            let stats = db.stats();
            assert_eq!(
                (stats.head_seq, stats.durable_seq),
                (expected.0, expected.1)
            );
            drop(db);
        }
        let recovered = inspect_disk(&root, None)?;
        assert_state(&recovered, expected_after_recovery(case), case)?;
        assert_eq!(
            recovered.frontier.durable_vlog_end,
            inventory_end(if case.dirty_put {
                &crash_vlog
            } else {
                &initial_vlog
            }),
            "case={}",
            case.name
        );
    }
    Ok(())
}

#[test]
fn real_write_path_preserves_empty_position_and_prefix_frontiers() -> TestResult {
    if std::env::var_os(CHILD_ENV).is_some() {
        return Ok(());
    }

    let temporary = TempDir::new()?;
    let fresh_root = temporary.path().join("fresh");
    let fresh = rustkv::Db::open(&public_create_options(), &fresh_root)?;
    fresh.delete(&rustkv::WriteOptions::default(), b"single-missing")?;
    let mut repeated = rustkv::WriteBatch::new();
    repeated.delete(b"batch-missing-a")?;
    repeated.delete(b"batch-missing-b")?;
    repeated.delete(b"batch-missing-a")?;
    fresh.write(&rustkv::WriteOptions::default(), &repeated)?;
    fresh.write(
        &rustkv::WriteOptions { sync: true },
        &rustkv::WriteBatch::new(),
    )?;
    let stats = fresh.stats();
    assert_eq!((stats.head_seq, stats.durable_seq), (2, 2));
    assert!(stats.durable_vlog_end.is_none());
    drop(fresh);
    assert!(vlog_inventory(&fresh_root)?.is_empty());
    let state = inspect_disk(&fresh_root, None)?;
    assert_eq!((state.head, state.frontier.durable_seq), (2, 2));
    assert_eq!(state.frontier.durable_vlog_seq, 0);
    assert_eq!(state.frontier.durable_vlog_end, DurableVLogEnd::Empty);

    let trailing_root = temporary.path().join("trailing");
    let trailing = rustkv::Db::open(&public_create_options(), &trailing_root)?;
    trailing.put(
        &rustkv::WriteOptions { sync: true },
        TARGET_KEY,
        TARGET_VALUE,
    )?;
    let stable_stats = trailing.stats();
    let stable_vlog = vlog_inventory(&trailing_root)?;
    trailing.delete(&rustkv::WriteOptions::default(), b"missing-one")?;
    trailing.delete(&rustkv::WriteOptions::default(), TARGET_KEY)?;
    let mut deletes = rustkv::WriteBatch::new();
    deletes.delete(b"missing-two")?;
    deletes.delete(b"missing-three")?;
    trailing.write(&rustkv::WriteOptions::default(), &deletes)?;
    trailing.write(
        &rustkv::WriteOptions { sync: true },
        &rustkv::WriteBatch::new(),
    )?;
    let trailing_stats = trailing.stats();
    assert_eq!(
        (trailing_stats.head_seq, trailing_stats.durable_seq),
        (4, 4)
    );
    assert_eq!(
        trailing_stats
            .durable_vlog_end
            .as_ref()
            .map(|end| (end.file_id, end.offset)),
        stable_stats
            .durable_vlog_end
            .as_ref()
            .map(|end| (end.file_id, end.offset))
    );
    assert_eq!(vlog_inventory(&trailing_root)?, stable_vlog);
    assert_eq!(
        trailing.get(&rustkv::ReadOptions::default(), TARGET_KEY)?,
        None
    );
    drop(trailing);
    let trailing_state = inspect_disk(&trailing_root, None)?;
    assert_eq!(trailing_state.frontier.durable_vlog_seq, 1);

    let alternating_root = temporary.path().join("alternating");
    let alternating = rustkv::Db::open(&public_create_options(), &alternating_root)?;
    alternating.put(&rustkv::WriteOptions::default(), b"a", b"one")?;
    alternating.delete(&rustkv::WriteOptions::default(), b"a")?;
    alternating.put(&rustkv::WriteOptions::default(), b"b", b"two")?;
    alternating.delete(&rustkv::WriteOptions { sync: true }, b"missing")?;
    let first_barrier = alternating.stats();
    assert_eq!((first_barrier.head_seq, first_barrier.durable_seq), (4, 4));
    alternating.write(
        &rustkv::WriteOptions { sync: true },
        &rustkv::WriteBatch::new(),
    )?;
    assert_eq!(alternating.stats().head_seq, 4);
    alternating.put(&rustkv::WriteOptions::default(), b"c", b"three")?;
    alternating.write(
        &rustkv::WriteOptions { sync: true },
        &rustkv::WriteBatch::new(),
    )?;
    let final_stats = alternating.stats();
    assert_eq!((final_stats.head_seq, final_stats.durable_seq), (5, 5));
    assert_eq!(
        alternating.get(&rustkv::ReadOptions::default(), b"a")?,
        None
    );
    assert_eq!(
        alternating.get(&rustkv::ReadOptions::default(), b"b")?,
        Some(b"two".to_vec())
    );
    assert_eq!(
        alternating.get(&rustkv::ReadOptions::default(), b"c")?,
        Some(b"three".to_vec())
    );
    drop(alternating);
    let alternating_state = inspect_disk(&alternating_root, None)?;
    assert_eq!(alternating_state.frontier.durable_vlog_seq, 5);
    Ok(())
}

// OPT-4 provides L3 process-crash evidence. Loss of OS page cache and actual
// power-failure behavior are L4 and remain explicitly deferred by the stage.
