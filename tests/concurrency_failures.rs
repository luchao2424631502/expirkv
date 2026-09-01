#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};

#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/runtime/mod.rs"]
mod runtime;

#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use commit::{CommitCoordinator, DurableFrontier, DurableVLogEnd, TxUuidSource, preflight_put};
use index::{
    FjallBackend, FjallIndexOptions, IndexAtomicBatch, IndexBackend, IndexCommitError,
    IndexCommitMode, IndexCompression, IndexEntry, InternalIndexSpace, InternalKeyRange,
    initialization_batch,
};
use runtime::{LifecycleController, RuntimeControl};
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, HandleOpener, VLogDirectory};
use vlog::format::{PageHeader, VLogFileHeader, VLogGeometry};
use vlog::reader::ValueLogReader;
use vlog::writer::ValueLogWriter;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type WorkerResult = std::result::Result<(), String>;

const TIMEOUT: Duration = Duration::from_secs(15);

fn runtime() -> Arc<RuntimeControl> {
    RuntimeControl::new(Arc::new(StatsState::new()))
}

fn failure(
    kind: StorageErrorKind,
    operation: Operation,
    stage: ProtocolStage,
    outcome: Option<WriteOutcome>,
    retry: RetryAdvice,
) -> StorageError {
    let mut error = StorageError::codec_error(kind, operation, stage, outcome, retry);
    error.os_code = Some(5);
    error.commit_seq = Some(17);
    error.vlog_file_id = Some(2);
    error.vlog_offset = Some(4096);
    error
}

fn write_stopped_failure() -> StorageError {
    failure(
        StorageErrorKind::StorageWriteStopped,
        Operation::Background,
        ProtocolStage::Maintenance,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    )
}

fn poisoned_failure() -> StorageError {
    failure(
        StorageErrorKind::Corruption,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn receive_workers(
    stage: &str,
    expected: usize,
    receiver: &mpsc::Receiver<WorkerResult>,
) -> TestResult {
    let deadline = Instant::now() + TIMEOUT;
    for completed in 0..expected {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = receiver.recv_timeout(remaining).map_err(|error| {
            format!(
                "stage={stage} completed={completed}/{expected} timed out or disconnected: {error}"
            )
        })?;
        result.map_err(|error| format!("stage={stage} worker failed: {error}"))?;
    }
    Ok(())
}

#[derive(Default)]
struct CommitBlockState {
    enabled: bool,
    entered: bool,
    released: bool,
}

struct BlockingFjallBackend {
    inner: FjallBackend,
    block: Mutex<CommitBlockState>,
    changed: Condvar,
}

impl BlockingFjallBackend {
    fn new(inner: FjallBackend) -> Self {
        Self {
            inner,
            block: Mutex::new(CommitBlockState::default()),
            changed: Condvar::new(),
        }
    }

    fn enable_next_commit_block(&self) {
        *self.block.lock().expect("commit-block mutex poisoned") = CommitBlockState {
            enabled: true,
            entered: false,
            released: false,
        };
    }

    fn wait_until_commit_entered(&self) -> TestResult {
        let deadline = Instant::now() + TIMEOUT;
        let mut state = self
            .block
            .lock()
            .map_err(|_| "commit-block mutex poisoned")?;
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("real Fjall commit entry timed out".into());
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "commit-block mutex poisoned while waiting")?;
            state = next;
            if wait.timed_out() && !state.entered {
                return Err("real Fjall commit entry timed out".into());
            }
        }
        Ok(())
    }

    fn release_commit(&self) {
        let mut state = self.block.lock().expect("commit-block mutex poisoned");
        state.released = true;
        drop(state);
        self.changed.notify_all();
    }
}

impl IndexBackend for BlockingFjallBackend {
    type Snapshot = <FjallBackend as IndexBackend>::Snapshot;
    type UserIterator = <FjallBackend as IndexBackend>::UserIterator;
    type InternalIterator = <FjallBackend as IndexBackend>::InternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        let mut state = self.block.lock().expect("commit-block mutex poisoned");
        if state.enabled {
            state.entered = true;
            self.changed.notify_all();
            let deadline = Instant::now() + TIMEOUT;
            while !state.released {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(IndexCommitError::not_applied(
                        index::InternalIndexError::new(StorageErrorKind::ResourceExhausted, None),
                    ));
                }
                let (next, wait) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .expect("commit-block mutex poisoned while waiting");
                state = next;
                if wait.timed_out() && !state.released {
                    return Err(IndexCommitError::not_applied(
                        index::InternalIndexError::new(StorageErrorKind::ResourceExhausted, None),
                    ));
                }
            }
            state.enabled = false;
        }
        drop(state);
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

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

struct RealFailureHarness {
    _temporary: TempDir,
    vlog_path: PathBuf,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
    backend: Arc<BlockingFjallBackend>,
    runtime: Arc<RuntimeControl>,
    coordinator: Arc<CommitCoordinator<BlockingFjallBackend, FixedUuid>>,
}

impl RealFailureHarness {
    fn new() -> TestResult<Self> {
        let temporary = TempDir::new()?;
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let fjall = FjallBackend::create(&index_path, fjall_options())?;
        fjall
            .commit_atomic(
                initialization_batch(0, database_uuid())
                    .map_err(|error| io::Error::other(format!("initial batch: {error:?}")))?,
                IndexCommitMode::SyncAll,
            )
            .map_err(|error| io::Error::other(format!("initial commit: {error:?}")))?;
        let backend = Arc::new(BlockingFjallBackend::new(fjall));
        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        let writer = ValueLogWriter::empty(
            Arc::clone(&directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&catalog),
        )?;
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let coordinator = Arc::new(CommitCoordinator::new(
            Arc::clone(&runtime),
            stats,
            Arc::clone(&backend),
            writer,
            FixedUuid(0x51),
            0,
            DurableFrontier {
                durable_seq: 0,
                durable_vlog_end: DurableVLogEnd::Empty,
            },
            None,
        )?);
        Ok(Self {
            _temporary: temporary,
            vlog_path,
            directory,
            catalog,
            backend,
            runtime,
            coordinator,
        })
    }

    fn reader(&self) -> TestResult<ValueLogReader> {
        let files = Arc::new(FileSet::new(
            Arc::clone(&self.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&self.catalog),
            2,
        )?);
        Ok(ValueLogReader::new(files, VLogGeometry::PRODUCTION)?)
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

fn total_vlog_bytes(path: &Path) -> io::Result<u64> {
    std::fs::read_dir(path)?.try_fold(0_u64, |total, entry| {
        let metadata = entry?.metadata()?;
        Ok(total + metadata.len())
    })
}

#[test]
fn real_started_commit_crosses_failure_without_queued_or_later_physical_side_effects() -> TestResult
{
    const FOLLOWERS: usize = 12;
    const ACTIVE_KEY: &[u8] = b"active-real-commit";
    const ACTIVE_VALUE: &[u8] = b"real-vlog-and-real-fjall";

    let harness = RealFailureHarness::new()?;
    harness.backend.enable_next_commit_block();
    let active_coordinator = Arc::clone(&harness.coordinator);
    let (active_sender, active_receiver) = mpsc::channel();
    let active = thread::spawn(move || {
        let result = preflight_put(ACTIVE_KEY, ACTIVE_VALUE, false)
            .and_then(|write| active_coordinator.commit_nonempty(&write));
        let _ = active_sender.send(result);
    });
    harness.backend.wait_until_commit_entered()?;

    // Reaching the backend means append() has completed, but the real Fjall
    // batch is deliberately still outside commit_atomic().
    let bytes_after_active_append = total_vlog_bytes(&harness.vlog_path)?;
    assert!(bytes_after_active_append > 0);
    assert_eq!(harness.coordinator.state_snapshot().head_seq, 0);
    assert_eq!(harness.backend.get_user(ACTIVE_KEY, None)?, None);

    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();
    for follower_id in 0..FOLLOWERS {
        let coordinator = Arc::clone(&harness.coordinator);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let key = format!("queued-{follower_id:02}");
            let result = match preflight_put(key.as_bytes(), b"must-not-commit", false)
                .and_then(|write| coordinator.commit_nonempty(&write))
            {
                Err(error)
                    if error.kind == StorageErrorKind::StorageWriteStopped
                        && error.protocol_stage == ProtocolStage::Admission
                        && error.write_outcome == Some(WriteOutcome::NotCommitted)
                        && error.instance_state == Some(InstanceState::WriteStopped) =>
                {
                    Ok(())
                }
                Err(error) => Err(format!("unexpected queued cancellation: {error:?}")),
                Ok(()) => Err("queued write crossed the failed active request".to_owned()),
            };
            let _ = sender.send(result);
        }));
    }
    let queue_deadline = Instant::now() + TIMEOUT;
    while harness.runtime.queued_write_count_for_test() != FOLLOWERS {
        if Instant::now() >= queue_deadline {
            return Err(format!(
                "queued writes did not reach {FOLLOWERS}; observed {}",
                harness.runtime.queued_write_count_for_test()
            )
            .into());
        }
        thread::yield_now();
    }

    let transition = harness
        .runtime
        .latch_failure(InstanceState::WriteStopped, &write_stopped_failure());
    assert!(transition.changed);
    assert_eq!(transition.cancelled_writes, FOLLOWERS);
    assert_eq!(
        transition.current.instance_state,
        InstanceState::WriteStopped
    );
    drop(sender);

    receive_workers(
        "real-active-background-failure-queued",
        FOLLOWERS,
        &receiver,
    )?;
    for handle in handles {
        handle.join().map_err(|_| "failure-race worker panicked")?;
    }
    assert_eq!(
        total_vlog_bytes(&harness.vlog_path)?,
        bytes_after_active_append
    );
    assert_eq!(harness.coordinator.state_snapshot().head_seq, 0);
    for follower_id in 0..FOLLOWERS {
        assert_eq!(
            harness
                .backend
                .get_user(format!("queued-{follower_id:02}").as_bytes(), None)?,
            None
        );
    }

    let later_write = preflight_put(b"later", b"must-not-append", false)?;
    let later = match harness.coordinator.commit_nonempty(&later_write) {
        Ok(()) => return Err("post-failure write was admitted".into()),
        Err(error) => error,
    };
    assert_eq!(later.kind, StorageErrorKind::StorageWriteStopped);
    assert_eq!(later.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(
        total_vlog_bytes(&harness.vlog_path)?,
        bytes_after_active_append
    );
    assert_eq!(harness.backend.get_user(b"later", None)?, None);

    harness.backend.release_commit();
    active_receiver
        .recv_timeout(TIMEOUT)
        .map_err(|error| format!("active real commit timed out: {error}"))??;
    active.join().map_err(|_| "active real commit panicked")?;

    assert!(harness.runtime.active_request_for_test().is_none());
    assert_eq!(harness.runtime.queued_write_count_for_test(), 0);
    assert_eq!(harness.coordinator.state_snapshot().head_seq, 1);
    let pointer = harness
        .backend
        .get_user(ACTIVE_KEY, None)?
        .expect("active request must reach the real Fjall commit endpoint");
    assert_eq!(
        harness.reader()?.read_value(&pointer, ACTIVE_KEY)?,
        ACTIVE_VALUE
    );
    Ok(())
}

fn state_rank(state: InstanceState) -> u8 {
    match state {
        InstanceState::Healthy => 0,
        InstanceState::WriteStopped => 1,
        InstanceState::Poisoned => 2,
    }
}

#[test]
fn stats_snapshots_stay_self_consistent_during_concurrent_updates_and_state_upgrades() -> TestResult
{
    const READERS: usize = 6;
    const COMMIT_UPDATES: u64 = 2_000;
    const HEALTHY_UPDATES_END: u64 = 700;
    const WRITE_STOPPED_UPDATES_END: u64 = 1_400;

    let stats = Arc::new(StatsState::new());
    let runtime = RuntimeControl::new(Arc::clone(&stats));
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let (reader_ready_sender, reader_ready_receiver) = mpsc::channel();
    let (stopped_seen_sender, stopped_seen_receiver) = mpsc::channel();
    let (stats_phase_sender, stats_phase_receiver) = mpsc::channel();
    let (stats_continue_sender, stats_continue_receiver) = mpsc::channel();
    let mut handles = Vec::new();
    let mut starts = Vec::new();

    for reader_id in 0..READERS {
        let (start_sender, start_receiver) = mpsc::channel();
        starts.push(start_sender);
        let runtime = Arc::clone(&runtime);
        let finished = Arc::clone(&finished);
        let reader_ready_sender = reader_ready_sender.clone();
        let stopped_seen_sender = stopped_seen_sender.clone();
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("reader {reader_id} start timeout: {error}"))?;
                let initial = runtime.stats();
                if initial.instance_state != InstanceState::Healthy || initial.state_epoch != 0 {
                    return Err(format!(
                        "reader {reader_id} missed the initial healthy state"
                    ));
                }
                reader_ready_sender
                    .send(())
                    .map_err(|error| format!("reader {reader_id} ready report failed: {error}"))?;
                let mut previous_epoch = 0;
                let mut previous_rank = 0;
                let mut first_error = None;
                let mut reported_write_stopped = false;
                let mut reads = 0;
                let deadline = Instant::now() + TIMEOUT;
                while reads < 500 || !finished.load(std::sync::atomic::Ordering::Acquire) {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "reader {reader_id} timed out after {reads} stats snapshots"
                        ));
                    }
                    let snapshot = runtime.stats();
                    if snapshot.state_epoch < previous_epoch
                        || state_rank(snapshot.instance_state) < previous_rank
                    {
                        return Err(format!(
                            "reader {reader_id} observed state regression epoch={} state={:?}",
                            snapshot.state_epoch, snapshot.instance_state
                        ));
                    }
                    if snapshot.head_seq < snapshot.durable_seq
                        || snapshot.durability_lag != snapshot.head_seq - snapshot.durable_seq
                    {
                        return Err(format!(
                            "reader {reader_id} observed torn commit stats H={} D={} lag={}",
                            snapshot.head_seq, snapshot.durable_seq, snapshot.durability_lag
                        ));
                    }
                    match snapshot.first_latched_error.as_ref() {
                        None if snapshot.instance_state != InstanceState::Healthy => {
                            return Err(format!("reader {reader_id} observed missing first error"));
                        }
                        Some(error) => {
                            let identity = (error.kind, error.operation, error.protocol_stage);
                            if let Some(previous) = first_error
                                && previous != identity
                            {
                                return Err(format!(
                                    "reader {reader_id} observed first-error replacement"
                                ));
                            }
                            first_error = Some(identity);
                        }
                        None => {}
                    }
                    if snapshot.instance_state == InstanceState::WriteStopped
                        && !reported_write_stopped
                    {
                        stopped_seen_sender.send(()).map_err(|error| {
                            format!("reader {reader_id} write-stop report failed: {error}")
                        })?;
                        reported_write_stopped = true;
                    }
                    previous_epoch = snapshot.state_epoch;
                    previous_rank = state_rank(snapshot.instance_state);
                    reads += 1;
                    if reads % 32 == 0 {
                        thread::yield_now();
                    }
                }
                let final_snapshot = runtime.stats();
                if final_snapshot.instance_state != InstanceState::Poisoned
                    || final_snapshot.state_epoch != 2
                    || final_snapshot
                        .first_latched_error
                        .as_ref()
                        .map(|error| error.kind)
                        != Some(StorageErrorKind::StorageWriteStopped)
                {
                    return Err(format!(
                        "reader {reader_id} observed invalid final state {:?}/{}",
                        final_snapshot.instance_state, final_snapshot.state_epoch
                    ));
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }

    let update_stats = Arc::clone(&stats);
    let update_finished = Arc::clone(&finished);
    let update_sender = sender.clone();
    let (update_start_sender, update_start_receiver) = mpsc::channel();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            update_start_receiver
                .recv_timeout(TIMEOUT)
                .map_err(|error| format!("stats updater start timeout: {error}"))?;
            for (phase, start, end) in [
                (1_u8, 1, HEALTHY_UPDATES_END),
                (2, HEALTHY_UPDATES_END + 1, WRITE_STOPPED_UPDATES_END),
                (3, WRITE_STOPPED_UPDATES_END + 1, COMMIT_UPDATES),
            ] {
                for head in start..=end {
                    let durable = head / 2;
                    if !update_stats.update_commit_state(
                        head,
                        durable,
                        Some((u32::try_from(head / 256).unwrap(), head * 64)),
                        Some(u32::try_from(head / 256).unwrap()),
                        u32::try_from(head / 256 + 1).unwrap(),
                        head * 64,
                    ) {
                        return Err(format!("commit stats rejected H={head} D={durable}"));
                    }
                    if head % 16 == 0 {
                        thread::yield_now();
                    }
                }
                if phase < 3 {
                    stats_phase_sender
                        .send(phase)
                        .map_err(|error| format!("stats phase {phase} report failed: {error}"))?;
                    let allowed =
                        stats_continue_receiver
                            .recv_timeout(TIMEOUT)
                            .map_err(|error| {
                                format!("stats phase {phase} continuation timed out: {error}")
                            })?;
                    if allowed != phase {
                        return Err(format!(
                            "stats phase {phase} received wrong continuation {allowed}"
                        ));
                    }
                }
            }
            Ok(())
        })();
        update_finished.store(true, std::sync::atomic::Ordering::Release);
        let _ = update_sender.send(result);
    }));

    let state_runtime = Arc::clone(&runtime);
    let state_sender = sender.clone();
    let (state_start_sender, state_start_receiver) = mpsc::channel();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            state_start_receiver
                .recv_timeout(TIMEOUT)
                .map_err(|error| format!("state writer start timeout: {error}"))?;
            let healthy_phase = stats_phase_receiver
                .recv_timeout(TIMEOUT)
                .map_err(|error| format!("healthy stats phase timed out: {error}"))?;
            if healthy_phase != 1 || state_runtime.stats().head_seq != HEALTHY_UPDATES_END {
                return Err(format!("invalid healthy stats phase {healthy_phase}"));
            }
            let stopped =
                state_runtime.latch_failure(InstanceState::WriteStopped, &write_stopped_failure());
            if !stopped.changed || stopped.current.state_epoch != 1 {
                return Err(format!("write-stop transition failed: {stopped:?}"));
            }
            stats_continue_sender
                .send(1)
                .map_err(|error| format!("write-stop continuation failed: {error}"))?;
            let deadline = Instant::now() + TIMEOUT;
            for observed in 0..READERS {
                stopped_seen_receiver
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .map_err(|error| {
                        format!("write-stop observation {observed}/{READERS} timed out: {error}")
                    })?;
            }
            let stopped_phase = stats_phase_receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| format!("write-stopped stats phase timed out: {error}"))?;
            if stopped_phase != 2 || state_runtime.stats().head_seq != WRITE_STOPPED_UPDATES_END {
                return Err(format!("invalid write-stopped stats phase {stopped_phase}"));
            }
            let poisoned =
                state_runtime.latch_failure(InstanceState::Poisoned, &poisoned_failure());
            if !poisoned.changed || poisoned.current.state_epoch != 2 {
                return Err(format!("poison transition failed: {poisoned:?}"));
            }
            stats_continue_sender
                .send(2)
                .map_err(|error| format!("poison continuation failed: {error}"))?;
            Ok(())
        })();
        let _ = state_sender.send(result);
    }));
    drop(sender);
    drop(reader_ready_sender);
    drop(stopped_seen_sender);

    for start in starts {
        start.send(())?;
    }
    let ready_deadline = Instant::now() + TIMEOUT;
    for ready in 0..READERS {
        reader_ready_receiver
            .recv_timeout(ready_deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| format!("reader ready {ready}/{READERS} timed out: {error}"))?;
    }
    update_start_sender.send(())?;
    state_start_sender.send(())?;
    receive_workers("stats-state-concurrency", READERS + 2, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "stats worker panicked")?;
    }

    let final_snapshot = runtime.stats();
    assert_eq!(final_snapshot.instance_state, InstanceState::Poisoned);
    assert_eq!(final_snapshot.state_epoch, 2);
    assert_eq!(final_snapshot.head_seq, COMMIT_UPDATES);
    assert_eq!(final_snapshot.durable_seq, COMMIT_UPDATES / 2);
    assert_eq!(
        final_snapshot.durability_lag,
        COMMIT_UPDATES - COMMIT_UPDATES / 2
    );
    let first = final_snapshot.first_latched_error.expect("first error");
    assert_eq!(first.kind, StorageErrorKind::StorageWriteStopped);
    assert_eq!(first.operation, Operation::Background);
    Ok(())
}

#[test]
fn last_lease_closes_admission_while_started_operation_guard_remains_live() -> TestResult {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put)?;
    let queued = runtime.enqueue_write(Operation::Delete)?;
    let (lifecycle, lease) = LifecycleController::new_with_external_lease();
    assert!(lifecycle.bind_runtime(Arc::clone(&runtime)));
    let operation = lifecycle.acquire_operation().expect("operation guard");

    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let dropper = thread::spawn(move || {
        drop(lease);
        let _ = dropped_sender.send(());
    });
    dropped_receiver
        .recv_timeout(TIMEOUT)
        .map_err(|error| format!("last lease drop timed out: {error}"))?;
    dropper.join().map_err(|_| "lease dropper panicked")?;

    let lifecycle_snapshot = lifecycle.snapshot();
    assert!(!lifecycle_snapshot.accepting_operations);
    assert_eq!(lifecycle_snapshot.external_leases, 0);
    assert_eq!(lifecycle_snapshot.operation_guards, 1);
    assert!(lifecycle.acquire_operation().is_none());
    assert!(!lifecycle.wait_for_quiescence(Duration::from_millis(20)));

    let queued_error = queued
        .wait_until_started_timeout(Duration::from_millis(200))
        .expect_err("queued write must be cancelled");
    assert_eq!(queued_error.kind, StorageErrorKind::Busy);
    assert_eq!(queued_error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert!(active.wait_until_started_timeout(Duration::ZERO)?);
    assert!(active.finish());

    let later = match runtime.enqueue_write(Operation::WriteBatch) {
        Ok(_) => return Err("closed lifecycle admitted a later write".into()),
        Err(error) => error,
    };
    assert_eq!(later.kind, StorageErrorKind::Busy);
    assert_eq!(later.write_outcome, Some(WriteOutcome::NotCommitted));

    drop(operation);
    assert!(lifecycle.wait_for_quiescence(Duration::from_millis(200)));
    Ok(())
}

fn database_uuid() -> [u8; 16] {
    [101, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47]
}

struct CacheHarness {
    _temporary: TempDir,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
}

impl CacheHarness {
    fn new(file_count: u32) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        for file_id in 0..file_count {
            let file = directory.create_new_for_test(file_id)?;
            let page = PageHeader {
                file_id,
                page_no: 0,
            }
            .encode()?;
            let header = VLogFileHeader::new(database_uuid(), file_id).encode()?;
            file.write_all_at(&page, 0)?;
            file.write_all_at(&header, page.len() as u64)?;
            catalog.register(file_id, &file)?;
        }
        Ok(Self {
            _temporary: temporary,
            directory,
            catalog,
        })
    }

    fn files(&self, capacity: usize, opener: Arc<dyn HandleOpener>) -> Result<Arc<FileSet>> {
        Ok(Arc::new(FileSet::with_opener(
            Arc::clone(&self.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&self.catalog),
            capacity,
            opener,
        )?))
    }
}

#[derive(Default)]
struct CountingOpener {
    calls: std::sync::atomic::AtomicUsize,
}

impl HandleOpener for CountingOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        directory.open_read_only(file_id)
    }
}

struct ConcurrentMissOpener {
    participants: usize,
    calls: std::sync::atomic::AtomicUsize,
    arrived: Mutex<usize>,
    changed: Condvar,
}

impl HandleOpener for ConcurrentMissOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let deadline = Instant::now() + TIMEOUT;
        let mut arrived = self
            .arrived
            .lock()
            .map_err(|_| io::Error::other("concurrent opener mutex poisoned"))?;
        *arrived += 1;
        if *arrived == self.participants {
            self.changed.notify_all();
        }
        while *arrived < self.participants {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "concurrent miss opener timed out at {}/{}",
                        *arrived, self.participants
                    ),
                ));
            }
            let (next, wait) = self
                .changed
                .wait_timeout(arrived, remaining)
                .map_err(|_| io::Error::other("concurrent opener mutex poisoned"))?;
            arrived = next;
            if wait.timed_out() && *arrived < self.participants {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "concurrent miss opener timed out",
                ));
            }
        }
        drop(arrived);
        directory.open_read_only(file_id)
    }
}

#[derive(Default)]
struct EvictionRaceOpener {
    calls: std::sync::atomic::AtomicUsize,
    arrived: Mutex<usize>,
    changed: Condvar,
}

impl HandleOpener for EvictionRaceOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if file_id != 0 {
            let deadline = Instant::now() + TIMEOUT;
            let mut arrived = self
                .arrived
                .lock()
                .map_err(|_| io::Error::other("eviction opener mutex poisoned"))?;
            *arrived += 1;
            if *arrived == 2 {
                self.changed.notify_all();
            }
            while *arrived < 2 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "concurrent eviction open timed out",
                    ));
                }
                let (next, wait) = self
                    .changed
                    .wait_timeout(arrived, remaining)
                    .map_err(|_| io::Error::other("eviction opener mutex poisoned"))?;
                arrived = next;
                if wait.timed_out() && *arrived < 2 {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "concurrent eviction open timed out",
                    ));
                }
            }
        }
        directory.open_read_only(file_id)
    }
}

#[test]
fn vlog_cache_concurrent_miss_converges_and_eviction_keeps_inflight_handles_valid() -> TestResult {
    const MISS_THREADS: usize = 4;

    let harness = CacheHarness::new(3)?;
    let opener = Arc::new(ConcurrentMissOpener {
        participants: MISS_THREADS,
        calls: std::sync::atomic::AtomicUsize::new(0),
        arrived: Mutex::new(0),
        changed: Condvar::new(),
    });
    let files = harness.files(2, opener.clone())?;
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();
    for _ in 0..MISS_THREADS {
        let files = Arc::clone(&files);
        let sender = sender.clone();
        workers.push(thread::spawn(move || {
            let _ = sender.send(files.handle(0));
        }));
    }
    drop(sender);

    let deadline = Instant::now() + TIMEOUT;
    let mut handles = Vec::new();
    for completed in 0..MISS_THREADS {
        let result = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| {
                format!("cache-miss completed={completed}/{MISS_THREADS}: {error}")
            })??;
        handles.push(result);
    }
    for worker in workers {
        worker.join().map_err(|_| "cache miss worker panicked")?;
    }
    assert_eq!(
        opener.calls.load(std::sync::atomic::Ordering::SeqCst),
        MISS_THREADS
    );
    assert_eq!(files.cache_len()?, 1);
    assert!(
        handles
            .iter()
            .all(|handle| Arc::ptr_eq(&handles[0], handle))
    );

    let eviction_opener = Arc::new(EvictionRaceOpener::default());
    let eviction_files = harness.files(2, eviction_opener.clone())?;
    let held = eviction_files.handle(0)?;
    let (sender, receiver) = mpsc::channel();
    let mut workers = Vec::new();
    for file_id in [1, 2] {
        let files = Arc::clone(&eviction_files);
        let sender = sender.clone();
        workers.push(thread::spawn(move || {
            let _ = sender.send(files.handle(file_id).map(|_| file_id));
        }));
    }
    drop(sender);

    let deadline = Instant::now() + TIMEOUT;
    let mut opened = BTreeSet::new();
    for completed in 0..2 {
        opened.insert(
            receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| format!("cache-eviction completed={completed}/2: {error}"))??,
        );
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| "cache eviction worker panicked")?;
    }
    assert_eq!(opened, BTreeSet::from([1, 2]));
    assert_eq!(eviction_files.cache_len()?, 2);
    let order = eviction_files.cache_order()?;
    assert_eq!(order.len(), 2);
    assert_eq!(order.iter().copied().collect::<BTreeSet<_>>(), opened);
    assert!(held.metadata()?.is_file());
    assert_eq!(
        eviction_opener
            .calls
            .load(std::sync::atomic::Ordering::SeqCst),
        3
    );
    Ok(())
}
