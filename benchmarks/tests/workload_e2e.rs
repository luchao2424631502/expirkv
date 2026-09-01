use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{
    BackendKind, BackendResult, BatchItem, BenchBackend, BenchConfig, BenchMode, ExpectedRecord,
    LevelDbBackend, RustKvBackend, ScanRequest, Trace, Workload, WorkloadRun, encode_key,
    fixed_value,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
enum BackendChoice {
    RustKv,
    LevelDb,
}

impl BackendChoice {
    const ALL: [Self; 2] = [Self::RustKv, Self::LevelDb];

    const fn kind(self) -> BackendKind {
        match self {
            Self::RustKv => BackendKind::RustKv,
            Self::LevelDb => BackendKind::LevelDb,
        }
    }

    const fn label(self) -> &'static str {
        self.kind().as_str()
    }
}

fn open_backend(
    choice: BackendChoice,
    path: &Path,
    config: &BenchConfig,
) -> BackendResult<Arc<dyn BenchBackend>> {
    match choice {
        BackendChoice::RustKv => {
            RustKvBackend::open(path, config).map(|backend| Arc::new(backend) as _)
        }
        BackendChoice::LevelDb => {
            LevelDbBackend::open(path, config).map(|backend| Arc::new(backend) as _)
        }
    }
}

#[test]
fn both_real_backends_run_all_workloads_at_one_and_ten_threads() -> TestResult {
    let config = BenchConfig::test_only(1_000, 100, 100, 100, 20);
    assert_eq!(config.mode(), BenchMode::Smoke);

    for workload in Workload::ALL {
        let trace = Trace::generate(&config, workload, 0).unwrap();
        for thread_count in [1, 10] {
            for choice in BackendChoice::ALL {
                run_and_verify(&config, &trace, thread_count, choice)?;
            }
        }
    }
    Ok(())
}

fn run_and_verify(
    config: &BenchConfig,
    trace: &Trace,
    thread_count: usize,
    choice: BackendChoice,
) -> TestResult {
    let label = format!(
        "{}-{}-{thread_count}",
        choice.label(),
        trace.workload().as_str()
    );
    let temporary = TestDirectory::new(&label);
    let path = temporary.path().join("db");
    if requires_full_initial_database(trace.workload()) {
        populate_all(choice, &path, config)?;
    }

    // Open used for population is dropped above. The measured workload always
    // starts from a reopen for reads/deletes and from a newly opened empty DB
    // for inserts.
    let backend = open_backend(choice, &path, config)?;
    let result = WorkloadRun::new(config, choice.kind(), backend, trace, thread_count).execute();
    assert!(
        result.is_valid(),
        "{choice:?} {} x{thread_count}: {result:?}",
        trace.workload()
    );
    assert_eq!(result.backend_kind, choice.kind());
    assert_eq!(result.workload, trace.workload());
    assert_eq!(result.thread_count, thread_count);
    assert_eq!(result.expected_ops, trace.request_count() as u64);
    assert_eq!(result.completed_ops, trace.request_count() as u64);
    assert_eq!(
        result.completed_records,
        result.completed_ops * trace.records_per_operation()
    );
    assert_eq!(result.error_count, 0);
    assert!(result.first_error.is_none());
    if thread_count == 10 {
        assert_eq!(result.thread_summaries.len(), 10);
        assert!(
            result
                .thread_summaries
                .iter()
                .all(|summary| summary.assigned_ops > 0),
            "every worker must execute requests for {choice:?} {}",
            trace.workload()
        );
    }
    let metrics = result.metrics.as_ref().unwrap();
    assert!(metrics.ops_per_second().is_finite());
    assert!(metrics.ops_per_second() > 0.0);
    assert_eq!(
        metrics.records_per_second().is_some(),
        matches!(
            trace.workload(),
            Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
        )
    );
    assert_eq!(metrics.latency().sample_count(), trace.request_count());
    assert!(metrics.latency().mean_us().is_finite());

    // WorkloadRun consumed the only Arc, so this is a genuine close/reopen
    // before all correctness checks outside the timed interval.
    let reopened = open_backend(choice, &path, config)?;
    match trace.workload() {
        Workload::RandomGet => {
            verify_full_database(reopened.as_ref(), config)?;
            for request in trace.requests() {
                let result = reopened.get(&encode_key(config, request[0]).unwrap())?;
                assert!(result.found);
                assert_eq!(result.value_length, config.value_length());
            }
        }
        Workload::RangeScan => {
            verify_full_database(reopened.as_ref(), config)?;
            verify_every_range(reopened.as_ref(), config, trace)?;
        }
        Workload::SinglePut | Workload::BatchPut => {
            verify_full_database(reopened.as_ref(), config)?;
        }
        Workload::SingleDelete | Workload::BatchDelete => {
            verify_empty_database(reopened.as_ref())?;
        }
    }
    Ok(())
}

fn requires_full_initial_database(workload: Workload) -> bool {
    matches!(
        workload,
        Workload::RandomGet | Workload::RangeScan | Workload::SingleDelete | Workload::BatchDelete
    )
}

fn populate_all(choice: BackendChoice, path: &Path, config: &BenchConfig) -> TestResult {
    let backend = open_backend(choice, path, config)?;
    let value = fixed_value(config);
    let keys: Vec<_> = (0..config.record_count())
        .map(|id| encode_key(config, id).unwrap())
        .collect();
    for chunk in keys.chunks(config.batch_size() as usize) {
        let items: Vec<_> = chunk
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect();
        backend.write_batch(&items)?;
    }
    drop(backend);
    Ok(())
}

fn verify_full_database(backend: &dyn BenchBackend, config: &BenchConfig) -> TestResult {
    let value = fixed_value(config);
    let keys: Vec<_> = (0..config.record_count())
        .map(|id| encode_key(config, id).unwrap())
        .collect();
    let expected: Vec<_> = keys
        .iter()
        .map(|key| ExpectedRecord { key, value: &value })
        .collect();
    let result = backend.iterator_scan(ScanRequest::full(b"", expected.len() + 1, &expected))?;
    assert_eq!(result.record_count, expected.len());
    assert_eq!(result.value_bytes, expected.len() * value.len());
    Ok(())
}

fn verify_empty_database(backend: &dyn BenchBackend) -> TestResult {
    let result = backend.iterator_scan(ScanRequest::full(b"", 1, &[]))?;
    assert_eq!(result.record_count, 0);
    assert_eq!(result.value_bytes, 0);
    Ok(())
}

fn verify_every_range(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
    trace: &Trace,
) -> TestResult {
    let value = fixed_value(config);
    for request in trace.requests() {
        let start_id = request[0];
        let keys: Vec<_> = (start_id..start_id + config.range_length())
            .map(|id| encode_key(config, id).unwrap())
            .collect();
        let expected: Vec<_> = keys
            .iter()
            .map(|key| ExpectedRecord { key, value: &value })
            .collect();
        let result =
            backend.iterator_scan(ScanRequest::full(&keys[0], expected.len(), &expected))?;
        assert_eq!(result.record_count, expected.len());
        assert_eq!(result.value_bytes, expected.len() * value.len());
    }
    Ok(())
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kv-bench-b4-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique temporary directory must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
