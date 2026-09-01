//! Backend-independent fixed-work concurrent runner.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::{
    BackendError, BackendKind, BatchItem, BenchBackend, GetResult, MetricsError, RunMetrics,
    ScanRequest, ScanResult, Trace, TracePartition, Workload, calculate_run_metrics,
};

const FORMAL_THREAD_COUNTS: [usize; 4] = [1, 10, 100, 1_000];

#[derive(Clone)]
pub struct RunSpec<'a> {
    pub backend_kind: BackendKind,
    pub backend: Arc<dyn BenchBackend>,
    pub workload: Workload,
    pub thread_count: usize,
    pub repetition: u32,
    pub trace: &'a Trace,
    pub expected_ops: u64,
    pub records_per_op: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunError {
    InvalidSpec(String),
    Backend(BackendError),
    WorkerPanic {
        thread_index: usize,
        message: String,
    },
    CompletedOpsMismatch {
        expected: u64,
        actual: u64,
    },
    CompletedOpsOverflow,
    LatencySampleMismatch {
        completed_ops: u64,
        samples: usize,
    },
    CompletedRecordsOverflow,
    LatencyDurationOverflow,
    MissingBackendCall,
    MultipleBackendCalls {
        calls: usize,
    },
    Statistics(MetricsError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadRunSummary {
    pub thread_index: usize,
    pub request_start: usize,
    pub assigned_ops: usize,
    pub completed_ops: u64,
    pub latency_samples: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunResult {
    pub backend_kind: BackendKind,
    pub workload: Workload,
    pub thread_count: usize,
    pub repetition: u32,
    pub expected_ops: u64,
    pub completed_ops: u64,
    pub completed_records: u64,
    pub wall_time: Duration,
    pub error_count: usize,
    pub first_error: Option<RunError>,
    pub thread_summaries: Vec<ThreadRunSummary>,
    pub metrics: Option<RunMetrics>,
}

impl RunResult {
    pub fn is_valid(&self) -> bool {
        self.error_count == 0 && self.first_error.is_none() && self.metrics.is_some()
    }
}

pub fn run_concurrent<F>(spec: RunSpec<'_>, execute: F) -> RunResult
where
    F: Fn(&mut RequestContext<'_>, &[u64]) -> Result<(), RunError> + Sync,
{
    let partitions = match validate_and_partition(&spec) {
        Ok(partitions) => partitions,
        Err(error) => return invalid_before_start(&spec, error),
    };
    let barrier = Arc::new(Barrier::new(spec.thread_count + 1));
    let shared_wall_start = Arc::new(OnceLock::<Instant>::new());

    let (wall_start, outcomes) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(spec.thread_count);
        for partition in partitions {
            let backend = Arc::clone(&spec.backend);
            let barrier = Arc::clone(&barrier);
            let shared_wall_start = Arc::clone(&shared_wall_start);
            let execute = &execute;
            let thread_index = partition.thread_index();
            let request_start = partition.request_start();
            let assigned_ops = partition.request_count();
            handles.push((
                thread_index,
                request_start,
                assigned_ops,
                scope
                    .spawn(move || worker(partition, backend, barrier, shared_wall_start, execute)),
            ));
        }

        // Barrier release proves every worker is ready. The OnceLock is then
        // initialized exactly once by the first released participant. Every
        // worker must observe that timestamp before it may execute a request,
        // so no Barrier wait is measured and no request can precede the start.
        barrier.wait();
        let wall_start = *shared_wall_start.get_or_init(Instant::now);

        let outcomes = handles
            .into_iter()
            .map(
                |(thread_index, request_start, assigned_ops, handle)| match handle.join() {
                    Ok(outcome) => outcome,
                    Err(payload) => WorkerOutcome::panic_from_join(
                        thread_index,
                        request_start,
                        assigned_ops,
                        payload,
                    ),
                },
            )
            .collect();
        (wall_start, outcomes)
    });
    // The end timestamp is deliberately obtained only after every Join.
    finish_run(&spec, wall_start.elapsed(), outcomes)
}

fn worker<F>(
    partition: TracePartition<'_>,
    backend: Arc<dyn BenchBackend>,
    barrier: Arc<Barrier>,
    shared_wall_start: Arc<OnceLock<Instant>>,
    execute: &F,
) -> WorkerOutcome
where
    F: Fn(&mut RequestContext<'_>, &[u64]) -> Result<(), RunError> + Sync,
{
    let mut outcome = WorkerOutcome::new(&partition);
    // All request execution is gated by both the Barrier release and the
    // shared timestamp initialization.
    barrier.wait();
    let _ = shared_wall_start.get_or_init(Instant::now);
    for request in partition.requests() {
        let mut request_context = RequestContext::new(backend.as_ref());
        let call = catch_unwind(AssertUnwindSafe(|| execute(&mut request_context, request)));
        match call {
            Ok(execution) => match request_context.finish(execution) {
                Ok(nanos) => {
                    outcome.latency_nanos.push(nanos);
                    outcome.completed_ops += 1;
                }
                Err(error) => {
                    outcome.fail(error);
                    break;
                }
            },
            Err(payload) => {
                outcome.fail(RunError::WorkerPanic {
                    thread_index: partition.thread_index(),
                    message: panic_message(payload),
                });
                break;
            }
        }
    }
    outcome
}

fn validate_and_partition<'a>(spec: &RunSpec<'a>) -> Result<Vec<TracePartition<'a>>, RunError> {
    if !FORMAL_THREAD_COUNTS.contains(&spec.thread_count) {
        return Err(RunError::InvalidSpec(format!(
            "thread_count {} is not one of 1, 10, 100, 1000",
            spec.thread_count
        )));
    }
    if spec.trace.workload() != spec.workload {
        return Err(RunError::InvalidSpec(
            "trace workload does not match RunSpec workload".into(),
        ));
    }
    if spec.trace.repetition() != spec.repetition {
        return Err(RunError::InvalidSpec(
            "trace repetition does not match RunSpec repetition".into(),
        ));
    }
    if usize::try_from(spec.expected_ops).ok() != Some(spec.trace.request_count()) {
        return Err(RunError::InvalidSpec(format!(
            "expected_ops {} does not match trace request count {}",
            spec.expected_ops,
            spec.trace.request_count()
        )));
    }
    if spec.expected_ops == 0 {
        return Err(RunError::InvalidSpec(
            "expected_ops must be non-zero".into(),
        ));
    }
    if spec.records_per_op == 0 {
        return Err(RunError::InvalidSpec(
            "records_per_op must be non-zero".into(),
        ));
    }
    if spec.records_per_op != spec.trace.records_per_operation() {
        return Err(RunError::InvalidSpec(format!(
            "records_per_op {} does not match trace records/op {}",
            spec.records_per_op,
            spec.trace.records_per_operation()
        )));
    }
    spec.trace
        .partition(spec.thread_count)
        .map_err(|error| RunError::InvalidSpec(format!("trace partition failed: {error:?}")))
}

/// Per-request facade that exposes exactly the five frozen Backend operations.
/// Request preparation happens before one of these methods is called; only the
/// selected Backend call is included in the request latency sample.
pub struct RequestContext<'a> {
    backend: &'a dyn BenchBackend,
    calls: usize,
    latency_nanos: Option<u64>,
    call_error: Option<RunError>,
}

impl<'a> RequestContext<'a> {
    fn new(backend: &'a dyn BenchBackend) -> Self {
        Self {
            backend,
            calls: 0,
            latency_nanos: None,
            call_error: None,
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<GetResult, RunError> {
        self.invoke(|backend| backend.get(key))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), RunError> {
        self.invoke(|backend| backend.put(key, value))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), RunError> {
        self.invoke(|backend| backend.delete(key))
    }

    pub fn write_batch(&mut self, items: &[BatchItem<'_>]) -> Result<(), RunError> {
        self.invoke(|backend| backend.write_batch(items))
    }

    pub fn iterator_scan(&mut self, request: ScanRequest<'_>) -> Result<ScanResult, RunError> {
        self.invoke(|backend| backend.iterator_scan(request))
    }

    fn invoke<T>(
        &mut self,
        call: impl FnOnce(&dyn BenchBackend) -> Result<T, BackendError>,
    ) -> Result<T, RunError> {
        self.calls = self.calls.saturating_add(1);
        if self.calls != 1 {
            return Err(RunError::MultipleBackendCalls { calls: self.calls });
        }

        let request_start = Instant::now();
        let result = call(self.backend);
        let elapsed = request_start.elapsed();
        self.latency_nanos =
            Some(u64::try_from(elapsed.as_nanos()).map_err(|_| RunError::LatencyDurationOverflow)?);
        result.map_err(|error| {
            let error = RunError::Backend(error);
            self.call_error = Some(error.clone());
            error
        })
    }

    fn finish(self, execution: Result<(), RunError>) -> Result<u64, RunError> {
        if self.calls == 0 {
            return Err(RunError::MissingBackendCall);
        }
        if self.calls != 1 {
            return Err(RunError::MultipleBackendCalls { calls: self.calls });
        }
        if let Some(error) = self.call_error {
            return Err(error);
        }
        execution?;
        self.latency_nanos.ok_or(RunError::MissingBackendCall)
    }
}

fn finish_run(spec: &RunSpec<'_>, wall_time: Duration, outcomes: Vec<WorkerOutcome>) -> RunResult {
    let mut completed_ops = 0_u64;
    let mut completed_ops_overflow = false;
    let mut latency_nanos = Vec::new();
    let mut observed_errors = Vec::new();
    let mut thread_summaries = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        match completed_ops.checked_add(outcome.completed_ops) {
            Some(total) => completed_ops = total,
            None => completed_ops_overflow = true,
        }
        latency_nanos.extend_from_slice(&outcome.latency_nanos);
        if let Some(error) = outcome.error {
            observed_errors.push(error);
        }
        thread_summaries.push(ThreadRunSummary {
            thread_index: outcome.thread_index,
            request_start: outcome.request_start,
            assigned_ops: outcome.assigned_ops,
            completed_ops: outcome.completed_ops,
            latency_samples: outcome.latency_nanos.len(),
        });
    }

    observed_errors.sort_by_key(|error| (error.observed_at, error.thread_index));
    let mut error_count = observed_errors.len();
    let mut first_error = observed_errors.into_iter().next().map(|error| error.error);
    if completed_ops_overflow {
        error_count += 1;
        if first_error.is_none() {
            first_error = Some(RunError::CompletedOpsOverflow);
        }
    }
    if completed_ops != spec.expected_ops && first_error.is_none() {
        error_count += 1;
        first_error = Some(RunError::CompletedOpsMismatch {
            expected: spec.expected_ops,
            actual: completed_ops,
        });
    }
    if usize::try_from(completed_ops).ok() != Some(latency_nanos.len()) && first_error.is_none() {
        error_count += 1;
        first_error = Some(RunError::LatencySampleMismatch {
            completed_ops,
            samples: latency_nanos.len(),
        });
    }
    let completed_records = match completed_ops.checked_mul(spec.records_per_op) {
        Some(records) => records,
        None => {
            if first_error.is_none() {
                error_count += 1;
                first_error = Some(RunError::CompletedRecordsOverflow);
            }
            0
        }
    };

    let mut result = RunResult {
        backend_kind: spec.backend_kind,
        workload: spec.workload,
        thread_count: spec.thread_count,
        repetition: spec.repetition,
        expected_ops: spec.expected_ops,
        completed_ops,
        completed_records,
        wall_time,
        error_count,
        first_error,
        thread_summaries,
        metrics: None,
    };
    if result.error_count == 0 && result.first_error.is_none() {
        let report_records = matches!(
            spec.workload,
            Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
        );
        match calculate_run_metrics(
            wall_time,
            completed_ops,
            completed_records,
            report_records,
            &latency_nanos,
        ) {
            Ok(metrics) => result.metrics = Some(metrics),
            Err(error) => {
                result.error_count = 1;
                result.first_error = Some(RunError::Statistics(error));
            }
        }
    }
    result
}

fn invalid_before_start(spec: &RunSpec<'_>, error: RunError) -> RunResult {
    RunResult {
        backend_kind: spec.backend_kind,
        workload: spec.workload,
        thread_count: spec.thread_count,
        repetition: spec.repetition,
        expected_ops: spec.expected_ops,
        completed_ops: 0,
        completed_records: 0,
        wall_time: Duration::ZERO,
        error_count: 1,
        first_error: Some(error),
        thread_summaries: Vec::new(),
        metrics: None,
    }
}

struct WorkerOutcome {
    thread_index: usize,
    request_start: usize,
    assigned_ops: usize,
    completed_ops: u64,
    latency_nanos: Vec<u64>,
    error: Option<ObservedError>,
}

impl WorkerOutcome {
    fn new(partition: &TracePartition<'_>) -> Self {
        Self {
            thread_index: partition.thread_index(),
            request_start: partition.request_start(),
            assigned_ops: partition.request_count(),
            completed_ops: 0,
            latency_nanos: Vec::with_capacity(partition.request_count()),
            error: None,
        }
    }

    fn fail(&mut self, error: RunError) {
        self.error = Some(ObservedError {
            observed_at: Instant::now(),
            thread_index: self.thread_index,
            error,
        });
    }

    fn panic_from_join(
        thread_index: usize,
        request_start: usize,
        assigned_ops: usize,
        payload: Box<dyn Any + Send>,
    ) -> Self {
        Self {
            thread_index,
            request_start,
            assigned_ops,
            completed_ops: 0,
            latency_nanos: Vec::new(),
            error: Some(ObservedError {
                observed_at: Instant::now(),
                thread_index,
                error: RunError::WorkerPanic {
                    thread_index,
                    message: panic_message(payload),
                },
            }),
        }
    }
}

struct ObservedError {
    observed_at: Instant,
    thread_index: usize,
    error: RunError,
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        BackendOperation, BackendResult, BatchItem, BenchConfig, GetResult, ScanRequest, ScanResult,
    };

    use super::*;

    #[test]
    fn kth_backend_error_is_first_error_and_requests_are_not_retried() {
        let config = BenchConfig::test_only(20, 10, 10, 8, 4);
        let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();
        let fake = Arc::new(FailingBackend {
            calls: AtomicUsize::new(0),
            fail_at: 7,
        });
        let backend: Arc<dyn BenchBackend> = fake.clone();
        let result = run_concurrent(
            RunSpec {
                backend_kind: BackendKind::RustKv,
                backend,
                workload: Workload::SinglePut,
                thread_count: 1,
                repetition: 0,
                trace: &trace,
                expected_ops: 20,
                records_per_op: 1,
            },
            |request, _| request.get(b"injected-error").map(|_| ()),
        );
        assert!(!result.is_valid());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 7);
        assert_eq!(result.completed_ops, 6);
        assert_eq!(result.error_count, 1);
        assert!(result.metrics.is_none());
        match result.first_error {
            Some(RunError::Backend(error)) => {
                assert_eq!(error.backend(), BackendKind::RustKv);
                assert_eq!(error.operation(), BackendOperation::Get);
                assert_eq!(error.source_text(), "injected seventh call failure");
            }
            other => panic!("unexpected first error: {other:?}"),
        }
    }

    #[test]
    fn backend_error_cannot_be_hidden_by_an_execute_callback() {
        let config = BenchConfig::test_only(2, 1, 1, 2, 1);
        let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
        let fake = Arc::new(FailingBackend {
            calls: AtomicUsize::new(0),
            fail_at: 1,
        });
        let backend: Arc<dyn BenchBackend> = fake.clone();
        let result = run_concurrent(
            RunSpec {
                backend_kind: BackendKind::RustKv,
                backend,
                workload: Workload::RandomGet,
                thread_count: 1,
                repetition: 0,
                trace: &trace,
                expected_ops: 2,
                records_per_op: 1,
            },
            |request, _| {
                let _ignored = request.get(b"ignored-backend-error");
                Ok(())
            },
        );

        assert!(!result.is_valid());
        assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.completed_ops, 0);
        assert_eq!(result.error_count, 1);
        assert!(matches!(result.first_error, Some(RunError::Backend(_))));
        assert!(result.metrics.is_none());
    }

    #[test]
    fn aggregation_rejects_count_sample_record_and_zero_time_inconsistencies() {
        let config = BenchConfig::test_only(2, 1, 1, 2, 1);
        let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
        let backend: Arc<dyn BenchBackend> = Arc::new(FailingBackend {
            calls: AtomicUsize::new(0),
            fail_at: usize::MAX,
        });
        let base_spec = || RunSpec {
            backend_kind: BackendKind::RustKv,
            backend: Arc::clone(&backend),
            workload: Workload::RandomGet,
            thread_count: 1,
            repetition: 0,
            trace: &trace,
            expected_ops: 2,
            records_per_op: 1,
        };

        let mismatch = finish_run(
            &base_spec(),
            Duration::from_secs(1),
            vec![synthetic_outcome(1, vec![10])],
        );
        assert!(matches!(
            mismatch.first_error,
            Some(RunError::CompletedOpsMismatch {
                expected: 2,
                actual: 1
            })
        ));

        let sample_mismatch = finish_run(
            &base_spec(),
            Duration::from_secs(1),
            vec![synthetic_outcome(2, vec![10])],
        );
        assert!(matches!(
            sample_mismatch.first_error,
            Some(RunError::LatencySampleMismatch {
                completed_ops: 2,
                samples: 1
            })
        ));

        let mut overflowing_records_spec = base_spec();
        overflowing_records_spec.records_per_op = u64::MAX;
        let record_overflow = finish_run(
            &overflowing_records_spec,
            Duration::from_secs(1),
            vec![synthetic_outcome(2, vec![10, 20])],
        );
        assert!(matches!(
            record_overflow.first_error,
            Some(RunError::CompletedRecordsOverflow)
        ));
        assert_eq!(record_overflow.completed_records, 0);

        let zero_time = finish_run(
            &base_spec(),
            Duration::ZERO,
            vec![synthetic_outcome(2, vec![10, 20])],
        );
        assert!(matches!(
            zero_time.first_error,
            Some(RunError::Statistics(MetricsError::ZeroElapsedTime))
        ));

        let operation_overflow = finish_run(
            &base_spec(),
            Duration::from_secs(1),
            vec![
                synthetic_outcome(u64::MAX, Vec::new()),
                synthetic_outcome(1, Vec::new()),
            ],
        );
        assert!(matches!(
            operation_overflow.first_error,
            Some(RunError::CompletedOpsOverflow)
        ));
    }

    #[test]
    fn aggregation_orders_worker_errors_and_join_payloads_are_preserved() {
        let config = BenchConfig::test_only(1, 1, 1, 1, 1);
        let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
        let backend: Arc<dyn BenchBackend> = Arc::new(FailingBackend {
            calls: AtomicUsize::new(0),
            fail_at: usize::MAX,
        });
        let spec = RunSpec {
            backend_kind: BackendKind::RustKv,
            backend,
            workload: Workload::RandomGet,
            thread_count: 1,
            repetition: 0,
            trace: &trace,
            expected_ops: 1,
            records_per_op: 1,
        };
        let earlier = Instant::now();
        let later = earlier + Duration::from_secs(1);
        let mut later_error = synthetic_outcome(0, Vec::new());
        later_error.thread_index = 0;
        later_error.error = Some(ObservedError {
            observed_at: later,
            thread_index: 0,
            error: RunError::MultipleBackendCalls { calls: 2 },
        });
        let mut earlier_error = synthetic_outcome(0, Vec::new());
        earlier_error.thread_index = 1;
        earlier_error.error = Some(ObservedError {
            observed_at: earlier,
            thread_index: 1,
            error: RunError::MissingBackendCall,
        });

        let result = finish_run(
            &spec,
            Duration::from_secs(1),
            vec![later_error, earlier_error],
        );
        assert_eq!(result.error_count, 2);
        assert!(matches!(
            result.first_error,
            Some(RunError::MissingBackendCall)
        ));

        let joined =
            WorkerOutcome::panic_from_join(7, 11, 13, Box::new(String::from("join panic payload")));
        assert_eq!(joined.thread_index, 7);
        assert_eq!(joined.request_start, 11);
        assert_eq!(joined.assigned_ops, 13);
        assert!(matches!(
            joined.error.map(|observed| observed.error),
            Some(RunError::WorkerPanic {
                thread_index: 7,
                ref message
            }) if message == "join panic payload"
        ));
    }

    fn synthetic_outcome(completed_ops: u64, latency_nanos: Vec<u64>) -> WorkerOutcome {
        WorkerOutcome {
            thread_index: 0,
            request_start: 0,
            assigned_ops: usize::try_from(completed_ops).unwrap_or(usize::MAX),
            completed_ops,
            latency_nanos,
            error: None,
        }
    }

    struct FailingBackend {
        calls: AtomicUsize,
        fail_at: usize,
    }

    impl FailingBackend {
        fn call(&self) -> BackendResult<GetResult> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_at {
                Err(BackendError::new(
                    BackendKind::RustKv,
                    BackendOperation::Get,
                    "injected seventh call failure",
                ))
            } else {
                Ok(GetResult {
                    found: true,
                    value_length: 1,
                })
            }
        }
    }

    impl BenchBackend for FailingBackend {
        fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
            self.call()
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
            self.call().map(|_| ())
        }

        fn delete(&self, _key: &[u8]) -> BackendResult<()> {
            self.call().map(|_| ())
        }

        fn write_batch(&self, _items: &[BatchItem<'_>]) -> BackendResult<()> {
            self.call().map(|_| ())
        }

        fn iterator_scan(&self, _request: ScanRequest<'_>) -> BackendResult<ScanResult> {
            self.call().map(|_| ScanResult {
                record_count: 1,
                value_bytes: 1,
            })
        }
    }
}
