use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use kv_bench::{
    BackendKind, BackendResult, BatchItem, BenchBackend, BenchConfig, GetResult, RequestContext,
    RunError, RunSpec, ScanRequest, ScanResult, ScanValidation, Trace, Workload, run_concurrent,
};

#[test]
fn fixed_trace_is_consumed_once_for_1_10_100_and_1000_os_threads() {
    let config = BenchConfig::test_only(110, 10, 10, 32, 20);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();
    for thread_count in [1, 10, 100, 1_000] {
        let fake = Arc::new(SuccessBackend::default());
        let backend: Arc<dyn BenchBackend> = fake.clone();
        let observed = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let observed_by_call = Arc::clone(&observed);
        let result = run_concurrent(
            spec(&trace, backend, thread_count, 1),
            move |backend, request| {
                observed_by_call.lock().unwrap().push(request.to_vec());
                backend.get(b"fake-request").map(|_| ())
            },
        );
        assert!(result.is_valid(), "unexpected result: {result:?}");
        assert_eq!(result.backend_kind, BackendKind::RustKv);
        assert_eq!(result.workload, Workload::SinglePut);
        assert_eq!(result.thread_count, thread_count);
        assert_eq!(result.repetition, 0);
        assert_eq!(result.expected_ops, trace.request_count() as u64);
        assert_eq!(result.thread_summaries.len(), thread_count);
        assert_eq!(fake.calls.load(Ordering::SeqCst), trace.request_count());
        assert_eq!(result.completed_ops, trace.request_count() as u64);
        assert_eq!(result.completed_records, trace.request_count() as u64);
        assert_eq!(
            result.metrics.as_ref().unwrap().latency().sample_count(),
            trace.request_count()
        );
        assert_eq!(
            result.metrics.as_ref().unwrap().ops_per_second(),
            result.completed_ops as f64 / result.wall_time.as_secs_f64()
        );

        let expected_partitions = trace.partition(thread_count).unwrap();
        for (summary, partition) in result
            .thread_summaries
            .iter()
            .zip(expected_partitions.iter())
        {
            assert_eq!(summary.thread_index, partition.thread_index());
            assert_eq!(summary.request_start, partition.request_start());
            assert_eq!(summary.assigned_ops, partition.request_count());
            assert_eq!(summary.completed_ops as usize, partition.request_count());
            assert_eq!(summary.latency_samples, partition.request_count());
        }

        let mut consumed: Vec<_> = observed.lock().unwrap().iter().flatten().copied().collect();
        consumed.sort_unstable();
        assert_eq!(consumed, (0..110).collect::<Vec<_>>());
    }
}

#[test]
fn ten_threads_really_overlap_and_backend_calls_follow_worker_barrier() {
    let config = BenchConfig::test_only(100, 10, 10, 16, 10);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();
    let fake = Arc::new(ConcurrentBackend::new(10));
    let backend: Arc<dyn BenchBackend> = fake.clone();
    let result = run_concurrent(spec(&trace, backend, 10, 1), |request, _| {
        request.get(b"concurrent-request").map(|_| ())
    });
    assert!(result.is_valid(), "unexpected result: {result:?}");
    assert_eq!(fake.max_in_flight.load(Ordering::SeqCst), 10);

    let source = include_str!("../src/runner.rs");
    let worker = source.split("fn worker<").nth(1).expect("worker function");
    let before_requests = &worker[..worker.find("for request in").unwrap()];
    assert_eq!(before_requests.matches("barrier.wait();").count(), 1);
    assert!(
        worker.find("barrier.wait();").unwrap() < worker.find("for request in").unwrap(),
        "worker must pass the shared start Barrier before any request"
    );
    assert!(
        before_requests.find("barrier.wait();").unwrap()
            < before_requests
                .find("shared_wall_start.get_or_init(Instant::now)")
                .unwrap()
    );
    let main = &source[..source.find("fn worker<").unwrap()];
    let main_barrier = main.rfind("barrier.wait();").expect("main Barrier");
    let main_timestamp = main
        .rfind("shared_wall_start.get_or_init(Instant::now)")
        .expect("shared wall timestamp");
    assert!(main_barrier < main_timestamp);

    let context = source
        .split("impl<'a> RequestContext<'a>")
        .nth(1)
        .expect("request context implementation");
    let invoke = context.split("fn invoke<T>").nth(1).expect("timed invoke");
    let timer_start = invoke.find("let request_start = Instant::now();").unwrap();
    let backend_call = invoke.find("let result = call(self.backend);").unwrap();
    let timer_end = invoke
        .find("let elapsed = request_start.elapsed();")
        .unwrap();
    assert!(timer_start < backend_call && backend_call < timer_end);
}

#[test]
fn op_and_record_accounting_matches_all_six_workload_shapes() {
    let config = BenchConfig::test_only(200, 100, 100, 20, 20);
    for (workload, records_per_op, auxiliary_rate) in [
        (Workload::RandomGet, 1, false),
        (Workload::RangeScan, 100, true),
        (Workload::SinglePut, 1, false),
        (Workload::BatchPut, 100, true),
        (Workload::SingleDelete, 1, false),
        (Workload::BatchDelete, 100, true),
    ] {
        let trace = Trace::generate(&config, workload, 0).unwrap();
        let backend: Arc<dyn BenchBackend> = Arc::new(SuccessBackend::default());
        let result = run_concurrent(spec(&trace, backend, 1, records_per_op), |request, _| {
            request.get(b"accounting").map(|_| ())
        });
        assert!(result.is_valid(), "unexpected result: {result:?}");
        assert_eq!(result.completed_ops, trace.request_count() as u64);
        assert_eq!(
            result.completed_records,
            result.completed_ops * records_per_op
        );
        assert_eq!(
            result
                .metrics
                .as_ref()
                .unwrap()
                .records_per_second()
                .is_some(),
            auxiliary_rate
        );
    }
}

#[test]
fn request_context_forwards_each_frozen_backend_operation_once() {
    let config = BenchConfig::test_only(10, 5, 5, 1, 1);

    run_one_context_method(
        &config,
        Workload::RandomGet,
        1,
        ObservedCall::Get(b"get-key".to_vec()),
        |request, _| request.get(b"get-key").map(|_| ()),
    );
    run_one_context_method(
        &config,
        Workload::RangeScan,
        5,
        ObservedCall::IteratorScan {
            start: b"scan-start".to_vec(),
            limit: 1,
            validation: ObservedScanValidation::Timed(1_024),
        },
        |request, _| {
            request
                .iterator_scan(ScanRequest::timed(b"scan-start", 1, 1_024))
                .map(|_| ())
        },
    );
    run_one_context_method(
        &config,
        Workload::SinglePut,
        1,
        ObservedCall::Put {
            key: b"put-key".to_vec(),
            value: b"put-value".to_vec(),
        },
        |request, _| request.put(b"put-key", b"put-value"),
    );
    run_one_context_method(
        &config,
        Workload::SingleDelete,
        1,
        ObservedCall::Delete(b"delete-key".to_vec()),
        |request, _| request.delete(b"delete-key"),
    );
    run_one_context_method(
        &config,
        Workload::BatchPut,
        5,
        ObservedCall::WriteBatch(vec![ObservedBatchItem::Put {
            key: b"batch-key".to_vec(),
            value: b"batch-value".to_vec(),
        }]),
        |request, _| {
            request.write_batch(&[BatchItem::Put {
                key: b"batch-key",
                value: b"batch-value",
            }])
        },
    );
}

fn run_one_context_method<F>(
    config: &BenchConfig,
    workload: Workload,
    records_per_op: u64,
    expected_call: ObservedCall,
    execute: F,
) where
    F: Fn(&mut RequestContext<'_>, &[u64]) -> Result<(), RunError> + Sync,
{
    let trace = Trace::generate(config, workload, 0).unwrap();
    let fake = Arc::new(RecordingBackend::default());
    let backend: Arc<dyn BenchBackend> = fake.clone();
    let result = run_concurrent(spec(&trace, backend, 1, records_per_op), execute);
    assert!(result.is_valid(), "unexpected result: {result:?}");
    let calls = fake.calls.lock().unwrap();
    assert_eq!(calls.len(), trace.request_count());
    assert!(calls.iter().all(|call| call == &expected_call));
}

#[test]
fn records_per_op_must_match_the_generating_trace_configuration() {
    let config = BenchConfig::test_only(100, 10, 10, 16, 8);
    for workload in [
        Workload::RangeScan,
        Workload::BatchPut,
        Workload::BatchDelete,
    ] {
        let trace = Trace::generate(&config, workload, 0).unwrap();
        let fake = Arc::new(SuccessBackend::default());
        let backend: Arc<dyn BenchBackend> = fake.clone();
        let result = run_concurrent(spec(&trace, backend, 1, 100), |request, _| {
            request.get(b"must-not-run").map(|_| ())
        });
        assert!(!result.is_valid());
        assert!(matches!(result.first_error, Some(RunError::InvalidSpec(_))));
        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn every_successful_request_requires_exactly_one_timed_backend_call() {
    let config = BenchConfig::test_only(10, 5, 5, 4, 4);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();

    let missing_backend: Arc<dyn BenchBackend> = Arc::new(SuccessBackend::default());
    let missing = run_concurrent(spec(&trace, missing_backend, 1, 1), |_request, _| Ok(()));
    assert!(!missing.is_valid());
    assert_eq!(missing.completed_ops, 0);
    assert!(matches!(
        missing.first_error,
        Some(RunError::MissingBackendCall)
    ));

    let duplicate_fake = Arc::new(SuccessBackend::default());
    let duplicate_backend: Arc<dyn BenchBackend> = duplicate_fake.clone();
    let duplicate = run_concurrent(spec(&trace, duplicate_backend, 1, 1), |request, _| {
        request.get(b"first")?;
        request.get(b"second")?;
        Ok(())
    });
    assert!(!duplicate.is_valid());
    assert_eq!(duplicate.completed_ops, 0);
    assert_eq!(duplicate_fake.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        duplicate.first_error,
        Some(RunError::MultipleBackendCalls { calls: 2 })
    ));
}

#[test]
fn worker_panic_returns_an_invalid_result_without_panicking_the_caller() {
    let config = BenchConfig::test_only(100, 10, 10, 16, 10);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();
    let backend: Arc<dyn BenchBackend> = Arc::new(SuccessBackend::default());
    let result = run_concurrent(spec(&trace, backend, 10, 1), |backend, request| {
        if request[0] == 50 {
            panic!("injected worker panic");
        }
        backend.get(b"panic-test").map(|_| ())
    });
    assert!(!result.is_valid());
    assert!(result.error_count > 0);
    assert!(result.metrics.is_none());
    assert!(result.completed_ops < result.expected_ops);
    assert!(matches!(
        result.first_error,
        Some(RunError::WorkerPanic { .. })
    ));
}

#[test]
fn invalid_spec_never_starts_threads_or_produces_metrics() {
    let config = BenchConfig::test_only(100, 10, 10, 16, 10);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();
    let fake = Arc::new(SuccessBackend::default());
    let backend: Arc<dyn BenchBackend> = fake.clone();
    let result = run_concurrent(spec(&trace, backend, 2, 1), |backend, _| {
        backend.get(b"must-not-run").map(|_| ())
    });
    assert!(!result.is_valid());
    assert_eq!(result.error_count, 1);
    assert!(matches!(result.first_error, Some(RunError::InvalidSpec(_))));
    assert!(result.thread_summaries.is_empty());
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    assert_eq!(result.wall_time, std::time::Duration::ZERO);
    assert!(result.metrics.is_none());
}

#[test]
fn every_remaining_invalid_spec_branch_fails_before_starting_workers() {
    let config = BenchConfig::test_only(100, 10, 10, 16, 10);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();

    assert_invalid_spec(&trace, |spec| spec.workload = Workload::RandomGet);
    assert_invalid_spec(&trace, |spec| spec.repetition = 1);
    assert_invalid_spec(&trace, |spec| spec.expected_ops += 1);
    assert_invalid_spec(&trace, |spec| spec.records_per_op = 0);

    let empty_config = BenchConfig::test_only(10, 5, 5, 0, 1);
    let empty_trace = Trace::generate(&empty_config, Workload::RandomGet, 0).unwrap();
    let fake = Arc::new(SuccessBackend::default());
    let backend: Arc<dyn BenchBackend> = fake.clone();
    let result = run_concurrent(spec(&empty_trace, backend, 1, 1), |request, _| {
        request.get(b"must-not-run").map(|_| ())
    });
    assert!(!result.is_valid());
    assert!(matches!(result.first_error, Some(RunError::InvalidSpec(_))));
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
}

fn assert_invalid_spec(trace: &Trace, mutate: impl FnOnce(&mut RunSpec<'_>)) {
    let fake = Arc::new(SuccessBackend::default());
    let backend: Arc<dyn BenchBackend> = fake.clone();
    let mut invalid = spec(trace, backend, 1, 1);
    mutate(&mut invalid);
    let result = run_concurrent(invalid, |request, _| {
        request.get(b"must-not-run").map(|_| ())
    });
    assert!(!result.is_valid());
    assert!(matches!(result.first_error, Some(RunError::InvalidSpec(_))));
    assert!(result.thread_summaries.is_empty());
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn string_and_non_string_panic_payloads_are_preserved_or_classified() {
    let config = BenchConfig::test_only(10, 5, 5, 1, 1);
    let trace = Trace::generate(&config, Workload::SinglePut, 0).unwrap();

    let string_backend: Arc<dyn BenchBackend> = Arc::new(SuccessBackend::default());
    let string_result = run_concurrent(spec(&trace, string_backend, 1, 1), |_request, _| {
        std::panic::panic_any(String::from("owned panic payload"))
    });
    assert!(matches!(
        string_result.first_error,
        Some(RunError::WorkerPanic { ref message, .. }) if message == "owned panic payload"
    ));

    let opaque_backend: Arc<dyn BenchBackend> = Arc::new(SuccessBackend::default());
    let opaque_result = run_concurrent(spec(&trace, opaque_backend, 1, 1), |_request, _| {
        std::panic::panic_any(17_u32)
    });
    assert!(matches!(
        opaque_result.first_error,
        Some(RunError::WorkerPanic { ref message, .. }) if message == "non-string panic payload"
    ));
}

fn spec<'a>(
    trace: &'a Trace,
    backend: Arc<dyn BenchBackend>,
    thread_count: usize,
    records_per_op: u64,
) -> RunSpec<'a> {
    RunSpec {
        backend_kind: BackendKind::RustKv,
        backend,
        workload: trace.workload(),
        thread_count,
        repetition: trace.repetition(),
        trace,
        expected_ops: trace.request_count() as u64,
        records_per_op,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObservedCall {
    Get(Vec<u8>),
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete(Vec<u8>),
    WriteBatch(Vec<ObservedBatchItem>),
    IteratorScan {
        start: Vec<u8>,
        limit: usize,
        validation: ObservedScanValidation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObservedBatchItem {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObservedScanValidation {
    Timed(usize),
    Full(Vec<(Vec<u8>, Vec<u8>)>),
}

#[derive(Default)]
struct RecordingBackend {
    calls: Mutex<Vec<ObservedCall>>,
}

impl RecordingBackend {
    fn record(&self, call: ObservedCall) {
        self.calls.lock().unwrap().push(call);
    }
}

impl BenchBackend for RecordingBackend {
    fn get(&self, key: &[u8]) -> BackendResult<GetResult> {
        self.record(ObservedCall::Get(key.to_vec()));
        Ok(GetResult {
            found: true,
            value_length: 1_024,
        })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BackendResult<()> {
        self.record(ObservedCall::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> BackendResult<()> {
        self.record(ObservedCall::Delete(key.to_vec()));
        Ok(())
    }

    fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()> {
        let items = items
            .iter()
            .map(|item| match item {
                BatchItem::Put { key, value } => ObservedBatchItem::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                BatchItem::Delete { key } => ObservedBatchItem::Delete(key.to_vec()),
            })
            .collect();
        self.record(ObservedCall::WriteBatch(items));
        Ok(())
    }

    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        let validation = match request.validation {
            ScanValidation::Timed {
                expected_value_length,
            } => ObservedScanValidation::Timed(expected_value_length),
            ScanValidation::Full { expected } => ObservedScanValidation::Full(
                expected
                    .iter()
                    .map(|record| (record.key.to_vec(), record.value.to_vec()))
                    .collect(),
            ),
        };
        self.record(ObservedCall::IteratorScan {
            start: request.start.to_vec(),
            limit: request.limit,
            validation,
        });
        Ok(ScanResult {
            record_count: request.limit,
            value_bytes: request.limit * 1_024,
        })
    }
}

#[derive(Default)]
struct SuccessBackend {
    calls: AtomicUsize,
}

impl SuccessBackend {
    fn succeed(&self) -> BackendResult<GetResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(GetResult {
            found: true,
            value_length: 1_024,
        })
    }
}

impl BenchBackend for SuccessBackend {
    fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
        self.succeed()
    }

    fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
        self.succeed().map(|_| ())
    }

    fn delete(&self, _key: &[u8]) -> BackendResult<()> {
        self.succeed().map(|_| ())
    }

    fn write_batch(&self, _items: &[BatchItem<'_>]) -> BackendResult<()> {
        self.succeed().map(|_| ())
    }

    fn iterator_scan(&self, _request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        self.succeed().map(|_| ScanResult {
            record_count: 1,
            value_bytes: 1_024,
        })
    }
}

struct ConcurrentBackend {
    calls: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    first_call_barrier: Barrier,
    barrier_participants: usize,
}

impl ConcurrentBackend {
    fn new(barrier_participants: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            first_call_barrier: Barrier::new(barrier_participants),
            barrier_participants,
        }
    }

    fn call(&self) -> BackendResult<GetResult> {
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        if call_index < self.barrier_participants {
            self.first_call_barrier.wait();
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(GetResult {
            found: true,
            value_length: 1_024,
        })
    }
}

impl BenchBackend for ConcurrentBackend {
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
            value_bytes: 1_024,
        })
    }
}
