//! The six frozen benchmark workloads, shared unchanged by both backends.

use std::sync::Arc;

use crate::runner::invalid_before_start;
use crate::{
    BackendKind, BatchItem, BenchBackend, BenchConfig, BenchMode, KeyCodecError, RunError,
    RunResult, RunSpec, ScanRequest, Trace, Workload, encode_key, fixed_value, run_concurrent,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadError {
    TraceConfigurationMismatch {
        workload: Workload,
        repetition: u32,
    },
    InvalidRequestWidth {
        workload: Workload,
        expected: usize,
        actual: usize,
    },
    SizeDoesNotFitUsize {
        workload: Workload,
        value: u64,
    },
    ValueByteCountOverflow {
        workload: Workload,
    },
    KeyEncoding {
        workload: Workload,
        id: u64,
        source: KeyCodecError,
    },
    GetNotFound {
        id: u64,
    },
    GetValueLengthMismatch {
        id: u64,
        expected: usize,
        actual: usize,
    },
    ScanRecordCountMismatch {
        start_id: u64,
        expected: usize,
        actual: usize,
    },
    ScanValueBytesMismatch {
        start_id: u64,
        expected: usize,
        actual: usize,
    },
}

/// One fixed Trace executed against one already-opened shared Backend.
#[derive(Clone)]
pub struct WorkloadRun<'a> {
    mode: BenchMode,
    config: &'a BenchConfig,
    backend_kind: BackendKind,
    backend: Arc<dyn BenchBackend>,
    trace: &'a Trace,
    thread_count: usize,
}

impl<'a> WorkloadRun<'a> {
    pub fn new(
        config: &'a BenchConfig,
        backend_kind: BackendKind,
        backend: Arc<dyn BenchBackend>,
        trace: &'a Trace,
        thread_count: usize,
    ) -> Self {
        Self {
            mode: config.mode(),
            config,
            backend_kind,
            backend,
            trace,
            thread_count,
        }
    }

    pub const fn mode(&self) -> BenchMode {
        self.mode
    }

    pub const fn workload(&self) -> Workload {
        self.trace.workload()
    }

    pub const fn thread_count(&self) -> usize {
        self.thread_count
    }

    pub fn execute(self) -> RunResult {
        run_workload(self)
    }

    fn spec(&self) -> RunSpec<'a> {
        let workload = self.trace.workload();
        RunSpec {
            backend_kind: self.backend_kind,
            backend: Arc::clone(&self.backend),
            workload,
            thread_count: self.thread_count,
            repetition: self.trace.repetition(),
            trace: self.trace,
            expected_ops: workload.operation_count(self.config),
            records_per_op: workload.records_per_operation(self.config),
        }
    }
}

pub fn run_workload(run: WorkloadRun<'_>) -> RunResult {
    if !run.trace.was_generated_from(run.config) {
        return invalid_before_start(
            &run.spec(),
            RunError::Workload(WorkloadError::TraceConfigurationMismatch {
                workload: run.workload(),
                repetition: run.trace.repetition(),
            }),
        );
    }
    match run.workload() {
        Workload::RandomGet => run_random_get(&run),
        Workload::RangeScan => run_range_scan(&run),
        Workload::SinglePut => run_single_put(&run),
        Workload::BatchPut => run_batch_put(&run),
        Workload::SingleDelete => run_single_delete(&run),
        Workload::BatchDelete => run_batch_delete(&run),
    }
}

fn run_random_get(run: &WorkloadRun<'_>) -> RunResult {
    let expected_value_length = run.config.value_length();
    run_concurrent(run.spec(), |request_context, request| {
        let id = single_id(Workload::RandomGet, request)?;
        let key = workload_key(run.config, Workload::RandomGet, id)?;
        let result = request_context.get(&key)?;
        if !result.found {
            return Err(RunError::Workload(WorkloadError::GetNotFound { id }));
        }
        if result.value_length != expected_value_length {
            return Err(RunError::Workload(WorkloadError::GetValueLengthMismatch {
                id,
                expected: expected_value_length,
                actual: result.value_length,
            }));
        }
        Ok(())
    })
}

fn run_range_scan(run: &WorkloadRun<'_>) -> RunResult {
    let expected_value_length = run.config.value_length();
    run_concurrent(run.spec(), |request_context, request| {
        let range_length = usize_value(Workload::RangeScan, run.config.range_length())?;
        let start_id = single_id(Workload::RangeScan, request)?;
        let start = workload_key(run.config, Workload::RangeScan, start_id)?;
        let expected_value_bytes =
            range_length
                .checked_mul(expected_value_length)
                .ok_or(RunError::Workload(WorkloadError::ValueByteCountOverflow {
                    workload: Workload::RangeScan,
                }))?;
        let result = request_context.iterator_scan(ScanRequest::timed(
            &start,
            range_length,
            expected_value_length,
        ))?;
        if result.record_count != range_length {
            return Err(RunError::Workload(WorkloadError::ScanRecordCountMismatch {
                start_id,
                expected: range_length,
                actual: result.record_count,
            }));
        }
        if result.value_bytes != expected_value_bytes {
            return Err(RunError::Workload(WorkloadError::ScanValueBytesMismatch {
                start_id,
                expected: expected_value_bytes,
                actual: result.value_bytes,
            }));
        }
        Ok(())
    })
}

fn run_single_put(run: &WorkloadRun<'_>) -> RunResult {
    let value = fixed_value(run.config);
    run_concurrent(run.spec(), |request_context, request| {
        let id = single_id(Workload::SinglePut, request)?;
        let key = workload_key(run.config, Workload::SinglePut, id)?;
        request_context.put(&key, &value)
    })
}

fn run_batch_put(run: &WorkloadRun<'_>) -> RunResult {
    let value = fixed_value(run.config);
    run_concurrent(run.spec(), |request_context, request| {
        let batch_size = usize_value(Workload::BatchPut, run.config.batch_size())?;
        require_width(Workload::BatchPut, batch_size, request)?;
        let keys = request
            .iter()
            .map(|id| workload_key(run.config, Workload::BatchPut, *id))
            .collect::<Result<Vec<_>, _>>()?;
        let items = keys
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect::<Vec<_>>();
        request_context.write_batch(&items)
    })
}

fn run_single_delete(run: &WorkloadRun<'_>) -> RunResult {
    run_concurrent(run.spec(), |request_context, request| {
        let id = single_id(Workload::SingleDelete, request)?;
        let key = workload_key(run.config, Workload::SingleDelete, id)?;
        request_context.delete(&key)
    })
}

fn run_batch_delete(run: &WorkloadRun<'_>) -> RunResult {
    run_concurrent(run.spec(), |request_context, request| {
        let batch_size = usize_value(Workload::BatchDelete, run.config.batch_size())?;
        require_width(Workload::BatchDelete, batch_size, request)?;
        let keys = request
            .iter()
            .map(|id| workload_key(run.config, Workload::BatchDelete, *id))
            .collect::<Result<Vec<_>, _>>()?;
        let items = keys
            .iter()
            .map(|key| BatchItem::Delete { key })
            .collect::<Vec<_>>();
        request_context.write_batch(&items)
    })
}

fn single_id(workload: Workload, request: &[u64]) -> Result<u64, RunError> {
    require_width(workload, 1, request)?;
    Ok(request[0])
}

fn require_width(workload: Workload, expected: usize, request: &[u64]) -> Result<(), RunError> {
    if request.len() == expected {
        Ok(())
    } else {
        Err(RunError::Workload(WorkloadError::InvalidRequestWidth {
            workload,
            expected,
            actual: request.len(),
        }))
    }
}

fn workload_key(
    config: &BenchConfig,
    workload: Workload,
    id: u64,
) -> Result<[u8; crate::KEY_LENGTH], RunError> {
    encode_key(config, id).map_err(|source| {
        RunError::Workload(WorkloadError::KeyEncoding {
            workload,
            id,
            source,
        })
    })
}

fn usize_value(workload: Workload, value: u64) -> Result<usize, RunError> {
    usize::try_from(value)
        .map_err(|_| RunError::Workload(WorkloadError::SizeDoesNotFitUsize { workload, value }))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        BackendError, BackendOperation, BackendResult, GetResult, ScanResult, ScanValidation,
    };

    use super::*;

    struct FailingBackend {
        operation: BackendOperation,
        source_text: &'static str,
        calls: AtomicUsize,
        scan_calls: Mutex<Vec<(Vec<u8>, usize, usize)>>,
    }

    impl FailingBackend {
        fn new(operation: BackendOperation, source_text: &'static str) -> Self {
            Self {
                operation,
                source_text,
                calls: AtomicUsize::new(0),
                scan_calls: Mutex::new(Vec::new()),
            }
        }

        fn call(&self, operation: BackendOperation) -> BackendResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if operation == self.operation {
                Err(BackendError::new(
                    BackendKind::LevelDb,
                    operation,
                    self.source_text,
                ))
            } else {
                Ok(())
            }
        }
    }

    impl BenchBackend for FailingBackend {
        fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
            self.call(BackendOperation::Get)?;
            Ok(GetResult {
                found: true,
                value_length: 1_024,
            })
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
            self.call(BackendOperation::Put)
        }

        fn delete(&self, _key: &[u8]) -> BackendResult<()> {
            self.call(BackendOperation::Delete)
        }

        fn write_batch(&self, _items: &[BatchItem<'_>]) -> BackendResult<()> {
            self.call(BackendOperation::WriteBatch)
        }

        fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
            let expected_value_length = match request.validation {
                ScanValidation::Timed {
                    expected_value_length,
                } => expected_value_length,
                ScanValidation::Full { .. } => panic!("B4 workload must use timed Scan validation"),
            };
            self.scan_calls.lock().unwrap().push((
                request.start.to_vec(),
                request.limit,
                expected_value_length,
            ));
            self.call(BackendOperation::IteratorScan)?;
            Ok(ScanResult {
                record_count: request.limit,
                value_bytes: request.limit * expected_value_length,
            })
        }
    }

    #[test]
    fn every_backend_failure_is_preserved_without_retry_or_successful_op() {
        let config = BenchConfig::test_only(200, 100, 100, 20, 10);
        for (workload, operation) in [
            (Workload::RandomGet, BackendOperation::Get),
            (Workload::RangeScan, BackendOperation::IteratorScan),
            (Workload::SinglePut, BackendOperation::Put),
            (Workload::BatchPut, BackendOperation::WriteBatch),
            (Workload::SingleDelete, BackendOperation::Delete),
            (Workload::BatchDelete, BackendOperation::WriteBatch),
        ] {
            let trace = Trace::generate(&config, workload, 0).unwrap();
            let backend = Arc::new(FailingBackend::new(operation, "injected backend failure"));
            let shared: Arc<dyn BenchBackend> = backend.clone();
            let result =
                WorkloadRun::new(&config, BackendKind::LevelDb, shared, &trace, 1).execute();

            assert!(!result.is_valid(), "{workload} unexpectedly succeeded");
            assert_eq!(result.completed_ops, 0);
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            match result.first_error {
                Some(RunError::Backend(error)) => {
                    assert_eq!(error.backend(), BackendKind::LevelDb);
                    assert_eq!(error.operation(), operation);
                    assert_eq!(error.source_text(), "injected backend failure");
                }
                other => panic!("unexpected {workload} failure: {other:?}"),
            }
        }
    }

    #[test]
    fn range_contract_failures_are_propagated_after_the_exact_scan_request() {
        let config = BenchConfig::test_only(200, 100, 100, 20, 10);
        let trace = Trace::generate(&config, Workload::RangeScan, 0).unwrap();
        let expected_start = encode_key(&config, trace.request(0).unwrap()[0]).unwrap();

        // Row order, seek-boundary, and per-row Value validation belong to B2
        // and are intentionally hidden behind BenchBackend. Inject each B2
        // failure at that contract boundary and prove B4 neither retries it
        // nor counts it as a completed Range operation.
        for failure in InjectedRangeFailure::ALL {
            let source_text = failure.source_text();
            let backend = Arc::new(FailingBackend::new(
                BackendOperation::IteratorScan,
                source_text,
            ));
            let shared: Arc<dyn BenchBackend> = backend.clone();
            let result =
                WorkloadRun::new(&config, BackendKind::LevelDb, shared, &trace, 1).execute();

            assert!(!result.is_valid());
            assert_eq!(result.completed_ops, 0);
            assert_eq!(result.error_count, 1);
            assert!(result.metrics.is_none());
            assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                *backend.scan_calls.lock().unwrap(),
                vec![(
                    expected_start.to_vec(),
                    config.range_length() as usize,
                    config.value_length(),
                )]
            );
            match result.first_error {
                Some(RunError::Backend(error)) => {
                    assert_eq!(error.backend(), BackendKind::LevelDb);
                    assert_eq!(error.operation(), BackendOperation::IteratorScan);
                    assert_eq!(error.source_text(), source_text);
                }
                other => panic!("unexpected range failure: {other:?}"),
            }
        }
    }

    #[derive(Clone, Copy)]
    enum InjectedRangeFailure {
        NonIncreasingKeys,
        KeyBeforeSeekTarget,
        WrongValueLength,
        BackendFailure,
    }

    impl InjectedRangeFailure {
        const ALL: [Self; 4] = [
            Self::NonIncreasingKeys,
            Self::KeyBeforeSeekTarget,
            Self::WrongValueLength,
            Self::BackendFailure,
        ];

        const fn source_text(self) -> &'static str {
            match self {
                Self::NonIncreasingKeys => "iterator keys are not strictly increasing",
                Self::KeyBeforeSeekTarget => "iterator returned a key below the seek target",
                Self::WrongValueLength => "iterator value length differs from expected",
                Self::BackendFailure => "injected iterator backend failure",
            }
        }
    }
}
