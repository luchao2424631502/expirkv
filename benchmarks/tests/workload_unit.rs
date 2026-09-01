use std::sync::{Arc, Mutex};

use kv_bench::{
    BackendKind, BackendResult, BatchItem, BenchBackend, BenchConfig, BenchMode, GetResult,
    RunError, ScanRequest, ScanResult, ScanValidation, Trace, Workload, WorkloadError, WorkloadRun,
    encode_key, fixed_value,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Call {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Batch(Vec<Mutation>),
    Scan {
        start: Vec<u8>,
        limit: usize,
        expected_value_length: usize,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Mutation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

struct RecordingBackend {
    calls: Mutex<Vec<Call>>,
    get_result: GetResult,
    scan_result: Option<ScanResult>,
}

impl Default for RecordingBackend {
    fn default() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            get_result: GetResult {
                found: true,
                value_length: 1_024,
            },
            scan_result: None,
        }
    }
}

impl BenchBackend for RecordingBackend {
    fn get(&self, key: &[u8]) -> BackendResult<GetResult> {
        self.calls.lock().unwrap().push(Call::Get(key.to_vec()));
        Ok(self.get_result)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BackendResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Put(key.to_vec(), value.to_vec()));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> BackendResult<()> {
        self.calls.lock().unwrap().push(Call::Delete(key.to_vec()));
        Ok(())
    }

    fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()> {
        let mutations = items
            .iter()
            .map(|item| match item {
                BatchItem::Put { key, value } => Mutation::Put(key.to_vec(), value.to_vec()),
                BatchItem::Delete { key } => Mutation::Delete(key.to_vec()),
            })
            .collect();
        self.calls.lock().unwrap().push(Call::Batch(mutations));
        Ok(())
    }

    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        let expected_value_length = match request.validation {
            ScanValidation::Timed {
                expected_value_length,
            } => expected_value_length,
            ScanValidation::Full { expected } => {
                expected.first().map_or(0, |record| record.value.len())
            }
        };
        self.calls.lock().unwrap().push(Call::Scan {
            start: request.start.to_vec(),
            limit: request.limit,
            expected_value_length,
        });
        Ok(self.scan_result.unwrap_or(ScanResult {
            record_count: request.limit,
            value_bytes: request.limit * expected_value_length,
        }))
    }
}

#[test]
fn all_six_workloads_issue_exact_calls_for_one_and_ten_threads() {
    let config = smoke_config();
    for workload in Workload::ALL {
        let trace = Trace::generate(&config, workload, 2).unwrap();
        let expected = expected_calls(&config, &trace);
        for thread_count in [1, 10] {
            let backend = Arc::new(RecordingBackend::default());
            let shared: Arc<dyn BenchBackend> = backend.clone();
            let run = WorkloadRun::new(&config, BackendKind::RustKv, shared, &trace, thread_count);
            assert_eq!(run.mode(), BenchMode::Smoke);
            assert_eq!(run.workload(), workload);
            assert_eq!(run.thread_count(), thread_count);

            let result = run.execute();
            assert!(
                result.is_valid(),
                "unexpected {workload} result: {result:?}"
            );
            assert_eq!(result.completed_ops, trace.request_count() as u64);
            assert_eq!(
                result.completed_records,
                result.completed_ops * trace.records_per_operation()
            );
            assert_eq!(
                result
                    .metrics
                    .as_ref()
                    .unwrap()
                    .records_per_second()
                    .is_some(),
                matches!(
                    workload,
                    Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
                )
            );

            let mut actual = backend.calls.lock().unwrap().clone();
            if thread_count == 1 {
                assert_eq!(actual, expected, "one thread must preserve Trace order");
            } else {
                actual.sort_unstable();
                let mut expected_set = expected.clone();
                expected_set.sort_unstable();
                assert_eq!(
                    actual, expected_set,
                    "ten threads must preserve the request set"
                );
            }
        }
    }
}

#[test]
fn get_not_found_and_wrong_value_length_fail_without_retry() {
    let config = smoke_config();
    let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
    for (get_result, expected_error) in [
        (
            GetResult {
                found: false,
                value_length: 0,
            },
            ExpectedGetError::NotFound,
        ),
        (
            GetResult {
                found: true,
                value_length: config.value_length() - 1,
            },
            ExpectedGetError::WrongLength,
        ),
    ] {
        let backend = Arc::new(RecordingBackend {
            get_result,
            ..RecordingBackend::default()
        });
        let shared: Arc<dyn BenchBackend> = backend.clone();
        let result = WorkloadRun::new(&config, BackendKind::RustKv, shared, &trace, 1).execute();
        assert!(!result.is_valid());
        assert_eq!(result.completed_ops, 0);
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
        match expected_error {
            ExpectedGetError::NotFound => assert!(matches!(
                result.first_error,
                Some(RunError::Workload(WorkloadError::GetNotFound { .. }))
            )),
            ExpectedGetError::WrongLength => assert!(matches!(
                result.first_error,
                Some(RunError::Workload(
                    WorkloadError::GetValueLengthMismatch { .. }
                ))
            )),
        }
    }
}

#[test]
fn range_wrong_record_or_value_byte_counts_fail_without_retry() {
    let config = smoke_config();
    let trace = Trace::generate(&config, Workload::RangeScan, 0).unwrap();
    for (scan_result, record_mismatch) in [
        (
            ScanResult {
                record_count: 99,
                value_bytes: 99 * config.value_length(),
            },
            true,
        ),
        (
            ScanResult {
                record_count: 100,
                value_bytes: 100 * config.value_length() - 1,
            },
            false,
        ),
    ] {
        let backend = Arc::new(RecordingBackend {
            scan_result: Some(scan_result),
            ..RecordingBackend::default()
        });
        let shared: Arc<dyn BenchBackend> = backend.clone();
        let result = WorkloadRun::new(&config, BackendKind::LevelDb, shared, &trace, 1).execute();
        assert!(!result.is_valid());
        assert_eq!(result.completed_ops, 0);
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
        if record_mismatch {
            assert!(matches!(
                result.first_error,
                Some(RunError::Workload(
                    WorkloadError::ScanRecordCountMismatch { .. }
                ))
            ));
        } else {
            assert!(matches!(
                result.first_error,
                Some(RunError::Workload(
                    WorkloadError::ScanValueBytesMismatch { .. }
                ))
            ));
        }
    }
}

#[test]
fn trace_and_execution_configuration_must_describe_the_same_fixed_work() {
    let trace_config = smoke_config();
    let different_domain = BenchConfig::test_only(1_000, 100, 100, 20, 10);

    // Both configs deliberately have the same request counts and records/op.
    // Count-only validation would accept these read Traces even though their
    // random domains differ.
    for workload in [Workload::RandomGet, Workload::RangeScan] {
        let trace = Trace::generate(&different_domain, workload, 0).unwrap();
        assert_configuration_mismatch_fails_before_backend(&trace_config, &trace);
    }

    let range_trace = Trace::generate(&trace_config, Workload::RangeScan, 0).unwrap();
    let different_range_width = BenchConfig::test_only(200, 50, 100, 20, 10);
    assert_configuration_mismatch_fails_before_backend(&different_range_width, &range_trace);
}

#[derive(Clone, Copy)]
enum ExpectedGetError {
    NotFound,
    WrongLength,
}

fn smoke_config() -> BenchConfig {
    BenchConfig::test_only(200, 100, 100, 20, 10)
}

fn assert_configuration_mismatch_fails_before_backend(config: &BenchConfig, trace: &Trace) {
    let backend = Arc::new(RecordingBackend::default());
    let shared: Arc<dyn BenchBackend> = backend.clone();
    let result = WorkloadRun::new(config, BackendKind::RustKv, shared, trace, 1).execute();
    assert!(!result.is_valid());
    assert_eq!(result.completed_ops, 0);
    assert_eq!(result.wall_time, std::time::Duration::ZERO);
    assert!(result.thread_summaries.is_empty());
    assert!(backend.calls.lock().unwrap().is_empty());
    assert!(matches!(
        result.first_error,
        Some(RunError::Workload(
            WorkloadError::TraceConfigurationMismatch { .. }
        ))
    ));
}

fn expected_calls(config: &BenchConfig, trace: &Trace) -> Vec<Call> {
    let value = fixed_value(config);
    trace
        .requests()
        .map(|request| match trace.workload() {
            Workload::RandomGet => Call::Get(encode_key(config, request[0]).unwrap().to_vec()),
            Workload::RangeScan => Call::Scan {
                start: encode_key(config, request[0]).unwrap().to_vec(),
                limit: config.range_length() as usize,
                expected_value_length: config.value_length(),
            },
            Workload::SinglePut => Call::Put(
                encode_key(config, request[0]).unwrap().to_vec(),
                value.clone(),
            ),
            Workload::BatchPut => Call::Batch(
                request
                    .iter()
                    .map(|id| {
                        Mutation::Put(encode_key(config, *id).unwrap().to_vec(), value.clone())
                    })
                    .collect(),
            ),
            Workload::SingleDelete => {
                Call::Delete(encode_key(config, request[0]).unwrap().to_vec())
            }
            Workload::BatchDelete => Call::Batch(
                request
                    .iter()
                    .map(|id| Mutation::Delete(encode_key(config, *id).unwrap().to_vec()))
                    .collect(),
            ),
        })
        .collect()
}
