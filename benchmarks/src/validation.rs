//! Untimed full-dataset validation shared by direct Load -> Run initial,
//! prewarm, and final checks, plus the retained historical template tests.

use std::error::Error;
use std::fmt;
use std::thread;

use crate::{
    BackendError, BenchBackend, BenchConfig, ExpectedRecord, KeyCodecError, ScanRequest, Workload,
    encode_key, fixed_value,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    RecordCountDoesNotFitUsize { count: u64 },
    ScanLimitOverflow { count: usize },
    ValueByteCountOverflow,
    KeyEncoding { id: u64, source: KeyCodecError },
    Backend(BackendError),
    ResultCountMismatch { expected: usize, actual: usize },
    ResultValueBytesMismatch { expected: usize, actual: usize },
    WorkerPanicked { worker_index: usize },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "benchmark validation failed: {self:?}")
    }
}

impl Error for ValidationError {}

impl From<BackendError> for ValidationError {
    fn from(error: BackendError) -> Self {
        Self::Backend(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidationSummary {
    pub record_count: usize,
    pub value_bytes: usize,
}

/// Performs one complete Iterator scan from the minimum key and asks the B2
/// Backend to compare every key and every Value byte with the frozen dataset.
pub fn validate_full_dataset(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
) -> Result<ValidationSummary, ValidationError> {
    let count = usize::try_from(config.record_count()).map_err(|_| {
        ValidationError::RecordCountDoesNotFitUsize {
            count: config.record_count(),
        }
    })?;
    let scan_limit = count
        .checked_add(1)
        .ok_or(ValidationError::ScanLimitOverflow { count })?;
    let value = fixed_value(config);
    let expected_value_bytes = count
        .checked_mul(value.len())
        .ok_or(ValidationError::ValueByteCountOverflow)?;
    let keys = (0..config.record_count())
        .map(|id| {
            encode_key(config, id).map_err(|source| ValidationError::KeyEncoding { id, source })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected = keys
        .iter()
        .map(|key| ExpectedRecord { key, value: &value })
        .collect::<Vec<_>>();
    let result = backend.iterator_scan(ScanRequest::full(b"", scan_limit, &expected))?;
    if result.record_count != count {
        return Err(ValidationError::ResultCountMismatch {
            expected: count,
            actual: result.record_count,
        });
    }
    if result.value_bytes != expected_value_bytes {
        return Err(ValidationError::ResultValueBytesMismatch {
            expected: expected_value_bytes,
            actual: result.value_bytes,
        });
    }
    Ok(ValidationSummary {
        record_count: result.record_count,
        value_bytes: result.value_bytes,
    })
}

pub fn prewarm_full_dataset(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
) -> Result<ValidationSummary, ValidationError> {
    validate_full_dataset(backend, config)
}

pub fn validate_empty_dataset(
    backend: &dyn BenchBackend,
) -> Result<ValidationSummary, ValidationError> {
    let result = backend.iterator_scan(ScanRequest::full(b"", 1, &[]))?;
    if result.record_count != 0 {
        return Err(ValidationError::ResultCountMismatch {
            expected: 0,
            actual: result.record_count,
        });
    }
    if result.value_bytes != 0 {
        return Err(ValidationError::ResultValueBytesMismatch {
            expected: 0,
            actual: result.value_bytes,
        });
    }
    Ok(ValidationSummary {
        record_count: 0,
        value_bytes: 0,
    })
}

pub fn validate_final_dataset(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
    workload: Workload,
) -> Result<ValidationSummary, ValidationError> {
    match workload {
        Workload::RandomGet | Workload::RangeScan | Workload::SinglePut | Workload::BatchPut => {
            validate_full_dataset_parallel(backend, config)
        }
        Workload::SingleDelete | Workload::BatchDelete => validate_empty_dataset(backend),
    }
}

fn validate_full_dataset_parallel(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
) -> Result<ValidationSummary, ValidationError> {
    let count = usize::try_from(config.record_count()).map_err(|_| {
        ValidationError::RecordCountDoesNotFitUsize {
            count: config.record_count(),
        }
    })?;
    let worker_count = thread::available_parallelism()
        .map_or(1, |parallelism| parallelism.get())
        .min(count);
    let value = fixed_value(config);
    let expected_value_bytes = count
        .checked_mul(value.len())
        .ok_or(ValidationError::ValueByteCountOverflow)?;
    let validation = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            workers.push(scope.spawn({
                let value = &value;
                move || validate_full_partition(backend, config, value, worker_index, worker_count)
            }));
        }

        let mut first_error = None;
        for (worker_index, worker) in workers.into_iter().enumerate() {
            let result = match worker.join() {
                Ok(result) => result,
                Err(_) => Err(ValidationError::WorkerPanicked { worker_index }),
            };
            if first_error.is_none()
                && let Err(error) = result
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    });
    validation?;

    Ok(ValidationSummary {
        record_count: count,
        value_bytes: expected_value_bytes,
    })
}

fn validate_full_partition(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
    value: &[u8],
    worker_index: usize,
    worker_count: usize,
) -> Result<(), ValidationError> {
    let worker_count_u64 =
        u64::try_from(worker_count).expect("available parallelism and record count fit into u64");
    let worker_index_u64 = u64::try_from(worker_index)
        .expect("worker index derived from available parallelism fits into u64");
    let record_count = config.record_count();
    let records_per_worker = record_count / worker_count_u64;
    let remainder = record_count % worker_count_u64;
    let own_start = worker_index_u64
        .checked_mul(records_per_worker)
        .and_then(|start| start.checked_add(worker_index_u64.min(remainder)))
        .expect("partition arithmetic is bounded by the validated record count");
    let own_length = records_per_worker + u64::from(worker_index_u64 < remainder);
    let own_end = own_start
        .checked_add(own_length)
        .expect("partition arithmetic is bounded by the validated record count");
    let validation_end = if own_end < record_count {
        own_end + 1
    } else {
        own_end
    };

    let keys = (own_start..validation_end)
        .map(|id| {
            encode_key(config, id).map_err(|source| ValidationError::KeyEncoding { id, source })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected = keys
        .iter()
        .map(|key| ExpectedRecord { key, value })
        .collect::<Vec<_>>();
    let start = if own_start == 0 {
        b"".as_slice()
    } else {
        keys[0].as_slice()
    };
    let scan_limit = expected
        .len()
        .checked_add(usize::from(own_end == record_count))
        .ok_or(ValidationError::ScanLimitOverflow {
            count: expected.len(),
        })?;
    let expected_value_bytes = expected
        .len()
        .checked_mul(value.len())
        .ok_or(ValidationError::ValueByteCountOverflow)?;
    let result = backend.iterator_scan(ScanRequest::full(start, scan_limit, &expected))?;
    if result.record_count != expected.len() {
        return Err(ValidationError::ResultCountMismatch {
            expected: expected.len(),
            actual: result.record_count,
        });
    }
    if result.value_bytes != expected_value_bytes {
        return Err(ValidationError::ResultValueBytesMismatch {
            expected: expected_value_bytes,
            actual: result.value_bytes,
        });
    }
    Ok(())
}
