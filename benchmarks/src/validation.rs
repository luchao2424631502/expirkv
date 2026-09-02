//! Untimed full-dataset validation shared by direct Load -> Run initial,
//! prewarm, and final checks, plus the retained historical template tests.

use std::error::Error;
use std::fmt;

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
            validate_full_dataset(backend, config)
        }
        Workload::SingleDelete | Workload::BatchDelete => validate_empty_dataset(backend),
    }
}
