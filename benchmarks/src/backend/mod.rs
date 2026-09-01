//! Backend-independent operations used by every benchmark workload.

mod leveldb;
mod leveldb_ffi;
mod rustkv;

use std::error::Error;
use std::fmt;

pub use leveldb::LevelDbBackend;
pub use leveldb_ffi::linked_leveldb_version;
pub use rustkv::RustKvBackend;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendKind {
    RustKv,
    LevelDb,
}

impl BackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RustKv => "rustkv",
            Self::LevelDb => "leveldb",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendOperation {
    Open,
    Get,
    Put,
    Delete,
    WriteBatch,
    IteratorScan,
}

impl BackendOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::WriteBatch => "write_batch",
            Self::IteratorScan => "iterator_scan",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendError {
    backend: BackendKind,
    operation: BackendOperation,
    source_text: String,
}

impl BackendError {
    pub(crate) fn new(
        backend: BackendKind,
        operation: BackendOperation,
        source_text: impl Into<String>,
    ) -> Self {
        Self {
            backend,
            operation,
            source_text: source_text.into(),
        }
    }

    pub const fn backend(&self) -> BackendKind {
        self.backend
    }

    pub const fn operation(&self) -> BackendOperation {
        self.operation
    }

    pub fn source_text(&self) -> &str {
        &self.source_text
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} {} failed: {}",
            self.backend.as_str(),
            self.operation.as_str(),
            self.source_text
        )
    }
}

impl Error for BackendError {}

pub type BackendResult<T> = Result<T, BackendError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GetResult {
    pub found: bool,
    pub value_length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchItem<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpectedRecord<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanValidation<'a> {
    Timed { expected_value_length: usize },
    Full { expected: &'a [ExpectedRecord<'a>] },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanRequest<'a> {
    pub start: &'a [u8],
    pub limit: usize,
    pub validation: ScanValidation<'a>,
}

impl<'a> ScanRequest<'a> {
    pub const fn timed(start: &'a [u8], limit: usize, expected_value_length: usize) -> Self {
        Self {
            start,
            limit,
            validation: ScanValidation::Timed {
                expected_value_length,
            },
        }
    }

    pub const fn full(start: &'a [u8], limit: usize, expected: &'a [ExpectedRecord<'a>]) -> Self {
        Self {
            start,
            limit,
            validation: ScanValidation::Full { expected },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanResult {
    pub record_count: usize,
    pub value_bytes: usize,
}

/// The complete operation boundary shared by RustKV and LevelDB.
pub trait BenchBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> BackendResult<GetResult>;
    fn put(&self, key: &[u8], value: &[u8]) -> BackendResult<()>;
    fn delete(&self, key: &[u8]) -> BackendResult<()>;
    fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()>;
    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult>;
}
