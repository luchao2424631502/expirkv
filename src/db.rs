//! Database lifecycle, public API implementation, and component assembly.

use std::path::Path;
use std::sync::Arc;

use crate::stats::StatsState;
use crate::{
    DbIterator, DbStats, InstanceState, KeyRange, Operation, Options, ProtocolStage, RangeCursor,
    ReadOptions, Result, Snapshot, StorageError, WriteBatch, WriteOptions,
};

pub(crate) struct DbInner {
    stats: StatsState,
}

impl DbInner {
    pub(crate) fn instance_state(&self) -> InstanceState {
        self.stats.snapshot().instance_state
    }

    pub(crate) fn unsupported_error(
        &self,
        operation: Operation,
        protocol_stage: ProtocolStage,
    ) -> StorageError {
        StorageError::unsupported(operation, protocol_stage, Some(self.instance_state()))
    }
}

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

impl Db {
    pub fn open(options: &Options, path: impl AsRef<Path>) -> Result<Self> {
        let _ = (options, path.as_ref());
        Err(StorageError::unsupported(
            Operation::Open,
            ProtocolStage::Lifecycle,
            None,
        ))
    }

    pub fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        let _ = (options, key, value);
        Err(self
            .inner
            .unsupported_error(Operation::Put, ProtocolStage::Preflight))
    }

    pub fn get(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let _ = (options, key);
        Err(self
            .inner
            .unsupported_error(Operation::Get, ProtocolStage::Read))
    }

    pub fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()> {
        let _ = (options, key);
        Err(self
            .inner
            .unsupported_error(Operation::Delete, ProtocolStage::Preflight))
    }

    pub fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<()> {
        let _ = (options, batch);
        Err(self
            .inner
            .unsupported_error(Operation::WriteBatch, ProtocolStage::Preflight))
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        Err(self
            .inner
            .unsupported_error(Operation::Snapshot, ProtocolStage::Read))
    }

    pub fn iter(&self, options: &ReadOptions<'_>) -> Result<DbIterator> {
        let _ = options;
        Err(self
            .inner
            .unsupported_error(Operation::Iterator, ProtocolStage::Read))
    }

    pub fn range(
        &self,
        options: &ReadOptions<'_>,
        range: KeyRange<'_>,
        limit: usize,
    ) -> Result<RangeCursor> {
        let _ = (options, range, limit);
        Err(self
            .inner
            .unsupported_error(Operation::Range, ProtocolStage::Read))
    }

    pub fn stats(&self) -> DbStats {
        self.inner.stats.snapshot()
    }

    pub fn destroy(path: impl AsRef<Path>, options: &Options) -> Result<()> {
        let _ = (path.as_ref(), options);
        Err(StorageError::unsupported(
            Operation::Destroy,
            ProtocolStage::Lifecycle,
            None,
        ))
    }
}
