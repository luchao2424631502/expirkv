//! Durable-frontier serialization and atomic-batch construction.

use std::sync::{Mutex, MutexGuard};

use crate::index::{
    DURABLE_FRONTIER_KEY, IndexAtomicBatch, IndexMutation, InternalIndexError, InternalIndexSpace,
};
use crate::{Operation, Result, RetryAdvice, StorageError, StorageErrorKind};

use super::descriptor::DurableFrontier;

pub(crate) struct DurabilityCoordinator {
    frontier_mutex: Mutex<()>,
}

impl DurabilityCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            frontier_mutex: Mutex::new(()),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        self.frontier_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for DurabilityCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn add_frontier_to_batch(
    batch: &mut IndexAtomicBatch,
    frontier: DurableFrontier,
    operation: Operation,
) -> Result<()> {
    let encoded = frontier
        .encode()
        .map_err(|error| remap_construction(error.kind, operation))?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy(DURABLE_FRONTIER_KEY, operation)?,
            value: try_copy(&encoded, operation)?,
        })
        .map_err(|error| map_index_error(error, operation))
}

pub(crate) fn frontier_only_batch(
    frontier: DurableFrontier,
    operation: Operation,
) -> Result<IndexAtomicBatch> {
    let mut batch = IndexAtomicBatch::try_with_capacity(1)
        .map_err(|error| map_index_error(error, operation))?;
    add_frontier_to_batch(&mut batch, frontier, operation)?;
    Ok(batch)
}

fn try_copy(bytes: &[u8], operation: Operation) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource_exhausted(operation))?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn map_index_error(error: InternalIndexError, operation: Operation) -> StorageError {
    remap_construction(error.kind, operation)
}

fn remap_construction(kind: StorageErrorKind, operation: Operation) -> StorageError {
    let retry = if kind == StorageErrorKind::ResourceExhausted {
        RetryAdvice::RetrySameInstance
    } else {
        RetryAdvice::FixRequestAndRetrySameInstance
    };
    StorageError::write_preflight(kind, operation, retry)
}

fn resource_exhausted(operation: Operation) -> StorageError {
    StorageError::write_preflight(
        StorageErrorKind::ResourceExhausted,
        operation,
        RetryAdvice::RetrySameInstance,
    )
}
