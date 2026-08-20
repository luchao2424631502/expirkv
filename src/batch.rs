//! Atomic write batches and batch operations.

use crate::{Result, RetryAdvice, StorageError, StorageErrorKind};

const MAX_KEY_VALUE_SIZE: usize = 60_000;

pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        validate_key_value(key, value)?;
        validate_next_operation_count(self.operations.len())?;

        let key = try_clone_key(key)?;
        let value = try_clone_value(value)?;
        let operation = BatchOperation::Put { key, value };

        reserve_operation(&mut self.operations)?;
        self.operations.push(operation);
        Ok(())
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        validate_key(key)?;
        validate_next_operation_count(self.operations.len())?;

        let key = try_clone_key(key)?;
        let operation = BatchOperation::Delete { key };

        reserve_operation(&mut self.operations)?;
        self.operations.push(operation);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.operations.clear();
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_VALUE_SIZE {
        return Err(invalid_argument());
    }
    u16::try_from(key.len()).map_err(|_| capacity_exceeded())?;
    Ok(())
}

fn validate_key_value(key: &[u8], value: &[u8]) -> Result<()> {
    validate_key(key)?;
    u16::try_from(value.len()).map_err(|_| capacity_exceeded())?;
    let total = key
        .len()
        .checked_add(value.len())
        .ok_or_else(capacity_exceeded)?;
    if total > MAX_KEY_VALUE_SIZE {
        return Err(invalid_argument());
    }
    Ok(())
}

fn validate_next_operation_count(current: usize) -> Result<()> {
    let next = current.checked_add(1).ok_or_else(capacity_exceeded)?;
    u32::try_from(next).map_err(|_| capacity_exceeded())?;
    Ok(())
}

fn try_clone_key(bytes: &[u8]) -> Result<Vec<u8>> {
    #[cfg(test)]
    if allocation_failure::should_fail(allocation_failure::Site::Key) {
        return Err(resource_exhausted());
    }
    try_clone_bytes(bytes)
}

fn try_clone_value(bytes: &[u8]) -> Result<Vec<u8>> {
    #[cfg(test)]
    if allocation_failure::should_fail(allocation_failure::Site::Value) {
        return Err(resource_exhausted());
    }
    try_clone_bytes(bytes)
}

fn try_clone_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource_exhausted())?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn reserve_operation(operations: &mut Vec<BatchOperation>) -> Result<()> {
    #[cfg(test)]
    if allocation_failure::should_fail(allocation_failure::Site::Operations) {
        return Err(resource_exhausted());
    }
    operations.try_reserve(1).map_err(|_| resource_exhausted())
}

fn invalid_argument() -> StorageError {
    StorageError::invalid_batch(
        StorageErrorKind::InvalidArgument,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn capacity_exceeded() -> StorageError {
    StorageError::invalid_batch(
        StorageErrorKind::CapacityExceeded,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn resource_exhausted() -> StorageError {
    StorageError::invalid_batch(
        StorageErrorKind::ResourceExhausted,
        RetryAdvice::RetrySameInstance,
    )
}

#[cfg(test)]
mod allocation_failure {
    use std::cell::Cell;

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub(super) enum Site {
        Key,
        Value,
        Operations,
    }

    thread_local! {
        static NEXT_FAILURE: Cell<Option<Site>> = const { Cell::new(None) };
    }

    pub(super) fn inject(site: Site) {
        NEXT_FAILURE.with(|next| {
            assert!(next.replace(Some(site)).is_none());
        });
    }

    pub(super) fn should_fail(site: Site) -> bool {
        NEXT_FAILURE.with(|next| {
            if next.get() == Some(site) {
                next.set(None);
                true
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::allocation_failure::{self, Site};
    use super::*;
    use crate::{InstanceState, Operation, ProtocolStage, WriteOutcome};

    #[test]
    fn preserves_operation_order_and_owns_input_bytes() {
        let mut batch = WriteBatch::new();
        let mut key = vec![1, 2, 3];
        let mut value = vec![4, 5, 6];

        batch.put(&key, &value).unwrap();
        batch.delete([7, 8]).unwrap();
        key.fill(9);
        value.fill(10);

        assert_eq!(
            batch.operations,
            vec![
                BatchOperation::Put {
                    key: vec![1, 2, 3],
                    value: vec![4, 5, 6],
                },
                BatchOperation::Delete { key: vec![7, 8] },
            ]
        );
    }

    #[test]
    fn allocation_failures_leave_batch_byte_for_byte_unchanged() {
        for site in [Site::Key, Site::Value, Site::Operations] {
            let mut batch = WriteBatch::new();
            batch.put(b"existing-key", b"existing-value").unwrap();
            let before = batch.operations.clone();

            allocation_failure::inject(site);
            let error = batch.put(b"new-key", b"new-value").unwrap_err();

            assert_eq!(error.schema_version, 1);
            assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
            assert_eq!(error.operation, Operation::WriteBatch);
            assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
            assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
            assert_eq!(error.instance_state, None::<InstanceState>);
            assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
            assert_eq!(batch.operations, before);

            batch.put(b"new-key", b"new-value").unwrap();
            assert_eq!(batch.len(), 2);
        }
    }
}
