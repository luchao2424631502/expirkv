#![allow(dead_code, unused_imports)]

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition as PublicVLogPosition};
#[path = "../src/vlog/mod.rs"]
mod vlog;
pub(crate) use vlog::format as vlog_format;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/runtime/mod.rs"]
mod runtime;

use batch::WriteBatch;
use commit::{
    AllocationFailureSite, DescriptorAllocationFailureSite, TxUuidSource,
    inject_allocation_failure_for_test, inject_descriptor_allocation_failure_for_test,
    preflight_batch, preflight_batch_with_operation_limit_for_test, preflight_delete,
    preflight_put, prepare_commit, validate_operation_count_for_test,
};
use index::{
    IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexEntry,
    InternalIndexSpace, InternalKeyRange, inject_index_batch_allocation_failure_for_test,
};
use vlog_format::{
    PrepareAllocationFailureSite, VLogGeometry, VLogPosition, ValuePointer,
    inject_prepare_allocation_failure_for_test,
};

#[derive(Clone, Debug)]
enum UserReadBehavior {
    Absent,
    Value(Vec<u8>),
    Error {
        kind: StorageErrorKind,
        os_code: Option<i32>,
    },
}

impl Default for UserReadBehavior {
    fn default() -> Self {
        Self::Absent
    }
}

#[derive(Default)]
struct CountingBackend {
    user_reads: AtomicUsize,
    commits: AtomicUsize,
    user_read_behavior: UserReadBehavior,
}

impl CountingBackend {
    fn with_user_value(encoded_pointer: Vec<u8>) -> Self {
        Self {
            user_read_behavior: UserReadBehavior::Value(encoded_pointer),
            ..Self::default()
        }
    }

    fn with_user_error(kind: StorageErrorKind, os_code: Option<i32>) -> Self {
        Self {
            user_read_behavior: UserReadBehavior::Error { kind, os_code },
            ..Self::default()
        }
    }
}

impl IndexBackend for CountingBackend {
    type Snapshot = ();
    type UserIterator = std::vec::IntoIter<Result<IndexEntry>>;
    type InternalIterator = std::vec::IntoIter<Result<IndexEntry>>;

    fn commit_atomic(
        &self,
        _batch: IndexAtomicBatch,
        _mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_user(&self, _key: &[u8], _snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        self.user_reads.fetch_add(1, Ordering::SeqCst);
        match &self.user_read_behavior {
            UserReadBehavior::Absent => Ok(None),
            UserReadBehavior::Value(value) => Ok(Some(value.clone())),
            UserReadBehavior::Error { kind, os_code } => {
                let mut error = StorageError::codec_error(
                    *kind,
                    Operation::Get,
                    ProtocolStage::Read,
                    None,
                    RetryAdvice::DoNotRetry,
                );
                error.os_code = *os_code;
                Err(error)
            }
        }
    }

    fn get_internal(&self, _space: InternalIndexSpace, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn scan_internal(
        &self,
        _space: InternalIndexSpace,
        _range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        Ok(Vec::new().into_iter())
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(())
    }

    fn iter_user(&self, _snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        Ok(Vec::new().into_iter())
    }
}

#[derive(Default)]
struct CountingUuidSource {
    calls: usize,
    fail: bool,
}

impl TxUuidSource for CountingUuidSource {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        self.calls += 1;
        if self.fail {
            Err(io::Error::from_raw_os_error(5))
        } else {
            output.fill(0x3a);
            Ok(())
        }
    }
}

fn assert_preflight_error(error: &StorageError, kind: StorageErrorKind, operation: Operation) {
    assert_eq!(error.kind, kind);
    assert_eq!(error.operation, operation);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(InstanceState::Healthy));
    assert_eq!(
        error.retry_advice,
        RetryAdvice::FixRequestAndRetrySameInstance
    );
}

fn assert_preflight_error_with_state_and_retry(
    error: &StorageError,
    kind: StorageErrorKind,
    operation: Operation,
    instance_state: InstanceState,
    retry_advice: RetryAdvice,
) {
    assert_eq!(error.kind, kind);
    assert_eq!(error.operation, operation);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(instance_state));
    assert_eq!(error.retry_advice, retry_advice);
}

fn prepare_valid_batch<'a>(batch: &'a WriteBatch) -> commit::ValidatedWrite<'a> {
    preflight_batch(batch, false).unwrap()
}

#[test]
fn put_delete_and_batch_preflight_enforce_frozen_size_rules() {
    let empty_key = preflight_put(b"", b"value", false).unwrap_err();
    assert_preflight_error(
        &empty_key,
        StorageErrorKind::InvalidArgument,
        Operation::Put,
    );

    let oversized = vec![0x5a; 60_000];
    let too_large = preflight_put(b"k", &oversized, false).unwrap_err();
    assert_preflight_error(
        &too_large,
        StorageErrorKind::InvalidArgument,
        Operation::Put,
    );

    let exact_limit = vec![0xa5; 59_999];
    assert!(preflight_put(b"k", &exact_limit, false).is_ok());
    let maximum_key = vec![0xff; 60_000];
    assert!(preflight_delete(&maximum_key, false).is_ok());

    let empty_value = preflight_put(b"key", b"", false).unwrap();
    assert_eq!(empty_value.logical_op_count(), 1);
    assert_eq!(empty_value.distinct_key_count(), 1);

    let delete_empty = preflight_delete(b"", false).unwrap_err();
    assert_preflight_error(
        &delete_empty,
        StorageErrorKind::InvalidArgument,
        Operation::Delete,
    );

    let mut batch = WriteBatch::new();
    batch.put(b"valid", b"value").unwrap();
    batch.push_delete_unchecked_for_test(Vec::new());
    let invalid_member = preflight_batch(&batch, false).unwrap_err();
    assert_preflight_error(
        &invalid_member,
        StorageErrorKind::InvalidArgument,
        Operation::WriteBatch,
    );
    assert_eq!(
        batch.len(),
        2,
        "defensive preflight must not mutate the batch"
    );
}

#[test]
fn operation_count_checks_cover_limit_conversion_and_arithmetic_overflow() {
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1").unwrap();
    batch.delete(b"b").unwrap();

    let count_error = preflight_batch_with_operation_limit_for_test(&batch, false, 1).unwrap_err();
    assert_preflight_error(
        &count_error,
        StorageErrorKind::CapacityExceeded,
        Operation::WriteBatch,
    );
    assert_eq!(
        count_error.retry_advice,
        RetryAdvice::FixRequestAndRetrySameInstance
    );

    if let Ok(count_above_u32) = usize::try_from(u64::from(u32::MAX) + 1) {
        let conversion_error =
            validate_operation_count_for_test(count_above_u32, usize::MAX).unwrap_err();
        assert_preflight_error(
            &conversion_error,
            StorageErrorKind::CapacityExceeded,
            Operation::WriteBatch,
        );
    }

    let arithmetic_error = validate_operation_count_for_test(usize::MAX, usize::MAX).unwrap_err();
    assert_preflight_error(
        &arithmetic_error,
        StorageErrorKind::CapacityExceeded,
        Operation::WriteBatch,
    );
}

#[test]
fn every_allocation_failure_site_returns_not_committed_before_vlog_or_index_commit() {
    inject_allocation_failure_for_test(AllocationFailureSite::Operations);
    let put_error = preflight_put(b"put", b"value", false).unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &put_error,
        StorageErrorKind::ResourceExhausted,
        Operation::Put,
        InstanceState::Healthy,
        RetryAdvice::RetrySameInstance,
    );

    inject_allocation_failure_for_test(AllocationFailureSite::Operations);
    let delete_error = preflight_delete(b"delete", false).unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &delete_error,
        StorageErrorKind::ResourceExhausted,
        Operation::Delete,
        InstanceState::Healthy,
        RetryAdvice::RetrySameInstance,
    );

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1").unwrap();
    batch.delete(b"b").unwrap();

    for site in [
        AllocationFailureSite::Operations,
        AllocationFailureSite::DistinctKeys,
        AllocationFailureSite::OperationOrdinals,
        AllocationFailureSite::OrdinalMap,
    ] {
        let backend = CountingBackend::default();
        let uuid = CountingUuidSource::default();
        inject_allocation_failure_for_test(site);
        let error = preflight_batch(&batch, false).unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            StorageErrorKind::ResourceExhausted,
            Operation::WriteBatch,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        );
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), 0);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, 0);
    }

    for (site, expected_reads, expected_uuid_calls) in [
        (AllocationFailureSite::BeforeStates, 0, 0),
        (AllocationFailureSite::LogicalOperations, 2, 1),
        (AllocationFailureSite::KeyPlans, 2, 1),
        (AllocationFailureSite::KeyPlanUserKey, 2, 1),
        (AllocationFailureSite::DescriptorMutations, 2, 1),
        (AllocationFailureSite::DescriptorUserKey, 2, 1),
        (AllocationFailureSite::IndexBatch, 2, 1),
        (AllocationFailureSite::IndexUserKey, 2, 1),
        (AllocationFailureSite::IndexPointer, 2, 1),
        (AllocationFailureSite::IndexMetaKey, 2, 1),
        (AllocationFailureSite::IndexMetaValue, 2, 1),
        (AllocationFailureSite::IndexMutationKey, 2, 1),
        (AllocationFailureSite::IndexHeadKey, 2, 1),
        (AllocationFailureSite::IndexHeadValue, 2, 1),
    ] {
        let write = preflight_batch(&batch, false).unwrap();
        let backend = CountingBackend::default();
        let mut uuid = CountingUuidSource::default();
        inject_allocation_failure_for_test(site);
        let error = prepare_commit(
            &write,
            [7; 16],
            0,
            VLogPosition {
                file_id: 0,
                offset: 0,
            },
            VLogGeometry::PRODUCTION,
            &backend,
            &mut uuid,
        )
        .unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            StorageErrorKind::ResourceExhausted,
            Operation::WriteBatch,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        );
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), expected_reads);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, expected_uuid_calls);
    }

    for site in [
        PrepareAllocationFailureSite::DistinctKeys,
        PrepareAllocationFailureSite::RecordLengths,
        PrepareAllocationFailureSite::PlacementPreludes,
        PrepareAllocationFailureSite::Placements,
        PrepareAllocationFailureSite::Chunks,
        PrepareAllocationFailureSite::ValuePointers,
        PrepareAllocationFailureSite::RecordBytes,
        PrepareAllocationFailureSite::StructuralBytes,
    ] {
        let write = preflight_batch(&batch, false).unwrap();
        let backend = CountingBackend::default();
        let mut uuid = CountingUuidSource::default();
        inject_prepare_allocation_failure_for_test(site);
        let error = prepare_commit(
            &write,
            [7; 16],
            0,
            VLogPosition {
                file_id: 0,
                offset: 0,
            },
            VLogGeometry::PRODUCTION,
            &backend,
            &mut uuid,
        )
        .unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            StorageErrorKind::ResourceExhausted,
            Operation::WriteBatch,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        );
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), 2);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, 1);
    }

    for site in [
        DescriptorAllocationFailureSite::SeenKeys,
        DescriptorAllocationFailureSite::EncodedMutations,
        DescriptorAllocationFailureSite::MutationValue,
    ] {
        let write = preflight_batch(&batch, false).unwrap();
        let backend = CountingBackend::default();
        let mut uuid = CountingUuidSource::default();
        inject_descriptor_allocation_failure_for_test(site);
        let error = prepare_commit(
            &write,
            [7; 16],
            0,
            VLogPosition {
                file_id: 0,
                offset: 0,
            },
            VLogGeometry::PRODUCTION,
            &backend,
            &mut uuid,
        )
        .unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            StorageErrorKind::ResourceExhausted,
            Operation::WriteBatch,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        );
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), 2);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, 1);
    }

    let write = preflight_batch(&batch, false).unwrap();
    let backend = CountingBackend::default();
    let mut uuid = CountingUuidSource::default();
    inject_index_batch_allocation_failure_for_test();
    let error = prepare_commit(
        &write,
        [7; 16],
        0,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &error,
        StorageErrorKind::ResourceExhausted,
        Operation::WriteBatch,
        InstanceState::Healthy,
        RetryAdvice::RetrySameInstance,
    );
    assert_eq!(backend.user_reads.load(Ordering::SeqCst), 2);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
    assert_eq!(uuid.calls, 1);
}

#[test]
fn exhausted_sequence_and_uuid_failure_have_no_write_side_effects() {
    let mut batch = WriteBatch::new();
    batch.put(b"key", b"value").unwrap();
    let write = prepare_valid_batch(&batch);
    let backend = CountingBackend::default();
    let mut uuid = CountingUuidSource::default();

    let exhausted = prepare_commit(
        &write,
        [9; 16],
        u64::MAX,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &exhausted,
        StorageErrorKind::CapacityExceeded,
        Operation::WriteBatch,
        InstanceState::Healthy,
        RetryAdvice::DoNotRetry,
    );
    assert_eq!(backend.user_reads.load(Ordering::SeqCst), 0);
    assert_eq!(uuid.calls, 0);

    uuid.fail = true;
    let uuid_error = prepare_commit(
        &write,
        [9; 16],
        0,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &uuid_error,
        StorageErrorKind::Io,
        Operation::WriteBatch,
        InstanceState::Healthy,
        RetryAdvice::RetrySameInstance,
    );
    assert_eq!(uuid_error.os_code, Some(5));
    assert_eq!(backend.user_reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
    assert_eq!(uuid.calls, 1);
}

#[test]
fn prepare_commit_preserves_permanent_vlog_capacity_and_poisoned_layout_semantics() {
    let geometry = VLogGeometry::test_only(256, 512, 1).unwrap();
    let write = preflight_put(b"key", b"value", false).unwrap();

    let backend = CountingBackend::default();
    let mut uuid = CountingUuidSource::default();
    let exhausted = prepare_commit(
        &write,
        [9; 16],
        0,
        VLogPosition {
            file_id: 1,
            offset: 512,
        },
        geometry,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &exhausted,
        StorageErrorKind::CapacityExceeded,
        Operation::Put,
        InstanceState::Healthy,
        RetryAdvice::DoNotRetry,
    );
    assert_eq!(backend.user_reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
    assert_eq!(uuid.calls, 1);

    let backend = CountingBackend::default();
    let mut uuid = CountingUuidSource::default();
    let invalid_layout = prepare_commit(
        &write,
        [9; 16],
        0,
        VLogPosition {
            file_id: 0,
            offset: 240,
        },
        geometry,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &invalid_layout,
        StorageErrorKind::InvalidLayout,
        Operation::Put,
        InstanceState::Poisoned,
        RetryAdvice::RestoreOrRepair,
    );
    assert_eq!(backend.user_reads.load(Ordering::SeqCst), 1);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
    assert_eq!(uuid.calls, 1);
}

#[test]
fn index_read_failures_preserve_storage_severity_and_retry_contract() {
    let write = preflight_put(b"key", b"value", false).unwrap();
    for (kind, os_code, expected_state, expected_retry) in [
        (
            StorageErrorKind::Io,
            Some(5),
            InstanceState::Poisoned,
            RetryAdvice::ReopenAndVerify,
        ),
        (
            StorageErrorKind::StoragePoisoned,
            None,
            InstanceState::Poisoned,
            RetryAdvice::ReopenAndVerify,
        ),
        (
            StorageErrorKind::Corruption,
            None,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            StorageErrorKind::InvalidLayout,
            None,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            StorageErrorKind::IncompatibleFormat,
            None,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::Unrecoverable,
            None,
            InstanceState::Poisoned,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            StorageErrorKind::ResourceExhausted,
            None,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        ),
        (
            StorageErrorKind::Busy,
            None,
            InstanceState::Healthy,
            RetryAdvice::RetrySameInstance,
        ),
        (
            StorageErrorKind::StorageWriteStopped,
            None,
            InstanceState::WriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
        (
            StorageErrorKind::NotFound,
            None,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::InvalidArgument,
            None,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::Unsupported,
            None,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
        (
            StorageErrorKind::CapacityExceeded,
            None,
            InstanceState::Poisoned,
            RetryAdvice::DoNotRetry,
        ),
    ] {
        let backend = CountingBackend::with_user_error(kind, os_code);
        let mut uuid = CountingUuidSource::default();
        let error = prepare_commit(
            &write,
            [9; 16],
            0,
            VLogPosition {
                file_id: 0,
                offset: 0,
            },
            VLogGeometry::PRODUCTION,
            &backend,
            &mut uuid,
        )
        .unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            kind,
            Operation::Put,
            expected_state,
            expected_retry,
        );
        assert_eq!(error.os_code, os_code);
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), 1);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, 0);
    }

    let backend = CountingBackend::with_user_error(StorageErrorKind::Io, Some(5));
    let mut uuid = CountingUuidSource::default();
    let delete = preflight_delete(b"key", false).unwrap();
    let delete_error = prepare_commit(
        &delete,
        [9; 16],
        0,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &delete_error,
        StorageErrorKind::Io,
        Operation::Delete,
        InstanceState::Poisoned,
        RetryAdvice::ReopenAndVerify,
    );

    let mut batch = WriteBatch::new();
    batch.put(b"key", b"value").unwrap();
    let write_batch = preflight_batch(&batch, false).unwrap();
    let backend = CountingBackend::with_user_error(StorageErrorKind::Io, Some(5));
    let mut uuid = CountingUuidSource::default();
    let batch_error = prepare_commit(
        &write_batch,
        [9; 16],
        0,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap_err();
    assert_preflight_error_with_state_and_retry(
        &batch_error,
        StorageErrorKind::Io,
        Operation::WriteBatch,
        InstanceState::Poisoned,
        RetryAdvice::ReopenAndVerify,
    );
}

#[test]
fn malformed_versioned_and_key_mismatched_before_state_pointers_poison_preflight() {
    let matching_pointer = ValuePointer {
        format_version: 0,
        file_id: 0,
        record_offset: 64,
        record_len: 58,
        value_len: 0,
    }
    .encode()
    .unwrap();
    let mut unknown_version = matching_pointer;
    unknown_version[0..2].copy_from_slice(&1_u16.to_le_bytes());
    let wrong_key_len = ValuePointer {
        format_version: 0,
        file_id: 0,
        record_offset: 64,
        record_len: 57,
        value_len: 0,
    }
    .encode()
    .unwrap();

    for (encoded, expected_kind, expected_retry) in [
        (
            vec![0; 15],
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
        ),
        (
            unknown_version.to_vec(),
            StorageErrorKind::IncompatibleFormat,
            RetryAdvice::DoNotRetry,
        ),
        (
            wrong_key_len.to_vec(),
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
        ),
    ] {
        let write = preflight_put(b"key", b"value", false).unwrap();
        let backend = CountingBackend::with_user_value(encoded);
        let mut uuid = CountingUuidSource::default();
        let error = prepare_commit(
            &write,
            [9; 16],
            0,
            VLogPosition {
                file_id: 0,
                offset: 0,
            },
            VLogGeometry::PRODUCTION,
            &backend,
            &mut uuid,
        )
        .unwrap_err();
        assert_preflight_error_with_state_and_retry(
            &error,
            expected_kind,
            Operation::Put,
            InstanceState::Poisoned,
            expected_retry,
        );
        assert_eq!(backend.user_reads.load(Ordering::SeqCst), 1);
        assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
        assert_eq!(uuid.calls, 0);
    }
}
