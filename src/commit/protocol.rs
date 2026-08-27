//! I/O-free validation and physical planning for compound commits.
#![allow(dead_code)] // Connected to the public write path in later stages.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};

use crate::batch::{BatchOperation, WriteBatch};
use crate::index::{
    HEAD_SEQ_KEY, IndexAtomicBatch, IndexBackend, IndexMutation, InternalIndexError,
    InternalIndexSpace,
};
use crate::vlog::format::{
    LayoutPlanner, LogicalOperationRef, MAX_KV_RECORD_LEN, MIN_DELETE_RECORD_LEN,
    MIN_KV_RECORD_LEN, PreparedEnvelope, VLogGeometry, VLogPosition, ValuePointer,
};
use crate::{InstanceState, Operation, Result, RetryAdvice, StorageError, StorageErrorKind};

use super::descriptor::{
    CommitSeq, TransactionDescriptor, TxMeta, TxMutation, TxUuid, VLogPos, ValueState,
    encode_descriptor, encode_head_seq, next_commit_seq,
};

const MAX_KEY_VALUE_SIZE: usize = 60_000;
const ENVELOPE_BOUNDARY_RECORD_COUNT: usize = 2;
const INDEX_FIXED_MUTATION_COUNT: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NormalizedOperation<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct NormalizedWrite<'a> {
    operations: Vec<NormalizedOperation<'a>>,
    sync: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ValidatedWrite<'a> {
    normalized: NormalizedWrite<'a>,
    distinct_keys: Vec<&'a [u8]>,
    operation_ordinals: Vec<usize>,
    public_operation: Operation,
}

impl ValidatedWrite<'_> {
    pub(crate) fn logical_op_count(&self) -> usize {
        self.normalized.operations.len()
    }

    pub(crate) fn distinct_key_count(&self) -> usize {
        self.distinct_keys.len()
    }

    pub(crate) fn sync(&self) -> bool {
        self.normalized.sync
    }

    pub(crate) fn public_operation(&self) -> Operation {
        self.public_operation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedCommit {
    pub(crate) commit_seq: CommitSeq,
    pub(crate) tx_uuid: TxUuid,
    pub(crate) vlog_begin: VLogPos,
    pub(crate) vlog_end: VLogPos,
    pub(crate) envelope: PreparedEnvelope,
    pub(crate) index_batch: IndexAtomicBatch,
    pub(crate) sync: bool,
}

pub(crate) trait TxUuidSource {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct OsTxUuidSource;

impl TxUuidSource for OsTxUuidSource {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        File::open("/dev/urandom")?.read_exact(output)
    }
}

pub(crate) fn preflight_put<'a>(
    key: &'a [u8],
    value: &'a [u8],
    sync: bool,
) -> Result<ValidatedWrite<'a>> {
    validate_put(key, value, Operation::Put)?;
    let mut operations =
        try_vec_with_capacity(1, Operation::Put, AllocationFailureSite::Operations)?;
    operations.push(NormalizedOperation::Put { key, value });
    validate_normalized(
        NormalizedWrite { operations, sync },
        Operation::Put,
        u32::MAX as usize,
    )
}

pub(crate) fn preflight_delete(key: &[u8], sync: bool) -> Result<ValidatedWrite<'_>> {
    validate_key(key, Operation::Delete)?;
    let mut operations =
        try_vec_with_capacity(1, Operation::Delete, AllocationFailureSite::Operations)?;
    operations.push(NormalizedOperation::Delete { key });
    validate_normalized(
        NormalizedWrite { operations, sync },
        Operation::Delete,
        u32::MAX as usize,
    )
}

pub(crate) fn preflight_batch(batch: &WriteBatch, sync: bool) -> Result<ValidatedWrite<'_>> {
    preflight_batch_with_operation_limit(batch, sync, u32::MAX as usize)
}

fn preflight_batch_with_operation_limit<'a>(
    batch: &'a WriteBatch,
    sync: bool,
    max_operation_count: usize,
) -> Result<ValidatedWrite<'a>> {
    validate_operation_count(batch.len(), max_operation_count, Operation::WriteBatch)?;
    for operation in batch.operations() {
        match operation {
            BatchOperation::Put { key, value } => {
                validate_put_encoding(key, value, Operation::WriteBatch)?;
            }
            BatchOperation::Delete { key } => {
                validate_delete_encoding(key, Operation::WriteBatch)?;
            }
        }
    }
    let mut operations = try_vec_with_capacity(
        batch.len(),
        Operation::WriteBatch,
        AllocationFailureSite::Operations,
    )?;
    for operation in batch.operations() {
        operations.push(match operation {
            BatchOperation::Put { key, value } => NormalizedOperation::Put {
                key: key.as_slice(),
                value: value.as_slice(),
            },
            BatchOperation::Delete { key } => NormalizedOperation::Delete {
                key: key.as_slice(),
            },
        });
    }
    validate_normalized(
        NormalizedWrite { operations, sync },
        Operation::WriteBatch,
        max_operation_count,
    )
}

#[cfg(test)]
pub(crate) fn preflight_batch_with_operation_limit_for_test<'a>(
    batch: &'a WriteBatch,
    sync: bool,
    max_operation_count: usize,
) -> Result<ValidatedWrite<'a>> {
    preflight_batch_with_operation_limit(batch, sync, max_operation_count)
}

fn validate_normalized<'a>(
    normalized: NormalizedWrite<'a>,
    public_operation: Operation,
    max_operation_count: usize,
) -> Result<ValidatedWrite<'a>> {
    let operation_count = normalized.operations.len();
    if operation_count == 0 {
        return Err(invalid_argument(public_operation));
    }
    validate_operation_count(operation_count, max_operation_count, public_operation)?;

    for operation in normalized.operations.iter().copied() {
        match operation {
            NormalizedOperation::Put { key, value } => {
                validate_put_encoding(key, value, public_operation)?;
            }
            NormalizedOperation::Delete { key } => {
                validate_delete_encoding(key, public_operation)?;
            }
        }
    }

    let mut distinct_keys = try_vec_with_capacity(
        operation_count,
        public_operation,
        AllocationFailureSite::DistinctKeys,
    )?;
    let mut operation_ordinals = try_vec_with_capacity(
        operation_count,
        public_operation,
        AllocationFailureSite::OperationOrdinals,
    )?;
    let mut ordinals = HashMap::<&[u8], usize>::new();
    inject_allocation_failure(AllocationFailureSite::OrdinalMap, public_operation)?;
    ordinals
        .try_reserve(operation_count)
        .map_err(|_| resource_exhausted(public_operation))?;

    for operation in normalized.operations.iter().copied() {
        let key = match operation {
            NormalizedOperation::Put { key, value } => {
                let _ = value;
                key
            }
            NormalizedOperation::Delete { key } => key,
        };

        let ordinal = if let Some(ordinal) = ordinals.get(key).copied() {
            ordinal
        } else {
            let ordinal = distinct_keys.len();
            u64::try_from(ordinal).map_err(|_| capacity_exceeded(public_operation))?;
            distinct_keys.push(key);
            ordinals.insert(key, ordinal);
            ordinal
        };
        operation_ordinals.push(ordinal);
    }

    let distinct_count = distinct_keys.len();
    u64::try_from(distinct_count).map_err(|_| capacity_exceeded(public_operation))?;
    distinct_count
        .checked_mul(2)
        .and_then(|count| count.checked_add(INDEX_FIXED_MUTATION_COUNT))
        .ok_or_else(|| capacity_exceeded(public_operation))?;

    Ok(ValidatedWrite {
        normalized,
        distinct_keys,
        operation_ordinals,
        public_operation,
    })
}

pub(crate) fn prepare_commit<B, U>(
    write: &ValidatedWrite<'_>,
    database_uuid: [u8; 16],
    head_seq: CommitSeq,
    append_cursor: VLogPosition,
    geometry: VLogGeometry,
    index: &B,
    uuid_source: &mut U,
) -> Result<PreparedCommit>
where
    B: IndexBackend,
    U: TxUuidSource,
{
    if database_uuid == [0; 16] {
        return Err(invalid_argument(write.public_operation));
    }
    let commit_seq = next_commit_seq(head_seq)
        .map_err(|_| permanent_capacity_exceeded(write.public_operation))?;

    let mut before_states = try_vec_with_capacity(
        write.distinct_keys.len(),
        write.public_operation,
        AllocationFailureSite::BeforeStates,
    )?;
    for key in &write.distinct_keys {
        let before_state = match index
            .get_user(key, None)
            .map_err(|error| remap_index_read(error, write.public_operation))?
        {
            None => ValueState::Absent,
            Some(encoded_pointer) => {
                decode_before_state_pointer(&encoded_pointer, key, write.public_operation)?
            }
        };
        before_states.push(before_state);
    }

    let tx_uuid = allocate_tx_uuid(uuid_source, write.public_operation)?;
    let logical_operations = logical_operations(write)?;
    let mut planner = LayoutPlanner::from_position(geometry, append_cursor)
        .map_err(|error| remap_preflight(error, write.public_operation))?;
    let envelope = crate::vlog::format::prepare_envelope(
        &mut planner,
        database_uuid,
        commit_seq,
        tx_uuid.0,
        &logical_operations,
    )
    .map_err(|error| remap_preflight(error, write.public_operation))?;

    let key_plans = build_key_plans(write, &before_states, &envelope.value_pointers)?;
    let logical_op_count = u64::try_from(write.normalized.operations.len())
        .map_err(|_| capacity_exceeded(write.public_operation))?;
    let distinct_key_count =
        u64::try_from(key_plans.len()).map_err(|_| capacity_exceeded(write.public_operation))?;
    let vlog_begin = to_descriptor_position(envelope.vlog_begin);
    let vlog_end = to_descriptor_position(envelope.vlog_end);
    let mutations = descriptor_mutations(&key_plans, write.public_operation)?;
    let descriptor = TransactionDescriptor {
        meta: TxMeta {
            commit_seq,
            tx_uuid,
            prev_seq: head_seq,
            vlog_begin,
            vlog_end,
            logical_op_count,
            distinct_key_count,
            envelope_crc32c: envelope.envelope_crc32c,
            descriptor_crc32c: 0,
        },
        mutations,
    };
    let encoded_descriptor = encode_descriptor(&descriptor)
        .map_err(|error| remap_preflight(error, write.public_operation))?;
    let index_batch = build_index_batch(
        &key_plans,
        encoded_descriptor,
        commit_seq,
        write.public_operation,
    )?;

    Ok(PreparedCommit {
        commit_seq,
        tx_uuid,
        vlog_begin,
        vlog_end,
        envelope,
        index_batch,
        sync: write.normalized.sync,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KeyPlan {
    user_key: Vec<u8>,
    before_state: ValueState,
    after_state: ValueState,
}

fn build_key_plans(
    write: &ValidatedWrite<'_>,
    before_states: &[ValueState],
    value_pointers: &[Option<ValuePointer>],
) -> Result<Vec<KeyPlan>> {
    if before_states.len() != write.distinct_keys.len()
        || value_pointers.len() != write.normalized.operations.len()
        || write.operation_ordinals.len() != write.normalized.operations.len()
    {
        return Err(invalid_argument(write.public_operation));
    }
    let mut plans = try_vec_with_capacity(
        write.distinct_keys.len(),
        write.public_operation,
        AllocationFailureSite::KeyPlans,
    )?;
    for (key, before_state) in write
        .distinct_keys
        .iter()
        .zip(before_states.iter().copied())
    {
        plans.push(KeyPlan {
            user_key: try_clone_bytes(
                key,
                write.public_operation,
                AllocationFailureSite::KeyPlanUserKey,
            )?,
            before_state,
            after_state: before_state,
        });
    }

    for ((operation, ordinal), pointer) in write
        .normalized
        .operations
        .iter()
        .zip(&write.operation_ordinals)
        .zip(value_pointers)
    {
        let plan = plans
            .get_mut(*ordinal)
            .ok_or_else(|| invalid_argument(write.public_operation))?;
        plan.after_state = match (operation, pointer) {
            (NormalizedOperation::Put { .. }, Some(pointer)) => ValueState::Present(*pointer),
            (NormalizedOperation::Delete { .. }, None) => ValueState::Absent,
            _ => return Err(invalid_argument(write.public_operation)),
        };
    }
    Ok(plans)
}

fn descriptor_mutations(plans: &[KeyPlan], operation: Operation) -> Result<Vec<TxMutation>> {
    let mut mutations = try_vec_with_capacity(
        plans.len(),
        operation,
        AllocationFailureSite::DescriptorMutations,
    )?;
    for plan in plans {
        mutations.push(TxMutation {
            user_key: try_clone_bytes(
                &plan.user_key,
                operation,
                AllocationFailureSite::DescriptorUserKey,
            )?,
            before_state: plan.before_state,
            after_state: plan.after_state,
        });
    }
    Ok(mutations)
}

fn build_index_batch(
    plans: &[KeyPlan],
    descriptor: super::descriptor::EncodedDescriptor,
    commit_seq: CommitSeq,
    operation: Operation,
) -> Result<IndexAtomicBatch> {
    let capacity = plans
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(INDEX_FIXED_MUTATION_COUNT))
        .ok_or_else(|| capacity_exceeded(operation))?;
    inject_allocation_failure(AllocationFailureSite::IndexBatch, operation)?;
    let mut batch = IndexAtomicBatch::try_with_capacity(capacity)
        .map_err(|error| map_index_construction_error(error, operation))?;

    for plan in plans {
        let user_key = try_clone_bytes(
            &plan.user_key,
            operation,
            AllocationFailureSite::IndexUserKey,
        )?;
        let mutation = match plan.after_state {
            ValueState::Absent => IndexMutation::DeleteUser { user_key },
            ValueState::Present(pointer) => IndexMutation::PutUser {
                user_key,
                encoded_pointer: try_clone_bytes(
                    &pointer
                        .encode()
                        .map_err(|error| remap_preflight(error, operation))?,
                    operation,
                    AllocationFailureSite::IndexPointer,
                )?,
            },
        };
        push_index(&mut batch, mutation, operation)?;
    }

    push_index(
        &mut batch,
        IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key: try_clone_bytes(
                &descriptor.meta_key,
                operation,
                AllocationFailureSite::IndexMetaKey,
            )?,
            value: try_clone_bytes(
                &descriptor.meta_value,
                operation,
                AllocationFailureSite::IndexMetaValue,
            )?,
        },
        operation,
    )?;
    for mutation in descriptor.mutations {
        push_index(
            &mut batch,
            IndexMutation::PutInternal {
                space: InternalIndexSpace::Transaction,
                key: try_clone_bytes(
                    &mutation.key,
                    operation,
                    AllocationFailureSite::IndexMutationKey,
                )?,
                value: mutation.value,
            },
            operation,
        )?;
    }
    push_index(
        &mut batch,
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_clone_bytes(HEAD_SEQ_KEY, operation, AllocationFailureSite::IndexHeadKey)?,
            value: try_clone_bytes(
                &encode_head_seq(commit_seq),
                operation,
                AllocationFailureSite::IndexHeadValue,
            )?,
        },
        operation,
    )?;
    debug_assert_eq!(batch.len(), capacity);
    Ok(batch)
}

fn push_index(
    batch: &mut IndexAtomicBatch,
    mutation: IndexMutation,
    operation: Operation,
) -> Result<()> {
    batch
        .try_push(mutation)
        .map_err(|error| map_index_construction_error(error, operation))
}

fn logical_operations<'a>(write: &'a ValidatedWrite<'a>) -> Result<Vec<LogicalOperationRef<'a>>> {
    let mut operations = try_vec_with_capacity(
        write.normalized.operations.len(),
        write.public_operation,
        AllocationFailureSite::LogicalOperations,
    )?;
    for operation in write.normalized.operations.iter().copied() {
        operations.push(match operation {
            NormalizedOperation::Put { key, value } => LogicalOperationRef::Put { key, value },
            NormalizedOperation::Delete { key } => LogicalOperationRef::Delete { key },
        });
    }
    Ok(operations)
}

fn allocate_tx_uuid<U: TxUuidSource>(source: &mut U, operation: Operation) -> Result<TxUuid> {
    let mut bytes = [0_u8; 16];
    source.fill_random_bytes(&mut bytes).map_err(|error| {
        let mut storage_error = StorageError::write_preflight(
            StorageErrorKind::Io,
            operation,
            RetryAdvice::RetrySameInstance,
        );
        storage_error.os_code = error.raw_os_error();
        storage_error
    })?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(TxUuid(bytes))
}

fn validate_key(key: &[u8], operation: Operation) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_VALUE_SIZE {
        return Err(invalid_argument(operation));
    }
    u16::try_from(key.len()).map_err(|_| capacity_exceeded(operation))?;
    Ok(())
}

fn validate_put(key: &[u8], value: &[u8], operation: Operation) -> Result<()> {
    validate_key(key, operation)?;
    u16::try_from(value.len()).map_err(|_| capacity_exceeded(operation))?;
    let combined_len = key
        .len()
        .checked_add(value.len())
        .ok_or_else(|| capacity_exceeded(operation))?;
    if combined_len > MAX_KEY_VALUE_SIZE {
        return Err(invalid_argument(operation));
    }
    Ok(())
}

fn validate_operation_count(
    operation_count: usize,
    max_operation_count: usize,
    operation: Operation,
) -> Result<()> {
    operation_count
        .checked_add(ENVELOPE_BOUNDARY_RECORD_COUNT)
        .ok_or_else(|| capacity_exceeded(operation))?;
    if operation_count > max_operation_count
        || u32::try_from(operation_count).is_err()
        || u64::try_from(operation_count).is_err()
    {
        return Err(capacity_exceeded(operation));
    }
    Ok(())
}

fn validate_put_encoding(key: &[u8], value: &[u8], operation: Operation) -> Result<()> {
    validate_put(key, value, operation)?;
    let combined_len = key
        .len()
        .checked_add(value.len())
        .ok_or_else(|| capacity_exceeded(operation))?;
    let encoded_len = usize::try_from(MIN_KV_RECORD_LEN - 1)
        .ok()
        .and_then(|fixed| fixed.checked_add(combined_len))
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| capacity_exceeded(operation))?;
    if encoded_len > MAX_KV_RECORD_LEN {
        return Err(invalid_argument(operation));
    }
    Ok(())
}

fn validate_delete_encoding(key: &[u8], operation: Operation) -> Result<()> {
    validate_key(key, operation)?;
    usize::try_from(MIN_DELETE_RECORD_LEN - 1)
        .ok()
        .and_then(|fixed| fixed.checked_add(key.len()))
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| capacity_exceeded(operation))?;
    Ok(())
}

fn to_descriptor_position(position: VLogPosition) -> VLogPos {
    VLogPos {
        file_id: position.file_id,
        offset: position.offset,
    }
}

fn try_clone_bytes(
    bytes: &[u8],
    operation: Operation,
    site: AllocationFailureSite,
) -> Result<Vec<u8>> {
    inject_allocation_failure(site, operation)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| resource_exhausted(operation))?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn try_vec_with_capacity<T>(
    capacity: usize,
    operation: Operation,
    site: AllocationFailureSite,
) -> Result<Vec<T>> {
    inject_allocation_failure(site, operation)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| resource_exhausted(operation))?;
    Ok(values)
}

fn map_index_construction_error(error: InternalIndexError, operation: Operation) -> StorageError {
    let retry = if error.kind == StorageErrorKind::ResourceExhausted {
        RetryAdvice::RetrySameInstance
    } else {
        RetryAdvice::FixRequestAndRetrySameInstance
    };
    StorageError::write_preflight(error.kind, operation, retry)
}

fn remap_index_read(error: StorageError, operation: Operation) -> StorageError {
    let (instance_state, retry) = match error.kind {
        StorageErrorKind::ResourceExhausted | StorageErrorKind::Busy => {
            (InstanceState::Healthy, RetryAdvice::RetrySameInstance)
        }
        StorageErrorKind::StorageWriteStopped => (
            InstanceState::WriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
        StorageErrorKind::Io | StorageErrorKind::StoragePoisoned => {
            (InstanceState::Poisoned, RetryAdvice::ReopenAndVerify)
        }
        StorageErrorKind::Corruption
        | StorageErrorKind::InvalidLayout
        | StorageErrorKind::Unrecoverable => {
            (InstanceState::Poisoned, RetryAdvice::RestoreOrRepair)
        }
        StorageErrorKind::IncompatibleFormat => (InstanceState::Poisoned, RetryAdvice::DoNotRetry),
        StorageErrorKind::InvalidArgument
        | StorageErrorKind::NotFound
        | StorageErrorKind::Unsupported
        | StorageErrorKind::CapacityExceeded => (InstanceState::Poisoned, RetryAdvice::DoNotRetry),
    };
    let mut mapped =
        StorageError::write_preflight_in_state(error.kind, operation, instance_state, retry);
    mapped.os_code = error.os_code;
    mapped
}

fn decode_before_state_pointer(
    encoded_pointer: &[u8],
    user_key: &[u8],
    operation: Operation,
) -> Result<ValueState> {
    let pointer = ValuePointer::decode(encoded_pointer)
        .map_err(|error| remap_stored_pointer(error, operation))?;
    let layout = pointer
        .layout()
        .map_err(|error| remap_stored_pointer(error, operation))?;
    if usize::from(layout.key_len) != user_key.len() {
        return Err(stored_pointer_error(
            StorageErrorKind::Corruption,
            operation,
        ));
    }
    Ok(ValueState::Present(pointer))
}

fn remap_stored_pointer(error: StorageError, operation: Operation) -> StorageError {
    let mut mapped = stored_pointer_error(error.kind, operation);
    mapped.os_code = error.os_code;
    mapped
}

fn stored_pointer_error(kind: StorageErrorKind, operation: Operation) -> StorageError {
    let retry = if kind == StorageErrorKind::IncompatibleFormat {
        RetryAdvice::DoNotRetry
    } else {
        RetryAdvice::RestoreOrRepair
    };
    StorageError::write_preflight_in_state(kind, operation, InstanceState::Poisoned, retry)
}

fn remap_preflight(error: StorageError, operation: Operation) -> StorageError {
    let (instance_state, retry) = match error.kind {
        StorageErrorKind::ResourceExhausted => {
            (InstanceState::Healthy, RetryAdvice::RetrySameInstance)
        }
        StorageErrorKind::Corruption | StorageErrorKind::InvalidLayout => {
            (InstanceState::Poisoned, RetryAdvice::RestoreOrRepair)
        }
        StorageErrorKind::IncompatibleFormat => (InstanceState::Poisoned, RetryAdvice::DoNotRetry),
        StorageErrorKind::CapacityExceeded if error.retry_advice == RetryAdvice::DoNotRetry => {
            (InstanceState::Healthy, RetryAdvice::DoNotRetry)
        }
        _ => (
            InstanceState::Healthy,
            RetryAdvice::FixRequestAndRetrySameInstance,
        ),
    };
    let mut mapped =
        StorageError::write_preflight_in_state(error.kind, operation, instance_state, retry);
    mapped.os_code = error.os_code;
    mapped
}

fn invalid_argument(operation: Operation) -> StorageError {
    StorageError::write_preflight(
        StorageErrorKind::InvalidArgument,
        operation,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn capacity_exceeded(operation: Operation) -> StorageError {
    StorageError::write_preflight(
        StorageErrorKind::CapacityExceeded,
        operation,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn permanent_capacity_exceeded(operation: Operation) -> StorageError {
    StorageError::write_preflight(
        StorageErrorKind::CapacityExceeded,
        operation,
        RetryAdvice::DoNotRetry,
    )
}

fn resource_exhausted(operation: Operation) -> StorageError {
    StorageError::write_preflight(
        StorageErrorKind::ResourceExhausted,
        operation,
        RetryAdvice::RetrySameInstance,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AllocationFailureSite {
    Operations,
    DistinctKeys,
    OperationOrdinals,
    OrdinalMap,
    BeforeStates,
    LogicalOperations,
    KeyPlans,
    KeyPlanUserKey,
    DescriptorMutations,
    DescriptorUserKey,
    IndexBatch,
    IndexUserKey,
    IndexPointer,
    IndexMetaKey,
    IndexMetaValue,
    IndexMutationKey,
    IndexHeadKey,
    IndexHeadValue,
}

#[cfg(test)]
mod allocation_failure {
    use std::cell::Cell;

    use super::AllocationFailureSite;

    thread_local! {
        static NEXT_FAILURE: Cell<Option<AllocationFailureSite>> = const { Cell::new(None) };
    }

    pub(super) fn inject(site: AllocationFailureSite) {
        NEXT_FAILURE.with(|next| assert!(next.replace(Some(site)).is_none()));
    }

    pub(super) fn should_fail(site: AllocationFailureSite) -> bool {
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
pub(crate) fn inject_allocation_failure_for_test(site: AllocationFailureSite) {
    allocation_failure::inject(site);
}

#[cfg(test)]
pub(crate) fn validate_operation_count_for_test(
    operation_count: usize,
    max_operation_count: usize,
) -> Result<()> {
    validate_operation_count(operation_count, max_operation_count, Operation::WriteBatch)
}

fn inject_allocation_failure(site: AllocationFailureSite, operation: Operation) -> Result<()> {
    #[cfg(test)]
    if allocation_failure::should_fail(site) {
        return Err(resource_exhausted(operation));
    }
    let _ = (site, operation);
    Ok(())
}
