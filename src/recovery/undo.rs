//! Recovery state and reverse-order undo.

use crate::Result;
use crate::commit::{
    RecoveryState, TransactionDescriptor, ValueState, decode_descriptor, decode_tx_meta_key,
    decode_tx_mutation_key, encode_tx_meta_key,
};
use crate::index::{
    HEAD_SEQ_KEY, IndexAtomicBatch, IndexBackend, IndexEntry, IndexMutation, InternalIndexSpace,
    InternalKeyRange,
};

use super::{
    RecoveryPlan, commit_recovery_batch, index_batch_context, recovery_context,
    recovery_corruption, recovery_resource, try_copy_recovery_bytes,
};

struct LoadedDescriptor {
    descriptor: TransactionDescriptor,
    meta: IndexEntry,
    mutations: Vec<IndexEntry>,
}

pub(super) fn undo_transactions<B: IndexBackend>(
    backend: &B,
    plan: &RecoveryPlan,
    mut state: RecoveryState,
) -> Result<RecoveryState> {
    while state.next_undo_seq > state.target_seq {
        let commit_seq = state.next_undo_seq;
        let expected = expected_descriptor(plan, commit_seq)?;
        let loaded = load_descriptor(backend, expected)?;
        validate_current_after_states(backend, &loaded.descriptor)?;

        let next_undo_seq = commit_seq.checked_sub(1).ok_or_else(recovery_corruption)?;
        let next_state = RecoveryState {
            next_undo_seq,
            ..state
        };
        let batch = build_undo_batch(loaded, next_state)?;
        commit_recovery_batch(backend, batch)?;
        state = next_state;
    }
    Ok(state)
}

fn expected_descriptor(plan: &RecoveryPlan, commit_seq: u64) -> Result<&TransactionDescriptor> {
    let relative = commit_seq
        .checked_sub(plan.durable_frontier.durable_seq)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(recovery_corruption)?;
    let index = usize::try_from(relative).map_err(|_| recovery_corruption())?;
    plan.descriptors.get(index).ok_or_else(recovery_corruption)
}

fn load_descriptor<B: IndexBackend>(
    backend: &B,
    expected: &TransactionDescriptor,
) -> Result<LoadedDescriptor> {
    let commit_seq = expected.meta.commit_seq;
    let range = descriptor_range(commit_seq)?;
    let entries = backend
        .scan_internal(InternalIndexSpace::Transaction, range)
        .map_err(recovery_context)?;
    let mut meta = None;
    let expected_count =
        usize::try_from(expected.meta.distinct_key_count).map_err(|_| recovery_corruption())?;
    let mut mutations = Vec::new();
    mutations
        .try_reserve_exact(expected_count)
        .map_err(|_| recovery_resource())?;
    for entry in entries {
        let entry = entry.map_err(recovery_context)?;
        match entry.key.len() {
            11 => {
                if decode_tx_meta_key(&entry.key).map_err(recovery_context)? != commit_seq
                    || meta.replace(entry).is_some()
                {
                    return Err(recovery_corruption());
                }
            }
            19 => {
                if decode_tx_mutation_key(&entry.key)
                    .map_err(recovery_context)?
                    .0
                    != commit_seq
                {
                    return Err(recovery_corruption());
                }
                mutations.try_reserve(1).map_err(|_| recovery_resource())?;
                mutations.push(entry);
            }
            _ => return Err(recovery_corruption()),
        }
    }
    let meta = meta.ok_or_else(recovery_corruption)?;
    let mut mutation_refs = Vec::new();
    mutation_refs
        .try_reserve_exact(mutations.len())
        .map_err(|_| recovery_resource())?;
    mutation_refs.extend(
        mutations
            .iter()
            .map(|entry| (entry.key.as_slice(), entry.value.as_slice())),
    );
    let descriptor =
        decode_descriptor(&meta.key, &meta.value, &mutation_refs).map_err(recovery_context)?;
    if descriptor != *expected {
        return Err(recovery_corruption());
    }
    Ok(LoadedDescriptor {
        descriptor,
        meta,
        mutations,
    })
}

fn descriptor_range(commit_seq: u64) -> Result<InternalKeyRange> {
    let start_key = encode_tx_meta_key(commit_seq).map_err(recovery_context)?;
    let start = try_copy_recovery_bytes(start_key.get(..10).ok_or_else(recovery_corruption)?)?;
    let end_exclusive = commit_seq
        .checked_add(1)
        .map(|next| {
            let key = encode_tx_meta_key(next).map_err(recovery_context)?;
            try_copy_recovery_bytes(key.get(..10).ok_or_else(recovery_corruption)?)
        })
        .transpose()?;
    Ok(InternalKeyRange {
        start_inclusive: Some(start),
        end_exclusive,
    })
}

fn validate_current_after_states<B: IndexBackend>(
    backend: &B,
    descriptor: &TransactionDescriptor,
) -> Result<()> {
    for mutation in &descriptor.mutations {
        let actual = backend
            .get_user(&mutation.user_key, None)
            .map_err(recovery_context)?;
        let matches = match mutation.after_state {
            ValueState::Absent => actual.is_none(),
            ValueState::Present(pointer) => {
                let encoded = pointer.encode().map_err(recovery_context)?;
                actual.as_deref() == Some(encoded.as_slice())
            }
        };
        if !matches {
            return Err(recovery_corruption());
        }
    }
    Ok(())
}

fn build_undo_batch(
    loaded: LoadedDescriptor,
    next_state: RecoveryState,
) -> Result<IndexAtomicBatch> {
    let mutation_count = loaded.descriptor.mutations.len();
    let capacity = mutation_count
        .checked_mul(2)
        .and_then(|value| value.checked_add(3))
        .ok_or_else(recovery_resource)?;
    let mut batch = IndexAtomicBatch::try_with_capacity(capacity).map_err(index_batch_context)?;

    for mutation in &loaded.descriptor.mutations {
        let user_key = try_copy_recovery_bytes(&mutation.user_key)?;
        let operation = match mutation.before_state {
            ValueState::Absent => IndexMutation::DeleteUser { user_key },
            ValueState::Present(pointer) => IndexMutation::PutUser {
                user_key,
                encoded_pointer: try_copy_recovery_bytes(
                    &pointer.encode().map_err(recovery_context)?,
                )?,
            },
        };
        batch.try_push(operation).map_err(index_batch_context)?;
    }
    for mutation in loaded.mutations {
        batch
            .try_push(IndexMutation::DeleteInternal {
                space: InternalIndexSpace::Transaction,
                key: mutation.key,
            })
            .map_err(index_batch_context)?;
    }
    batch
        .try_push(IndexMutation::DeleteInternal {
            space: InternalIndexSpace::Transaction,
            key: loaded.meta.key,
        })
        .map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(HEAD_SEQ_KEY)?,
            value: try_copy_recovery_bytes(&next_state.next_undo_seq.to_le_bytes())?,
        })
        .map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(crate::commit::RECOVERY_STATE_KEY)?,
            value: try_copy_recovery_bytes(&next_state.encode().map_err(recovery_context)?)?,
        })
        .map_err(index_batch_context)?;
    Ok(batch)
}
