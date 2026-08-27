//! Ordered compound-commit orchestration.

mod coordinator;
mod descriptor;
mod durability;
mod protocol;

#[allow(unused_imports)]
pub(crate) use coordinator::{CommitCoordinator, CommitStateSnapshot};
#[allow(unused_imports)]
pub(crate) use descriptor::{
    CommitSeq, DurableFrontier, DurableVLogEnd, RECOVERY_STATE_KEY, RecoveryPhase, RecoveryState,
    TransactionDescriptor, TxMutation, TxUuid, VLogPos, ValueState, decode_descriptor,
    decode_head_seq, decode_tx_meta_key, decode_tx_mutation_key, encode_tx_meta_key,
};

#[allow(unused_imports)]
pub(crate) use protocol::{
    OsTxUuidSource, PreparedCommit, TxUuidSource, ValidatedWrite, preflight_batch,
    preflight_delete, preflight_put, prepare_commit,
};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use protocol::{
    AllocationFailureSite, inject_allocation_failure_for_test,
    preflight_batch_with_operation_limit_for_test, validate_operation_count_for_test,
};

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use descriptor::{
    DescriptorAllocationFailureSite, inject_descriptor_allocation_failure_for_test,
};
