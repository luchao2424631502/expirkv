//! Ordered compound-commit orchestration.

mod coordinator;
mod descriptor;
mod durability;
mod protocol;

#[allow(unused_imports)]
pub(crate) use coordinator::{CommitCoordinator, CommitStateSnapshot};
#[allow(unused_imports)]
pub(crate) use descriptor::{DurableFrontier, DurableVLogEnd};

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
    DescriptorAllocationFailureSite, TransactionDescriptor, TxMutation, VLogPos, ValueState,
    decode_descriptor, decode_head_seq, inject_descriptor_allocation_failure_for_test,
};
