//! Public statistics data and private in-memory snapshots.

use std::sync::RwLock;

use crate::{InstanceState, Operation, ProtocolStage, RetryAdvice, StorageError, StorageErrorKind};

#[derive(Clone, Debug)]
pub struct VLogPosition {
    pub file_id: u32,
    pub offset: u64,
}

#[derive(Clone, Debug)]
pub struct LatchedErrorSummary {
    pub kind: StorageErrorKind,
    pub operation: Operation,
    pub protocol_stage: ProtocolStage,
    pub retry_advice: RetryAdvice,
    pub os_code: Option<i32>,
    pub commit_seq: Option<u64>,
    pub vlog_file_id: Option<u32>,
    pub vlog_offset: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct DbStats {
    pub schema_version: u16,
    pub instance_state: InstanceState,
    pub state_epoch: u64,
    pub first_latched_error: Option<LatchedErrorSummary>,
    pub head_seq: u64,
    pub durable_seq: u64,
    pub durability_lag: u64,
    pub durable_vlog_end: Option<VLogPosition>,
    pub active_vlog_file_id: Option<u32>,
    pub vlog_file_count: u32,
    pub vlog_logical_bytes: u64,
}

pub(crate) struct StatsState {
    snapshot: RwLock<DbStats>,
}

impl StatsState {
    pub(crate) fn new() -> Self {
        Self {
            snapshot: RwLock::new(DbStats {
                schema_version: 1,
                instance_state: InstanceState::Healthy,
                state_epoch: 0,
                first_latched_error: None,
                head_seq: 0,
                durable_seq: 0,
                durability_lag: 0,
                durable_vlog_end: None,
                active_vlog_file_id: None,
                vlog_file_count: 0,
                vlog_logical_bytes: 0,
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> DbStats {
        match self.snapshot.read() {
            Ok(snapshot) => snapshot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub(crate) fn update_runtime_state(
        &self,
        instance_state: InstanceState,
        state_epoch: u64,
        first_latched_error: Option<LatchedErrorSummary>,
    ) {
        let mut snapshot = match self.snapshot.write() {
            Ok(snapshot) => snapshot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state_epoch > snapshot.state_epoch
            || (state_epoch == snapshot.state_epoch && instance_state == snapshot.instance_state)
        {
            snapshot.instance_state = instance_state;
            snapshot.state_epoch = state_epoch;
            snapshot.first_latched_error = first_latched_error;
        }
    }

    #[must_use]
    pub(crate) fn update_commit_state(
        &self,
        head_seq: u64,
        durable_seq: u64,
        durable_vlog_end: Option<(u32, u64)>,
        active_vlog_file_id: Option<u32>,
        vlog_file_count: u32,
        vlog_logical_bytes: u64,
    ) -> bool {
        if head_seq < durable_seq {
            return false;
        }
        let mut snapshot = match self.snapshot.write() {
            Ok(snapshot) => snapshot,
            Err(poisoned) => poisoned.into_inner(),
        };
        snapshot.head_seq = head_seq;
        snapshot.durable_seq = durable_seq;
        snapshot.durability_lag = head_seq - durable_seq;
        snapshot.durable_vlog_end =
            durable_vlog_end.map(|(file_id, offset)| VLogPosition { file_id, offset });
        snapshot.active_vlog_file_id = active_vlog_file_id;
        snapshot.vlog_file_count = vlog_file_count;
        snapshot.vlog_logical_bytes = vlog_logical_bytes;
        true
    }
}

impl Default for StatsState {
    fn default() -> Self {
        Self::new()
    }
}

impl LatchedErrorSummary {
    pub(crate) fn from_storage_error(error: &StorageError) -> Self {
        Self {
            kind: error.kind,
            operation: error.operation,
            protocol_stage: error.protocol_stage,
            retry_advice: error.retry_advice,
            os_code: error.os_code,
            commit_seq: error.commit_seq,
            vlog_file_id: error.vlog_file_id,
            vlog_offset: error.vlog_offset,
        }
    }
}
