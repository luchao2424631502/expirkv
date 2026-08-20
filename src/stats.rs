//! Public statistics data and private in-memory snapshots.

use std::sync::RwLock;

use crate::{InstanceState, Operation, ProtocolStage, RetryAdvice, StorageErrorKind};

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
    pub(crate) fn snapshot(&self) -> DbStats {
        match self.snapshot.read() {
            Ok(snapshot) => snapshot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}
