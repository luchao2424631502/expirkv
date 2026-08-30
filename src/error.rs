//! Structured public errors, write outcomes, and retry advice.

use std::error::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    NotCommitted,
    CommitUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstanceState {
    Healthy,
    WriteStopped,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAdvice {
    FixRequestAndRetrySameInstance,
    RetrySameInstance,
    FixEnvironmentAndReopen,
    ReopenAndVerify,
    RestoreOrRepair,
    DoNotRetry,
}

// 大致的存储错误
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageErrorKind {
    InvalidArgument,
    NotFound,
    Busy,
    Unsupported,
    ResourceExhausted,
    CapacityExceeded,
    Io,
    Corruption,
    InvalidLayout,
    IncompatibleFormat,
    StorageWriteStopped,
    StoragePoisoned,
    Unrecoverable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Open,
    Put,
    Delete,
    WriteBatch,
    Get,
    Snapshot,
    Iterator,
    Range,
    Sync,
    Destroy,
    Drop,
    Background,
    Recovery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolStage {
    Admission,
    Preflight,
    VLogAppend,
    VLogSync,
    IndexCommit,
    DurableFrontier,
    Read,
    Recovery,
    Maintenance,
    Lifecycle,
}

pub enum ManagedObject {
    Lock,
    Format,
    FormatTemporary,
    DatabaseIdentity,
    IndexDirectory,
    VLogDirectory,
    VLogFile { file_id: u32 },
}

pub enum DestroyStage {
    AcquireLock,
    Inventory,
    RemoveFile,
    RemoveTree,
    SyncDirectory,
}

pub struct DestroyFailureContext {
    pub failed_object: ManagedObject,
    pub stage: DestroyStage,
    pub partially_deleted: bool,
    pub os_code: Option<i32>,
}

// 具体的存储错误
pub struct StorageError {
    pub schema_version: u16,
    pub kind: StorageErrorKind,
    pub operation: Operation,
    pub protocol_stage: ProtocolStage,
    pub write_outcome: Option<WriteOutcome>,
    pub instance_state: Option<InstanceState>,
    pub retry_advice: RetryAdvice,
    pub os_code: Option<i32>,
    pub commit_seq: Option<u64>,
    pub tx_uuid: Option<[u8; 16]>,
    pub vlog_file_id: Option<u32>,
    pub vlog_offset: Option<u64>,
    pub destroy_failure: Option<DestroyFailureContext>,
    pub message: String,
    pub source: Option<Box<dyn Error + Send + Sync>>,
}

impl StorageError {
    pub(crate) fn unsupported(
        operation: Operation,
        protocol_stage: ProtocolStage,
        instance_state: Option<InstanceState>,
    ) -> Self {
        let write_outcome = matches!(
            operation,
            Operation::Put | Operation::Delete | Operation::WriteBatch | Operation::Sync
        )
        .then_some(WriteOutcome::NotCommitted);

        Self::new(
            StorageErrorKind::Unsupported,
            operation,
            protocol_stage,
            write_outcome,
            instance_state,
            RetryAdvice::DoNotRetry,
        )
    }

    pub(crate) fn invalid_batch(kind: StorageErrorKind, retry_advice: RetryAdvice) -> Self {
        Self::new(
            kind,
            Operation::WriteBatch,
            ProtocolStage::Preflight,
            Some(WriteOutcome::NotCommitted),
            None,
            retry_advice,
        )
    }

    pub(crate) fn write_preflight(
        kind: StorageErrorKind,
        operation: Operation,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::write_preflight_in_state(kind, operation, InstanceState::Healthy, retry_advice)
    }

    pub(crate) fn write_preflight_in_state(
        kind: StorageErrorKind,
        operation: Operation,
        instance_state: InstanceState,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::new(
            kind,
            operation,
            ProtocolStage::Preflight,
            Some(WriteOutcome::NotCommitted),
            Some(instance_state),
            retry_advice,
        )
    }

    pub(crate) fn write_protocol(
        kind: StorageErrorKind,
        operation: Operation,
        protocol_stage: ProtocolStage,
        write_outcome: WriteOutcome,
        instance_state: InstanceState,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::new(
            kind,
            operation,
            protocol_stage,
            Some(write_outcome),
            Some(instance_state),
            retry_advice,
        )
    }

    pub(crate) fn read_error(
        kind: StorageErrorKind,
        instance_state: InstanceState,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::read_operation_error(kind, Operation::Get, instance_state, retry_advice)
    }

    pub(crate) fn read_operation_error(
        kind: StorageErrorKind,
        operation: Operation,
        instance_state: InstanceState,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::new(
            kind,
            operation,
            ProtocolStage::Read,
            None,
            Some(instance_state),
            retry_advice,
        )
    }

    #[allow(dead_code)] // Stage 2 codecs are connected to production paths in later stages.
    pub(crate) fn codec_error(
        kind: StorageErrorKind,
        operation: Operation,
        protocol_stage: ProtocolStage,
        write_outcome: Option<WriteOutcome>,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self::new(
            kind,
            operation,
            protocol_stage,
            write_outcome,
            None,
            retry_advice,
        )
    }

    fn new(
        kind: StorageErrorKind,
        operation: Operation,
        protocol_stage: ProtocolStage,
        write_outcome: Option<WriteOutcome>,
        instance_state: Option<InstanceState>,
        retry_advice: RetryAdvice,
    ) -> Self {
        Self {
            schema_version: 1,
            kind,
            operation,
            protocol_stage,
            write_outcome,
            instance_state,
            retry_advice,
            os_code: None,
            commit_seq: None,
            tx_uuid: None,
            vlog_file_id: None,
            vlog_offset: None,
            destroy_failure: None,
            message: String::new(),
            source: None,
        }
    }
}

impl fmt::Debug for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageError")
            .field("schema_version", &self.schema_version)
            .field("kind", &self.kind)
            .field("operation", &self.operation)
            .field("protocol_stage", &self.protocol_stage)
            .field("write_outcome", &self.write_outcome)
            .field("instance_state", &self.instance_state)
            .field("retry_advice", &self.retry_advice)
            .field("os_code", &self.os_code)
            .field("commit_seq", &self.commit_seq)
            .field("tx_uuid", &self.tx_uuid)
            .field("vlog_file_id", &self.vlog_file_id)
            .field("vlog_offset", &self.vlog_offset)
            .field("destroy_failure", &self.destroy_failure.is_some())
            .field("message", &self.message)
            .field("source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}
