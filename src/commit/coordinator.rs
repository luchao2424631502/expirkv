//! Commit ordering, compound publication, and in-memory commit state.
#![allow(dead_code)] // Public Db wiring is completed in a later stage.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::index::{IndexApplyState, IndexBackend, IndexCommitError, IndexCommitMode};
use crate::runtime::{RuntimeControl, WriteTicket};
use crate::stats::StatsState;
use crate::vlog::format::VLogPosition;
use crate::vlog::writer::{FrontierSync, ValueLogWriter};
use crate::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

use super::descriptor::{CommitSeq, DurableFrontier, DurableVLogEnd, TxUuid, VLogPos};
use super::durability::{DurabilityCoordinator, add_frontier_to_batch, frontier_only_batch};
use super::protocol::{TxUuidSource, ValidatedWrite, prepare_commit};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommitStateSnapshot {
    pub(crate) head_seq: CommitSeq,
    pub(crate) durable_seq: CommitSeq,
    pub(crate) head_vlog_end: Option<VLogPosition>,
    pub(crate) durable_vlog_end: Option<VLogPosition>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitState {
    head_seq: CommitSeq,
    durable_frontier: DurableFrontier,
    head_vlog_end: Option<VLogPosition>,
}

pub(crate) struct CommitCoordinator<B, U> {
    runtime: Arc<RuntimeControl>,
    stats: Arc<StatsState>,
    index: Arc<B>,
    writer: Mutex<ValueLogWriter>,
    uuid_source: Mutex<U>,
    state: Mutex<CommitState>,
    durability: DurabilityCoordinator,
}

impl<B, U> CommitCoordinator<B, U>
where
    B: IndexBackend,
    U: TxUuidSource + Send,
{
    pub(crate) fn new(
        runtime: Arc<RuntimeControl>,
        stats: Arc<StatsState>,
        index: Arc<B>,
        writer: ValueLogWriter,
        uuid_source: U,
        head_seq: CommitSeq,
        durable_frontier: DurableFrontier,
        head_vlog_end: Option<VLogPosition>,
    ) -> Result<Self> {
        validate_initial_state(head_seq, durable_frontier, head_vlog_end, &writer)?;
        let coordinator = Self {
            runtime,
            stats,
            index,
            writer: Mutex::new(writer),
            uuid_source: Mutex::new(uuid_source),
            state: Mutex::new(CommitState {
                head_seq,
                durable_frontier,
                head_vlog_end,
            }),
            durability: DurabilityCoordinator::new(),
        };
        coordinator.publish_stats();
        Ok(coordinator)
    }

    pub(crate) fn commit_nonempty(&self, write: &ValidatedWrite<'_>) -> Result<()> {
        if write.sync() {
            let _frontier = self.durability.lock();
            self.commit_started(write, true)
        } else {
            self.commit_started(write, false)
        }
    }

    pub(crate) fn commit_empty_batch(&self, sync: bool) -> Result<()> {
        if !sync {
            return self.runtime.check_write_admission(Operation::WriteBatch);
        }

        let _frontier = self.durability.lock();
        let ticket = self.start_request(Operation::WriteBatch)?;
        self.commit_empty_barrier(ticket)
    }

    pub(crate) fn state_snapshot(&self) -> CommitStateSnapshot {
        let state = lock(&self.state);
        snapshot(*state)
    }

    #[cfg(test)]
    pub(crate) fn dirty_state_for_test(&self) -> crate::vlog::writer::VLogDirtyState {
        lock(&self.writer).dirty_state().clone()
    }

    #[cfg(test)]
    pub(crate) fn publish_head_behind_durable_for_test(&self) {
        let mut state = lock(&self.state);
        state.durable_frontier.durable_seq = state.head_seq.saturating_add(1);
        drop(state);
        self.publish_stats();
    }

    fn commit_started(&self, write: &ValidatedWrite<'_>, sync: bool) -> Result<()> {
        let operation = write.public_operation();
        let ticket = self.start_request(operation)?;
        let state = *lock(&self.state);
        let mut writer = lock(&self.writer);
        let mut uuid_source = lock(&self.uuid_source);
        let preparation = prepare_commit(
            write,
            writer.database_uuid(),
            state.head_seq,
            writer.position(),
            writer.geometry(),
            self.index.as_ref(),
            &mut *uuid_source,
        );
        drop(uuid_source);
        let mut prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => {
                drop(writer);
                return Err(self.fail_started(ticket, error, None, None));
            }
        };

        let frontier = sync.then(|| frontier_for(prepared.commit_seq, prepared.envelope.vlog_end));
        if let Some(frontier) = frontier
            && let Err(error) =
                add_frontier_to_batch(&mut prepared.index_batch, frontier, operation)
        {
            drop(writer);
            return Err(self.fail_started(ticket, error, None, None));
        }

        let commit_seq = prepared.commit_seq;
        let tx_uuid = prepared.tx_uuid;
        let vlog_end = prepared.envelope.vlog_end;
        if let Err(error) = writer.append(&prepared.envelope) {
            let error = remap_writer_operation(error, operation);
            let target = classify_vlog_append_failure(&error, writer.has_terminal_failure());
            drop(writer);
            return Err(self.fail_started(
                ticket,
                error,
                Some(target),
                Some((commit_seq, tx_uuid)),
            ));
        }

        let synced = if sync {
            match writer.sync_through(commit_seq, Some(vlog_end)) {
                Ok(synced) => Some(synced),
                Err(error) => {
                    let error = remap_writer_operation(error, operation);
                    drop(writer);
                    return Err(self.fail_started(
                        ticket,
                        error,
                        Some(InstanceState::Poisoned),
                        Some((commit_seq, tx_uuid)),
                    ));
                }
            }
        } else {
            None
        };

        let mode = if sync {
            IndexCommitMode::SyncAll
        } else {
            IndexCommitMode::Buffer
        };
        match self.index.commit_atomic(prepared.index_batch, mode) {
            Ok(()) => {
                if let Some(synced) = synced {
                    self.finish_frontier_after_success(&mut writer, synced, operation);
                }
                drop(writer);
                self.publish_nonempty_success(commit_seq, vlog_end, frontier);
                let _ = ticket.finish();
                Ok(())
            }
            Err(error) => {
                if let Some(synced) = synced {
                    let _ = writer.frontier_failed(synced);
                }
                let (storage_error, target) = map_index_commit_error(
                    error,
                    operation,
                    ProtocolStage::IndexCommit,
                    Some((commit_seq, tx_uuid)),
                );
                drop(writer);
                Err(self.fail_started(ticket, storage_error, Some(target), None))
            }
        }
    }

    fn commit_empty_barrier(&self, ticket: WriteTicket) -> Result<()> {
        let captured = *lock(&self.state);
        if captured.head_seq == captured.durable_frontier.durable_seq {
            let _ = ticket.finish();
            return Ok(());
        }
        let target_end = captured.head_vlog_end;
        let frontier = frontier_from_optional(captured.head_seq, target_end);
        let batch = match frontier_only_batch(frontier, Operation::WriteBatch) {
            Ok(batch) => batch,
            Err(error) => return Err(self.fail_started(ticket, error, None, None)),
        };

        let mut writer = lock(&self.writer);
        let synced = match writer.sync_through(captured.head_seq, target_end) {
            Ok(synced) => synced,
            Err(error) => {
                let error = remap_writer_operation(error, Operation::WriteBatch);
                drop(writer);
                return Err(self.fail_started(ticket, error, Some(InstanceState::Poisoned), None));
            }
        };

        let current_durable = lock(&self.state).durable_frontier.durable_seq;
        if current_durable >= captured.head_seq {
            self.finish_frontier_after_success(&mut writer, synced, Operation::WriteBatch);
            drop(writer);
            let _ = ticket.finish();
            return Ok(());
        }

        match self.index.commit_atomic(batch, IndexCommitMode::SyncAll) {
            Ok(()) => {
                self.finish_frontier_after_success(&mut writer, synced, Operation::WriteBatch);
                drop(writer);
                self.publish_empty_barrier_success(frontier);
                let _ = ticket.finish();
                Ok(())
            }
            Err(error) => {
                let _ = writer.frontier_failed(synced);
                let (storage_error, target) = map_index_commit_error(
                    error,
                    Operation::WriteBatch,
                    ProtocolStage::DurableFrontier,
                    None,
                );
                drop(writer);
                Err(self.fail_started(ticket, storage_error, Some(target), None))
            }
        }
    }

    fn start_request(&self, operation: Operation) -> Result<WriteTicket> {
        let ticket = self.runtime.enqueue_write(operation)?;
        ticket.wait_until_started()?;
        Ok(ticket)
    }

    fn finish_frontier_after_success(
        &self,
        writer: &mut ValueLogWriter,
        synced: FrontierSync,
        operation: Operation,
    ) {
        if let Err(mut error) = writer.frontier_succeeded(synced) {
            error.operation = operation;
            error.instance_state = Some(InstanceState::Poisoned);
            error.retry_advice = RetryAdvice::ReopenAndVerify;
            self.runtime.latch_failure(InstanceState::Poisoned, &error);
        }
    }

    fn publish_nonempty_success(
        &self,
        commit_seq: CommitSeq,
        vlog_end: VLogPosition,
        frontier: Option<DurableFrontier>,
    ) {
        {
            let mut state = lock(&self.state);
            state.head_seq = commit_seq;
            state.head_vlog_end = Some(vlog_end);
            if let Some(frontier) = frontier {
                state.durable_frontier = frontier;
            }
        }
        self.publish_stats();
    }

    fn publish_empty_barrier_success(&self, frontier: DurableFrontier) {
        lock(&self.state).durable_frontier = frontier;
        self.publish_stats();
    }

    fn publish_stats(&self) {
        let state = *lock(&self.state);
        let writer = lock(&self.writer);
        let durable_end = descriptor_end_to_format(state.durable_frontier.durable_vlog_end);
        if !self.stats.update_commit_state(
            state.head_seq,
            state.durable_frontier.durable_seq,
            durable_end.map(|position| (position.file_id, position.offset)),
            writer.active_file_id(),
            writer.file_count(),
            writer.logical_bytes(),
        ) {
            self.runtime
                .latch_failure(InstanceState::Poisoned, &stats_invariant_error());
        }
    }

    fn fail_started(
        &self,
        ticket: WriteTicket,
        mut error: StorageError,
        requested_state: Option<InstanceState>,
        identity: Option<(CommitSeq, TxUuid)>,
    ) -> StorageError {
        if let Some((commit_seq, tx_uuid)) = identity {
            error.commit_seq = Some(commit_seq);
            error.tx_uuid = Some(tx_uuid.0);
        }

        let requested_state = requested_state
            .or(error.instance_state)
            .unwrap_or(InstanceState::Healthy);
        let target = self.runtime.required_failure_state(requested_state, &error);
        error.retry_advice = retry_for_state(&error, target);
        error.instance_state = Some(target);
        if target != InstanceState::Healthy {
            self.runtime.latch_failure(target, &error);
        }

        let state = self.runtime.state().instance_state;
        error.retry_advice = retry_for_state(&error, state);
        error.instance_state = Some(state);
        self.publish_stats();
        let _ = ticket.finish();
        error
    }
}

fn validate_initial_state(
    head_seq: CommitSeq,
    frontier: DurableFrontier,
    head_vlog_end: Option<VLogPosition>,
    writer: &ValueLogWriter,
) -> Result<()> {
    frontier
        .validate_against_head(head_seq)
        .map_err(|_| initial_state_error())?;
    let durable_end = descriptor_end_to_format(frontier.durable_vlog_end);
    let valid_empty = head_seq == 0
        && head_vlog_end.is_none()
        && frontier.durable_seq == 0
        && durable_end.is_none()
        && writer.position()
            == VLogPosition {
                file_id: 0,
                offset: 0,
            };
    let valid_nonempty = head_seq > 0
        && frontier.durable_seq == head_seq
        && head_vlog_end == Some(writer.position())
        && durable_end == head_vlog_end;
    if writer.database_uuid() == [0; 16] || !(valid_empty || valid_nonempty) {
        return Err(initial_state_error());
    }
    Ok(())
}

fn initial_state_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidLayout,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn frontier_for(seq: CommitSeq, end: VLogPosition) -> DurableFrontier {
    DurableFrontier {
        durable_seq: seq,
        durable_vlog_end: DurableVLogEnd::Position(VLogPos {
            file_id: end.file_id,
            offset: end.offset,
        }),
    }
}

fn frontier_from_optional(seq: CommitSeq, end: Option<VLogPosition>) -> DurableFrontier {
    match (seq, end) {
        (0, None) => DurableFrontier {
            durable_seq: 0,
            durable_vlog_end: DurableVLogEnd::Empty,
        },
        (_, Some(end)) => frontier_for(seq, end),
        _ => DurableFrontier {
            durable_seq: seq,
            durable_vlog_end: DurableVLogEnd::Empty,
        },
    }
}

fn descriptor_end_to_format(end: DurableVLogEnd) -> Option<VLogPosition> {
    match end {
        DurableVLogEnd::Empty => None,
        DurableVLogEnd::Position(position) => Some(VLogPosition {
            file_id: position.file_id,
            offset: position.offset,
        }),
    }
}

fn snapshot(state: CommitState) -> CommitStateSnapshot {
    CommitStateSnapshot {
        head_seq: state.head_seq,
        durable_seq: state.durable_frontier.durable_seq,
        head_vlog_end: state.head_vlog_end,
        durable_vlog_end: descriptor_end_to_format(state.durable_frontier.durable_vlog_end),
    }
}

fn classify_vlog_append_failure(error: &StorageError, terminal: bool) -> InstanceState {
    if !terminal {
        return InstanceState::Healthy;
    }
    if error.kind == StorageErrorKind::CapacityExceeded
        || matches!(error.os_code, Some(28 | 30 | 69 | 122))
    {
        InstanceState::WriteStopped
    } else {
        InstanceState::Poisoned
    }
}

fn remap_writer_operation(mut error: StorageError, operation: Operation) -> StorageError {
    error.operation = operation;
    error
}

fn map_index_commit_error(
    error: IndexCommitError,
    operation: Operation,
    stage: ProtocolStage,
    identity: Option<(CommitSeq, TxUuid)>,
) -> (StorageError, InstanceState) {
    let (outcome, state, retry) = match error.apply_state {
        IndexApplyState::Unknown => (
            WriteOutcome::CommitUnknown,
            InstanceState::Poisoned,
            RetryAdvice::ReopenAndVerify,
        ),
        IndexApplyState::NotApplied
            if matches!(
                error.source.kind,
                StorageErrorKind::Io
                    | StorageErrorKind::StoragePoisoned
                    | StorageErrorKind::Corruption
                    | StorageErrorKind::InvalidLayout
                    | StorageErrorKind::IncompatibleFormat
                    | StorageErrorKind::Unrecoverable
            ) =>
        {
            (
                WriteOutcome::NotCommitted,
                InstanceState::Poisoned,
                match error.source.kind {
                    StorageErrorKind::Corruption
                    | StorageErrorKind::InvalidLayout
                    | StorageErrorKind::Unrecoverable => RetryAdvice::RestoreOrRepair,
                    StorageErrorKind::IncompatibleFormat => RetryAdvice::DoNotRetry,
                    _ => RetryAdvice::ReopenAndVerify,
                },
            )
        }
        IndexApplyState::NotApplied => (
            WriteOutcome::NotCommitted,
            InstanceState::WriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
    };
    let mut mapped =
        StorageError::write_protocol(error.source.kind, operation, stage, outcome, state, retry);
    mapped.os_code = error.source.os_code;
    if let Some((commit_seq, tx_uuid)) = identity {
        mapped.commit_seq = Some(commit_seq);
        mapped.tx_uuid = Some(tx_uuid.0);
    }
    (mapped, state)
}

fn retry_for_state(error: &StorageError, state: InstanceState) -> RetryAdvice {
    if error.write_outcome == Some(WriteOutcome::CommitUnknown) {
        return RetryAdvice::ReopenAndVerify;
    }

    if error.instance_state == Some(state) {
        return error.retry_advice;
    }

    match state {
        InstanceState::Healthy => error.retry_advice,
        InstanceState::WriteStopped => RetryAdvice::FixEnvironmentAndReopen,
        InstanceState::Poisoned
            if matches!(
                error.kind,
                StorageErrorKind::Corruption
                    | StorageErrorKind::InvalidLayout
                    | StorageErrorKind::Unrecoverable
            ) =>
        {
            RetryAdvice::RestoreOrRepair
        }
        InstanceState::Poisoned if error.kind == StorageErrorKind::IncompatibleFormat => {
            RetryAdvice::DoNotRetry
        }
        InstanceState::Poisoned => RetryAdvice::ReopenAndVerify,
    }
}

fn stats_invariant_error() -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Corruption,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::RestoreOrRepair,
    );
    error.instance_state = Some(InstanceState::Poisoned);
    error
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
