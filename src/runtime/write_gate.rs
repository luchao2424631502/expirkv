//! Write admission, ordered queuing, started boundaries, and wakeups.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

use super::RuntimeControl;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestId(u64);

pub(super) struct WriteGateInner {
    pub(super) accepting_writes: bool,
    pub(super) ordered_queue: VecDeque<Arc<WriteRequest>>,
    pub(super) active_request: Option<RequestId>,
}

impl WriteGateInner {
    pub(super) fn new() -> Self {
        Self {
            accepting_writes: true,
            ordered_queue: VecDeque::new(),
            active_request: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestPhase {
    Queued,
    Started,
    Cancelled(InstanceState),
    Completed,
}

pub(super) struct WriteRequest {
    id: RequestId,
    operation: Operation,
    phase: Mutex<RequestPhase>,
    changed: Condvar,
}

impl WriteRequest {
    pub(super) fn new(operation: Operation) -> Arc<Self> {
        Arc::new(Self {
            id: RequestId(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)),
            operation,
            phase: Mutex::new(RequestPhase::Queued),
            changed: Condvar::new(),
        })
    }

    pub(super) fn id(&self) -> RequestId {
        self.id
    }

    pub(super) fn operation(&self) -> Operation {
        self.operation
    }

    pub(super) fn mark_started(&self) {
        let mut phase = lock_phase(&self.phase);
        if *phase == RequestPhase::Queued {
            *phase = RequestPhase::Started;
            drop(phase);
            self.changed.notify_all();
        }
    }

    pub(super) fn mark_completed(&self) {
        let mut phase = lock_phase(&self.phase);
        if *phase == RequestPhase::Started {
            *phase = RequestPhase::Completed;
            drop(phase);
            self.changed.notify_all();
        }
    }

    pub(super) fn cancel_for_state(&self, state: InstanceState) {
        let mut phase = lock_phase(&self.phase);
        if *phase == RequestPhase::Queued {
            *phase = RequestPhase::Cancelled(state);
            drop(phase);
            self.changed.notify_all();
        }
    }

    pub(super) fn cancel_while_healthy(&self) {
        self.cancel_for_state(InstanceState::Healthy);
    }

    fn wait_until_started(&self) -> Result<()> {
        let mut phase = lock_phase(&self.phase);
        loop {
            match *phase {
                RequestPhase::Started | RequestPhase::Completed => return Ok(()),
                RequestPhase::Cancelled(state) => {
                    return Err(cancelled_error(self.operation, state));
                }
                RequestPhase::Queued => {
                    phase = match self.changed.wait(phase) {
                        Ok(phase) => phase,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                }
            }
        }
    }

    fn wait_until_started_timeout(&self, timeout: Duration) -> Result<bool> {
        let started_at = Instant::now();
        let mut phase = lock_phase(&self.phase);
        loop {
            match *phase {
                RequestPhase::Started | RequestPhase::Completed => return Ok(true),
                RequestPhase::Cancelled(state) => {
                    return Err(cancelled_error(self.operation, state));
                }
                RequestPhase::Queued => {}
            }

            let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
                return Ok(false);
            };
            if remaining.is_zero() {
                return Ok(false);
            }
            let (next_phase, wait_result) = match self.changed.wait_timeout(phase, remaining) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            phase = next_phase;
            if wait_result.timed_out() && *phase == RequestPhase::Queued {
                return Ok(false);
            }
        }
    }
}

pub(crate) struct WriteTicket {
    runtime: Arc<RuntimeControl>,
    request: Arc<WriteRequest>,
    released: bool,
}

impl WriteTicket {
    pub(super) fn new(runtime: Arc<RuntimeControl>, request: Arc<WriteRequest>) -> Self {
        Self {
            runtime,
            request,
            released: false,
        }
    }

    pub(crate) fn request_id(&self) -> RequestId {
        self.request.id()
    }

    pub(crate) fn wait_until_started(&self) -> Result<()> {
        self.request.wait_until_started()
    }

    pub(crate) fn wait_until_started_timeout(&self, timeout: Duration) -> Result<bool> {
        self.request.wait_until_started_timeout(timeout)
    }

    pub(crate) fn cancel_queued(&self) -> bool {
        self.runtime.cancel_queued_write(&self.request)
    }

    pub(crate) fn finish(mut self) -> bool {
        let finished = self.runtime.finish_write(&self.request);
        self.released = finished;
        finished
    }
}

impl Drop for WriteTicket {
    fn drop(&mut self) {
        if !self.released {
            self.runtime.abandon_write(&self.request);
            self.released = true;
        }
    }
}

pub(super) fn abandoned_active_write_error(operation: Operation) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        operation,
        ProtocolStage::Lifecycle,
        Some(WriteOutcome::CommitUnknown),
        RetryAdvice::ReopenAndVerify,
    );
    error.instance_state = Some(InstanceState::Poisoned);
    error
}

pub(super) fn admission_error(operation: Operation, state: InstanceState) -> StorageError {
    let (kind, retry_advice) = match state {
        InstanceState::Healthy => (StorageErrorKind::Busy, RetryAdvice::RetrySameInstance),
        InstanceState::WriteStopped => (
            StorageErrorKind::StorageWriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
        InstanceState::Poisoned => (
            StorageErrorKind::StoragePoisoned,
            RetryAdvice::ReopenAndVerify,
        ),
    };
    let mut error = StorageError::codec_error(
        kind,
        operation,
        ProtocolStage::Admission,
        Some(WriteOutcome::NotCommitted),
        retry_advice,
    );
    error.instance_state = Some(state);
    error
}

fn cancelled_error(operation: Operation, state: InstanceState) -> StorageError {
    admission_error(operation, state)
}

fn lock_phase(phase: &Mutex<RequestPhase>) -> std::sync::MutexGuard<'_, RequestPhase> {
    match phase.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
