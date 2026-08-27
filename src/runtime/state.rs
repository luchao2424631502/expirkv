//! Instance state, epoch, first-error latching, and state transitions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::stats::{LatchedErrorSummary, StatsState};
use crate::{
    InstanceState, Operation, ProtocolStage, Result, StorageError, StorageErrorKind, WriteOutcome,
};

use super::write_gate::{WriteGateInner, WriteRequest, WriteTicket, abandoned_active_write_error};

const STATE_BITS: u32 = 2;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateSnapshot {
    pub(crate) instance_state: InstanceState,
    pub(crate) state_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateTransition {
    pub(crate) previous: StateSnapshot,
    pub(crate) current: StateSnapshot,
    pub(crate) changed: bool,
    pub(crate) cancelled_writes: usize,
}

pub(crate) struct RuntimeControl {
    state_word: AtomicU64,
    first_latched_error: OnceLock<Arc<LatchedErrorSummary>>,
    gate: Mutex<WriteGateInner>,
    stats: Arc<StatsState>,
}

impl RuntimeControl {
    pub(crate) fn new(stats: Arc<StatsState>) -> Arc<Self> {
        let control = Arc::new(Self {
            state_word: AtomicU64::new(encode_state(StateSnapshot {
                instance_state: InstanceState::Healthy,
                state_epoch: 0,
            })),
            first_latched_error: OnceLock::new(),
            gate: Mutex::new(WriteGateInner::new()),
            stats,
        });
        control.publish_stats(StateSnapshot {
            instance_state: InstanceState::Healthy,
            state_epoch: 0,
        });
        control
    }

    pub(crate) fn state(&self) -> StateSnapshot {
        decode_state(self.state_word.load(Ordering::Acquire))
    }

    pub(crate) fn first_latched_error(&self) -> Option<Arc<LatchedErrorSummary>> {
        self.first_latched_error.get().cloned()
    }

    /// Confirms a no-queue write completion point under the same gate used by
    /// ordinary write admission. Empty non-sync batches use this path.
    pub(crate) fn check_write_admission(&self, operation: Operation) -> Result<()> {
        let fast_state = self.state();
        if fast_state.instance_state != InstanceState::Healthy {
            return Err(super::write_gate::admission_error(
                operation,
                fast_state.instance_state,
            ));
        }

        let gate = lock_gate(&self.gate);
        let checked_state = self.state();
        if checked_state.instance_state != InstanceState::Healthy || !gate.accepting_writes {
            return Err(super::write_gate::admission_error(
                operation,
                checked_state.instance_state,
            ));
        }
        Ok(())
    }

    pub(crate) fn enqueue_write(self: &Arc<Self>, operation: Operation) -> Result<WriteTicket> {
        let fast_state = self.state();
        if fast_state.instance_state != InstanceState::Healthy {
            return Err(super::write_gate::admission_error(
                operation,
                fast_state.instance_state,
            ));
        }

        let request = WriteRequest::new(operation);
        let starts_now = {
            let mut gate = lock_gate(&self.gate);
            let checked_state = self.state();
            if checked_state.instance_state != InstanceState::Healthy || !gate.accepting_writes {
                return Err(super::write_gate::admission_error(
                    operation,
                    checked_state.instance_state,
                ));
            }

            if gate.active_request.is_none() && gate.ordered_queue.is_empty() {
                gate.active_request = Some(request.id());
                true
            } else {
                gate.ordered_queue.push_back(Arc::clone(&request));
                false
            }
        };

        if starts_now {
            request.mark_started();
        }
        Ok(WriteTicket::new(Arc::clone(self), request))
    }

    pub(crate) fn required_failure_state(
        &self,
        target: InstanceState,
        error: &StorageError,
    ) -> InstanceState {
        let required = if error.write_outcome == Some(WriteOutcome::CommitUnknown)
            || error.protocol_stage == ProtocolStage::VLogSync
            || matches!(
                error.kind,
                StorageErrorKind::Corruption
                    | StorageErrorKind::InvalidLayout
                    | StorageErrorKind::IncompatibleFormat
                    | StorageErrorKind::StoragePoisoned
                    | StorageErrorKind::Unrecoverable
            ) {
            InstanceState::Poisoned
        } else if error.kind == StorageErrorKind::StorageWriteStopped {
            InstanceState::WriteStopped
        } else {
            InstanceState::Healthy
        };

        if state_rank(required) > state_rank(target) {
            required
        } else {
            target
        }
    }

    pub(crate) fn latch_failure(
        &self,
        target: InstanceState,
        error: &StorageError,
    ) -> StateTransition {
        let target = self.required_failure_state(target, error);
        let summary = LatchedErrorSummary::from_storage_error(error);
        let (transition, cancelled) = {
            let mut gate = lock_gate(&self.gate);
            let previous = self.state();
            if !is_upgrade(previous.instance_state, target) {
                return StateTransition {
                    previous,
                    current: previous,
                    changed: false,
                    cancelled_writes: 0,
                };
            }

            gate.accepting_writes = false;
            if previous.instance_state == InstanceState::Healthy {
                let _ = self.first_latched_error.set(Arc::new(summary));
            }
            let current = StateSnapshot {
                instance_state: target,
                state_epoch: previous.state_epoch.saturating_add(1),
            };
            self.state_word
                .store(encode_state(current), Ordering::Release);
            let cancelled = std::mem::take(&mut gate.ordered_queue);
            let transition = StateTransition {
                previous,
                current,
                changed: true,
                cancelled_writes: cancelled.len(),
            };
            (transition, cancelled)
        };

        for request in cancelled {
            request.cancel_for_state(target);
        }
        self.publish_stats(transition.current);
        transition
    }

    pub(crate) fn stats(&self) -> crate::DbStats {
        let state = self.state();
        let mut snapshot = self.stats.snapshot();
        snapshot.instance_state = state.instance_state;
        snapshot.state_epoch = state.state_epoch;
        snapshot.first_latched_error = if state.instance_state == InstanceState::Healthy {
            None
        } else {
            self.first_latched_error
                .get()
                .map(|summary| summary.as_ref().clone())
        };
        snapshot
    }

    pub(super) fn cancel_queued_write(&self, request: &Arc<WriteRequest>) -> bool {
        let removed = {
            let mut gate = lock_gate(&self.gate);
            let Some(position) = gate
                .ordered_queue
                .iter()
                .position(|queued| queued.id() == request.id())
            else {
                return false;
            };
            gate.ordered_queue.remove(position)
        };

        if let Some(removed) = removed {
            removed.cancel_while_healthy();
            true
        } else {
            false
        }
    }

    pub(super) fn abandon_write(&self, request: &Arc<WriteRequest>) {
        let (removed, was_active) = {
            let mut gate = lock_gate(&self.gate);
            let removed = gate
                .ordered_queue
                .iter()
                .position(|queued| queued.id() == request.id())
                .and_then(|position| gate.ordered_queue.remove(position));
            let was_active = removed.is_none() && gate.active_request == Some(request.id());
            (removed, was_active)
        };

        if let Some(removed) = removed {
            removed.cancel_while_healthy();
            return;
        }
        if !was_active {
            return;
        }

        let error = abandoned_active_write_error(request.operation());
        self.latch_failure(InstanceState::Poisoned, &error);
        let cleared = self.finish_write(request);
        debug_assert!(
            cleared,
            "active write must remain owned until it is cleared"
        );
    }

    pub(super) fn finish_write(&self, request: &Arc<WriteRequest>) -> bool {
        let next = {
            let mut gate = lock_gate(&self.gate);
            if gate.active_request != Some(request.id()) {
                return false;
            }
            gate.active_request = None;

            let state = self.state();
            if state.instance_state == InstanceState::Healthy && gate.accepting_writes {
                if let Some(next) = gate.ordered_queue.pop_front() {
                    gate.active_request = Some(next.id());
                    Some(next)
                } else {
                    None
                }
            } else {
                None
            }
        };

        request.mark_completed();
        if let Some(next) = next {
            next.mark_started();
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn active_request_for_test(&self) -> Option<super::write_gate::RequestId> {
        lock_gate(&self.gate).active_request
    }

    #[cfg(test)]
    pub(crate) fn queued_write_count_for_test(&self) -> usize {
        lock_gate(&self.gate).ordered_queue.len()
    }

    fn publish_stats(&self, snapshot: StateSnapshot) {
        self.stats.update_runtime_state(
            snapshot.instance_state,
            snapshot.state_epoch,
            self.first_latched_error
                .get()
                .map(|summary| summary.as_ref().clone()),
        );
    }
}

fn lock_gate(gate: &Mutex<WriteGateInner>) -> std::sync::MutexGuard<'_, WriteGateInner> {
    match gate.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn state_rank(state: InstanceState) -> u8 {
    match state {
        InstanceState::Healthy => 0,
        InstanceState::WriteStopped => 1,
        InstanceState::Poisoned => 2,
    }
}

fn is_upgrade(current: InstanceState, target: InstanceState) -> bool {
    state_rank(target) > state_rank(current)
}

fn encode_state(snapshot: StateSnapshot) -> u64 {
    let state = match snapshot.instance_state {
        InstanceState::Healthy => 0,
        InstanceState::WriteStopped => 1,
        InstanceState::Poisoned => 2,
    };
    (snapshot.state_epoch << STATE_BITS) | state
}

fn decode_state(word: u64) -> StateSnapshot {
    let instance_state = match word & STATE_MASK {
        0 => InstanceState::Healthy,
        1 => InstanceState::WriteStopped,
        _ => InstanceState::Poisoned,
    };
    StateSnapshot {
        instance_state,
        state_epoch: word >> STATE_BITS,
    }
}
