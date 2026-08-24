#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};

#[path = "../src/runtime/mod.rs"]
mod runtime;

use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use runtime::{LifecycleController, RuntimeControl};
use stats::StatsState;

fn runtime() -> Arc<RuntimeControl> {
    RuntimeControl::new(Arc::new(StatsState::new()))
}

fn join_before_deadline(handle: thread::JoinHandle<()>, deadline: Instant, label: &str) {
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "{label} did not terminate before deadline"
        );
        thread::yield_now();
    }
    handle.join().unwrap_or_else(|_| panic!("{label} panicked"));
}

fn structured_failure(
    kind: StorageErrorKind,
    operation: Operation,
    protocol_stage: ProtocolStage,
    write_outcome: Option<WriteOutcome>,
    retry_advice: RetryAdvice,
) -> StorageError {
    let mut error =
        StorageError::codec_error(kind, operation, protocol_stage, write_outcome, retry_advice);
    error.os_code = Some(5);
    error.commit_seq = Some(41);
    error.vlog_file_id = Some(7);
    error.vlog_offset = Some(1024);
    error
}

fn write_stopped_failure(operation: Operation) -> StorageError {
    structured_failure(
        StorageErrorKind::Io,
        operation,
        ProtocolStage::VLogAppend,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    )
}

fn corruption_failure(operation: Operation) -> StorageError {
    structured_failure(
        StorageErrorKind::Corruption,
        operation,
        ProtocolStage::Read,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn commit_unknown_failure(operation: Operation) -> StorageError {
    structured_failure(
        StorageErrorKind::Io,
        operation,
        ProtocolStage::IndexCommit,
        Some(WriteOutcome::CommitUnknown),
        RetryAdvice::ReopenAndVerify,
    )
}

fn vlog_sync_failure(operation: Operation) -> StorageError {
    structured_failure(
        StorageErrorKind::Io,
        operation,
        ProtocolStage::VLogSync,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::ReopenAndVerify,
    )
}

#[test]
fn state_transitions_are_monotonic_and_increment_epoch_once_per_upgrade() {
    let runtime = runtime();
    assert_eq!(runtime.state().instance_state, InstanceState::Healthy);
    assert_eq!(runtime.state().state_epoch, 0);

    let first = write_stopped_failure(Operation::Put);
    let stopped = runtime.latch_failure(InstanceState::WriteStopped, &first);
    assert!(stopped.changed);
    assert_eq!(stopped.previous.instance_state, InstanceState::Healthy);
    assert_eq!(stopped.current.instance_state, InstanceState::WriteStopped);
    assert_eq!(stopped.current.state_epoch, 1);

    let duplicate = runtime.latch_failure(InstanceState::WriteStopped, &first);
    assert!(!duplicate.changed);
    assert_eq!(duplicate.current.state_epoch, 1);

    let illegal = runtime.latch_failure(InstanceState::Healthy, &first);
    assert!(!illegal.changed);
    assert_eq!(illegal.current.instance_state, InstanceState::WriteStopped);
    assert_eq!(illegal.current.state_epoch, 1);

    let second = corruption_failure(Operation::Get);
    let poisoned = runtime.latch_failure(InstanceState::Poisoned, &second);
    assert!(poisoned.changed);
    assert_eq!(
        poisoned.previous.instance_state,
        InstanceState::WriteStopped
    );
    assert_eq!(poisoned.current.instance_state, InstanceState::Poisoned);
    assert_eq!(poisoned.current.state_epoch, 2);

    let no_recovery = runtime.latch_failure(InstanceState::Healthy, &second);
    assert!(!no_recovery.changed);
    assert_eq!(runtime.state().instance_state, InstanceState::Poisoned);
    assert_eq!(runtime.state().state_epoch, 2);

    let no_downgrade = runtime.latch_failure(InstanceState::WriteStopped, &second);
    assert!(!no_downgrade.changed);
    assert_eq!(runtime.state().instance_state, InstanceState::Poisoned);
    assert_eq!(runtime.state().state_epoch, 2);
}

#[test]
fn direct_healthy_to_poisoned_transition_is_legal() {
    let runtime = runtime();
    let error = corruption_failure(Operation::Get);

    let transition = runtime.latch_failure(InstanceState::Poisoned, &error);

    assert!(transition.changed);
    assert_eq!(transition.previous.instance_state, InstanceState::Healthy);
    assert_eq!(transition.current.instance_state, InstanceState::Poisoned);
    assert_eq!(transition.current.state_epoch, 1);
}

#[test]
fn only_the_first_reason_for_leaving_healthy_is_latched_and_published_to_stats() {
    let runtime = runtime();
    let first = write_stopped_failure(Operation::Put);
    let second = corruption_failure(Operation::Get);

    runtime.latch_failure(InstanceState::WriteStopped, &first);
    runtime.latch_failure(InstanceState::Poisoned, &second);

    let latched = runtime.first_latched_error().expect("first error latched");
    assert_eq!(latched.kind, StorageErrorKind::Io);
    assert_eq!(latched.operation, Operation::Put);
    assert_eq!(latched.protocol_stage, ProtocolStage::VLogAppend);
    assert_eq!(latched.os_code, Some(5));
    assert_eq!(latched.commit_seq, Some(41));
    assert_eq!(latched.vlog_file_id, Some(7));
    assert_eq!(latched.vlog_offset, Some(1024));

    let stats = runtime.stats();
    assert_eq!(stats.schema_version, 1);
    assert_eq!(stats.instance_state, InstanceState::Poisoned);
    assert_eq!(stats.state_epoch, 2);
    assert_eq!(
        stats.first_latched_error.expect("stats error").kind,
        StorageErrorKind::Io
    );
}

#[test]
fn nonhealthy_instances_reject_new_writes_with_not_committed() {
    for (state, expected_kind, expected_retry) in [
        (
            InstanceState::WriteStopped,
            StorageErrorKind::StorageWriteStopped,
            RetryAdvice::FixEnvironmentAndReopen,
        ),
        (
            InstanceState::Poisoned,
            StorageErrorKind::StoragePoisoned,
            RetryAdvice::ReopenAndVerify,
        ),
    ] {
        let runtime = runtime();
        let failure = match state {
            InstanceState::WriteStopped => write_stopped_failure(Operation::Put),
            InstanceState::Poisoned => corruption_failure(Operation::Get),
            InstanceState::Healthy => unreachable!(),
        };
        runtime.latch_failure(state, &failure);

        let error = match runtime.enqueue_write(Operation::Delete) {
            Ok(_) => panic!("nonhealthy runtime admitted a write"),
            Err(error) => error,
        };
        assert_eq!(error.kind, expected_kind);
        assert_eq!(error.operation, Operation::Delete);
        assert_eq!(error.protocol_stage, ProtocolStage::Admission);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(state));
        assert_eq!(error.retry_advice, expected_retry);
    }
}

#[test]
fn mandatory_error_state_combinations_fail_closed() {
    for error in [
        commit_unknown_failure(Operation::WriteBatch),
        vlog_sync_failure(Operation::WriteBatch),
        corruption_failure(Operation::Get),
    ] {
        let runtime = runtime();
        let transition = runtime.latch_failure(InstanceState::WriteStopped, &error);
        assert!(transition.changed);
        assert_eq!(transition.current.instance_state, InstanceState::Poisoned);
        assert_eq!(transition.current.state_epoch, 1);
    }

    let runtime = runtime();
    let write_stopped = structured_failure(
        StorageErrorKind::StorageWriteStopped,
        Operation::Put,
        ProtocolStage::Admission,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixEnvironmentAndReopen,
    );
    let transition = runtime.latch_failure(InstanceState::Healthy, &write_stopped);
    assert!(transition.changed);
    assert_eq!(
        transition.current.instance_state,
        InstanceState::WriteStopped
    );
}

#[test]
fn write_gate_starts_exactly_one_request_and_preserves_fifo_order() {
    let runtime = runtime();
    let first = runtime.enqueue_write(Operation::Put).expect("first");
    let second = runtime.enqueue_write(Operation::Delete).expect("second");
    let third = runtime.enqueue_write(Operation::WriteBatch).expect("third");

    assert!(
        first
            .wait_until_started_timeout(Duration::ZERO)
            .expect("first state")
    );
    assert!(
        !second
            .wait_until_started_timeout(Duration::from_millis(10))
            .expect("second remains queued")
    );
    assert!(
        !third
            .wait_until_started_timeout(Duration::from_millis(10))
            .expect("third remains queued")
    );

    assert!(first.finish());
    assert!(
        second
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect("second starts")
    );
    assert!(
        !third
            .wait_until_started_timeout(Duration::from_millis(10))
            .expect("third cannot bypass second")
    );

    assert!(second.finish());
    assert!(
        third
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect("third starts")
    );
    assert!(third.finish());
}

#[test]
fn cancelling_a_queued_request_reports_not_committed_without_stopping_runtime() {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let queued = runtime.enqueue_write(Operation::Delete).expect("queued");

    assert!(!active.cancel_queued());
    assert!(queued.cancel_queued());
    let error = queued
        .wait_until_started_timeout(Duration::from_millis(100))
        .expect_err("queued cancellation");
    assert_eq!(error.kind, StorageErrorKind::Busy);
    assert_eq!(error.protocol_stage, ProtocolStage::Admission);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(InstanceState::Healthy));
    assert_eq!(runtime.state().instance_state, InstanceState::Healthy);

    assert!(active.finish());
    let next = runtime.enqueue_write(Operation::Put).expect("new active");
    assert!(
        next.wait_until_started_timeout(Duration::from_millis(100))
            .expect("new active starts")
    );
    assert!(next.finish());
}

#[test]
fn dropping_a_queued_follower_removes_it_without_stopping_or_blocking_the_gate() {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let dropped = runtime
        .enqueue_write(Operation::Delete)
        .expect("follower to drop");
    let survivor = runtime
        .enqueue_write(Operation::WriteBatch)
        .expect("surviving follower");

    assert!(
        !dropped
            .wait_until_started_timeout(Duration::from_millis(10))
            .expect("follower remains queued before drop")
    );
    let dropped_id = dropped.request_id();
    let survivor_id = survivor.request_id();
    assert_ne!(dropped_id, survivor_id);

    drop(dropped);

    let state = runtime.state();
    assert_eq!(state.instance_state, InstanceState::Healthy);
    assert_eq!(state.state_epoch, 0);
    assert!(runtime.first_latched_error().is_none());

    assert!(active.finish());
    assert!(
        survivor
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect("surviving follower becomes leader")
    );
    assert_eq!(runtime.active_request_for_test(), Some(survivor_id));
    assert!(survivor.finish());
    assert!(runtime.active_request_for_test().is_none());

    let next = runtime.enqueue_write(Operation::Put).expect("new write");
    assert!(
        next.wait_until_started_timeout(Duration::from_millis(100))
            .expect("gate remains usable")
    );
    assert!(next.finish());
}

#[test]
fn dropping_an_active_ticket_poisons_and_unblocks_the_gate_fail_closed() {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let follower = runtime.enqueue_write(Operation::Delete).expect("follower");

    drop(active);

    let state = runtime.state();
    assert_eq!(state.instance_state, InstanceState::Poisoned);
    assert_eq!(state.state_epoch, 1);
    assert!(runtime.active_request_for_test().is_none());

    let error = follower
        .wait_until_started_timeout(Duration::from_millis(100))
        .expect_err("follower must be cancelled");
    assert_eq!(error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));

    let error = match runtime.enqueue_write(Operation::WriteBatch) {
        Ok(_) => panic!("poisoned runtime admitted another write"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));

    let first = runtime.first_latched_error().expect("active drop latched");
    assert_eq!(first.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(first.operation, Operation::Put);
    assert_eq!(first.protocol_stage, ProtocolStage::Lifecycle);
}

#[test]
fn state_failure_cancels_followers_but_does_not_terminate_the_active_request() {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let follower_one = runtime
        .enqueue_write(Operation::Delete)
        .expect("follower one");
    let follower_two = runtime
        .enqueue_write(Operation::WriteBatch)
        .expect("follower two");

    let transition = runtime.latch_failure(
        InstanceState::WriteStopped,
        &write_stopped_failure(Operation::Put),
    );
    assert_eq!(transition.cancelled_writes, 2);

    assert!(
        active
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect("active remains started")
    );
    for follower in [follower_one, follower_two] {
        let error = follower
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect_err("follower must be cancelled");
        assert_eq!(error.kind, StorageErrorKind::StorageWriteStopped);
        assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
        assert_eq!(error.instance_state, Some(InstanceState::WriteStopped));
    }
    assert!(active.finish());
}

#[test]
fn fifo_handoff_wakes_all_waiters_without_lost_notifications() {
    const FOLLOWERS: usize = 24;
    const DEADLINE: Duration = Duration::from_secs(5);

    let runtime = runtime();
    let leader = runtime.enqueue_write(Operation::Put).expect("leader");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let mut threads = Vec::new();

    for index in 0..FOLLOWERS {
        let ticket = runtime.enqueue_write(Operation::Put).expect("follower");
        let observed_tx = observed_tx.clone();
        let completed_tx = completed_tx.clone();
        threads.push(thread::spawn(move || {
            assert!(
                ticket
                    .wait_until_started_timeout(DEADLINE)
                    .expect("follower starts before deadline")
            );
            observed_tx.send(index).expect("report follower order");
            assert!(ticket.finish());
            completed_tx.send(()).expect("report follower completion");
        }));
    }
    drop(observed_tx);
    drop(completed_tx);

    assert!(leader.finish());
    let deadline = Instant::now() + DEADLINE;
    for _ in 0..FOLLOWERS {
        completed_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("all followers complete before deadline");
    }
    for thread in threads {
        join_before_deadline(thread, deadline, "follower thread");
    }
    let order = (0..FOLLOWERS)
        .map(|_| {
            observed_rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("all follower order observations arrive before deadline")
        })
        .collect::<Vec<_>>();
    assert_eq!(order, (0..FOLLOWERS).collect::<Vec<_>>());
}

#[test]
fn concurrent_admission_never_starts_a_follower_after_failure_publication() {
    const ATTEMPTS: usize = 64;
    const DEADLINE: Duration = Duration::from_secs(5);

    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let (ready_tx, ready_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let mut start_senders = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..ATTEMPTS {
        let runtime = Arc::clone(&runtime);
        let ready_tx = ready_tx.clone();
        let result_tx = result_tx.clone();
        let (start_tx, start_rx) = mpsc::channel();
        start_senders.push(start_tx);
        threads.push(thread::spawn(move || {
            ready_tx.send(()).expect("report admission thread ready");
            start_rx
                .recv_timeout(DEADLINE)
                .expect("admission start signal before deadline");
            result_tx
                .send(runtime.enqueue_write(Operation::Put))
                .expect("report admission result");
        }));
    }
    drop(ready_tx);
    drop(result_tx);

    let ready_deadline = Instant::now() + DEADLINE;
    for _ in 0..ATTEMPTS {
        ready_rx
            .recv_timeout(ready_deadline.saturating_duration_since(Instant::now()))
            .expect("all admission threads ready before deadline");
    }
    for start in start_senders {
        start.send(()).expect("start admission thread");
    }
    runtime.latch_failure(
        InstanceState::WriteStopped,
        &write_stopped_failure(Operation::Put),
    );

    let result_deadline = Instant::now() + DEADLINE;
    for _ in 0..ATTEMPTS {
        match result_rx
            .recv_timeout(result_deadline.saturating_duration_since(Instant::now()))
            .expect("all admission results arrive before deadline")
        {
            Ok(ticket) => {
                let error = ticket
                    .wait_until_started_timeout(Duration::from_millis(100))
                    .expect_err("pre-publication follower is cancelled");
                assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
            }
            Err(error) => {
                assert_eq!(error.kind, StorageErrorKind::StorageWriteStopped);
                assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
            }
        }
    }
    for thread in threads {
        join_before_deadline(thread, result_deadline, "admission thread");
    }
    assert!(active.finish());
}

#[test]
fn stats_remain_self_consistent_while_state_is_upgraded() {
    const DEADLINE: Duration = Duration::from_secs(5);

    let runtime = runtime();
    let (start_tx, start_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let writer_runtime = Arc::clone(&runtime);
    let writer = thread::spawn(move || {
        start_rx
            .recv_timeout(DEADLINE)
            .expect("state writer starts before deadline");
        writer_runtime.latch_failure(
            InstanceState::WriteStopped,
            &write_stopped_failure(Operation::Put),
        );
        writer_runtime.latch_failure(InstanceState::Poisoned, &corruption_failure(Operation::Get));
        completed_tx
            .send(())
            .expect("report state writer completion");
    });

    start_tx.send(()).expect("start state writer");
    let deadline = Instant::now() + DEADLINE;
    let mut previous_epoch = 0;
    loop {
        let snapshot = runtime.stats();
        assert!(snapshot.state_epoch >= previous_epoch);
        previous_epoch = snapshot.state_epoch;
        match snapshot.instance_state {
            InstanceState::Healthy => {
                assert_eq!(snapshot.state_epoch, 0);
                assert!(snapshot.first_latched_error.is_none());
            }
            InstanceState::WriteStopped => {
                assert_eq!(snapshot.state_epoch, 1);
                assert_eq!(
                    snapshot.first_latched_error.expect("first error").kind,
                    StorageErrorKind::Io
                );
            }
            InstanceState::Poisoned => {
                assert_eq!(snapshot.state_epoch, 2);
                assert_eq!(
                    snapshot.first_latched_error.expect("first error").kind,
                    StorageErrorKind::Io
                );
                break;
            }
        }
        assert!(Instant::now() < deadline, "state transition timed out");
        thread::yield_now();
    }
    completed_rx
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .expect("state writer completes before deadline");
    join_before_deadline(writer, deadline, "state writer");
}

#[test]
fn last_external_lease_closes_admission_and_resources_wait_for_operation_guards() {
    let (lifecycle, lease) = LifecycleController::new_with_external_lease();
    let second_lease = lease.clone();
    let operation = lifecycle.acquire_operation().expect("operation guard");

    let snapshot = lifecycle.snapshot();
    assert!(snapshot.accepting_operations);
    assert_eq!(snapshot.external_leases, 2);
    assert_eq!(snapshot.operation_guards, 1);

    drop(lease);
    assert!(lifecycle.snapshot().accepting_operations);
    drop(second_lease);
    assert!(!lifecycle.snapshot().accepting_operations);
    assert!(lifecycle.acquire_operation().is_none());
    assert!(!lifecycle.wait_for_quiescence(Duration::from_millis(20)));

    drop(operation);
    assert!(lifecycle.wait_for_quiescence(Duration::from_millis(100)));
    let snapshot = lifecycle.snapshot();
    assert_eq!(snapshot.external_leases, 0);
    assert_eq!(snapshot.operation_guards, 0);
}

#[test]
fn lifecycle_is_quiescent_regardless_of_guard_and_last_lease_release_order() {
    let (lifecycle, lease) = LifecycleController::new_with_external_lease();
    let operation = lifecycle.acquire_operation().expect("operation guard");

    drop(operation);
    assert!(!lifecycle.wait_for_quiescence(Duration::from_millis(10)));
    assert!(lifecycle.snapshot().accepting_operations);

    drop(lease);
    assert!(lifecycle.wait_for_quiescence(Duration::from_millis(100)));
    assert!(!lifecycle.snapshot().accepting_operations);
}

#[test]
fn waits_have_bounded_timeout_behavior() {
    let runtime = runtime();
    let active = runtime.enqueue_write(Operation::Put).expect("active");
    let queued = runtime.enqueue_write(Operation::Put).expect("queued");

    let started_at = Instant::now();
    assert!(
        !queued
            .wait_until_started_timeout(Duration::from_millis(25))
            .expect("timeout")
    );
    let elapsed = started_at.elapsed();
    assert!(elapsed >= Duration::from_millis(15));
    assert!(elapsed < Duration::from_secs(1));

    assert!(active.finish());
    assert!(
        queued
            .wait_until_started_timeout(Duration::from_millis(100))
            .expect("queued starts")
    );
    assert!(queued.finish());
}
