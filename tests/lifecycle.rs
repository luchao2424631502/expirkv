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

use std::sync::Arc;
use std::time::Duration;
use std::{panic::AssertUnwindSafe, panic::catch_unwind};

use fjall::{Database, KeyspaceCreateOptions};
use runtime::{LifecycleController, RuntimeControl};
use stats::StatsState;
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn runtime() -> Arc<RuntimeControl> {
    RuntimeControl::new(Arc::new(StatsState::new()))
}

#[test]
fn closing_healthy_write_stopped_and_poisoned_runtime_never_panics_or_clears_first_error() {
    for target in [
        InstanceState::Healthy,
        InstanceState::WriteStopped,
        InstanceState::Poisoned,
    ] {
        let runtime = runtime();
        let (lifecycle, lease) = LifecycleController::new_with_external_lease();
        assert!(lifecycle.bind_runtime(Arc::clone(&runtime)));
        if target != InstanceState::Healthy {
            let kind = if target == InstanceState::WriteStopped {
                StorageErrorKind::StorageWriteStopped
            } else {
                StorageErrorKind::Corruption
            };
            let error = StorageError::codec_error(
                kind,
                Operation::Background,
                ProtocolStage::Maintenance,
                None,
                RetryAdvice::ReopenAndVerify,
            );
            runtime.latch_failure(target, &error);
        }
        let first_before = runtime.first_latched_error();
        assert!(catch_unwind(AssertUnwindSafe(|| drop(lease))).is_ok());
        assert_eq!(runtime.state().instance_state, target);
        let first_after = runtime.first_latched_error();
        match (first_before, first_after) {
            (None, None) => {}
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, &after)),
            _ => panic!("shutdown changed the first latched error"),
        }
    }
}

#[test]
fn last_external_lease_closes_admission_cancels_queued_and_waits_for_started() {
    let runtime = runtime();
    let (lifecycle, first_lease) = LifecycleController::new_with_external_lease();
    assert!(lifecycle.bind_runtime(Arc::clone(&runtime)));
    let second_lease = first_lease.clone();
    let operation = lifecycle.acquire_operation().expect("operation admitted");
    let started = runtime
        .enqueue_write(Operation::Put)
        .expect("started write");
    assert!(
        started
            .wait_until_started_timeout(Duration::from_secs(1))
            .unwrap()
    );
    let queued = runtime
        .enqueue_write(Operation::Delete)
        .expect("queued write");

    drop(first_lease);
    assert!(lifecycle.snapshot().accepting_operations);
    assert!(runtime.accepting_writes_for_test());

    drop(second_lease);
    let closed = lifecycle.snapshot();
    assert!(!closed.accepting_operations);
    assert_eq!(closed.external_leases, 0);
    assert_eq!(closed.operation_guards, 1);
    assert!(!runtime.accepting_writes_for_test());
    assert_eq!(
        runtime.active_request_for_test(),
        Some(started.request_id())
    );
    assert_eq!(runtime.queued_write_count_for_test(), 0);

    let cancelled = queued.wait_until_started().unwrap_err();
    assert_eq!(cancelled.kind, StorageErrorKind::Busy);
    assert_eq!(cancelled.protocol_stage, ProtocolStage::Admission);
    assert_eq!(cancelled.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(cancelled.instance_state, Some(InstanceState::Healthy));
    assert_eq!(runtime.state().instance_state, InstanceState::Healthy);
    assert_eq!(runtime.state().state_epoch, 0);

    assert!(!lifecycle.wait_for_quiescence(Duration::from_millis(1)));
    assert!(started.finish());
    assert!(!lifecycle.wait_for_quiescence(Duration::from_millis(1)));
    drop(operation);
    assert!(lifecycle.wait_for_quiescence(Duration::from_secs(1)));
}

#[test]
fn snapshot_iterator_and_range_each_keep_the_database_lock_alive() -> TestResult {
    let temporary = TempDir::new()?;
    let path = temporary.path().join("db");
    let options = rustkv::Options {
        create_if_missing: true,
        ..rustkv::Options::default()
    };
    let db = rustkv::Db::open(&options, &path)?;
    db.put(&rustkv::WriteOptions::default(), b"a", b"one")?;
    db.put(&rustkv::WriteOptions::default(), b"b", b"two")?;
    let snapshot = db.snapshot()?;
    let iterator = db.iter(&rustkv::ReadOptions::default())?;
    let range = db.range(
        &rustkv::ReadOptions::default(),
        rustkv::KeyRange {
            start: Some(b"a"),
            end: Some(b"z"),
        },
        10,
    )?;
    let clone = db.clone();

    drop(db);
    assert_eq!(
        rustkv::Db::destroy(&path, &rustkv::Options::default())
            .unwrap_err()
            .kind,
        rustkv::StorageErrorKind::Busy
    );
    drop(clone);
    drop(snapshot);
    drop(iterator);
    assert_eq!(
        rustkv::Db::destroy(&path, &rustkv::Options::default())
            .unwrap_err()
            .kind,
        rustkv::StorageErrorKind::Busy
    );
    drop(range);
    rustkv::Db::destroy(&path, &rustkv::Options::default())?;
    Ok(())
}

#[test]
fn drop_does_not_turn_an_async_write_into_a_durability_barrier() -> TestResult {
    let temporary = TempDir::new()?;
    let path = temporary.path().join("db");
    let create = rustkv::Options {
        create_if_missing: true,
        ..rustkv::Options::default()
    };
    let db = rustkv::Db::open(&create, &path)?;
    db.put(&rustkv::WriteOptions { sync: false }, b"volatile", b"value")?;
    assert_eq!(db.stats().head_seq, 1);
    assert_eq!(db.stats().durable_seq, 0);
    drop(db);

    // Inspect the persisted metadata before RustKV Open recovery is allowed to
    // promote a physically complete asynchronous commit.
    let index = Database::builder(path.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let system = index.keyspace("rustkv_system_metadata", KeyspaceCreateOptions::default)?;
    let frontier = system
        .get(b"durable_frontier")?
        .expect("durable frontier")
        .to_vec();
    let durable_seq = u64::from_le_bytes(frontier[6..14].try_into()?);
    assert_eq!(durable_seq, 0);
    Ok(())
}
