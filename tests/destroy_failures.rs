#![allow(dead_code, unused_imports)]

use std::fs;
use std::path::{Path, PathBuf};

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    DestroyFailureContext, DestroyStage, InstanceState, ManagedObject, Operation, ProtocolStage,
    Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};
#[path = "../src/snapshot.rs"]
mod snapshot;
pub(crate) use snapshot::Snapshot;
#[path = "../src/cursor.rs"]
mod cursor;
pub(crate) use cursor::{DbIterator, KeyRange, RangeCursor};
#[path = "../src/batch.rs"]
mod batch;
pub(crate) use batch::WriteBatch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/options.rs"]
mod options;
pub(crate) use options::{Options, ReadOptions, WriteOptions};
#[path = "../src/db.rs"]
mod db;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/recovery/mod.rs"]
mod recovery;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use db::{DestroyFaultPoint, destroy_with_fault_for_test};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn prepare_final_database(root: &Path) -> TestResult {
    let options = ::rustkv::Options {
        create_if_missing: true,
        ..::rustkv::Options::default()
    };
    let database = ::rustkv::Db::open(&options, root)?;
    database.put(
        &::rustkv::WriteOptions { sync: true },
        b"key",
        &vec![0x3C; 40_000],
    )?;
    drop(database);
    fs::write(root.join("unmanaged.txt"), b"keep")?;
    Ok(())
}

fn prepare_temporary_database(root: &Path) -> TestResult {
    let options = ::rustkv::Options {
        create_if_missing: true,
        ..::rustkv::Options::default()
    };
    let database = ::rustkv::Db::open(&options, root)?;
    drop(database);
    fs::rename(root.join("FORMAT"), root.join("FORMAT.tmp"))?;
    fs::write(root.join("unmanaged.txt"), b"keep")?;
    Ok(())
}

fn assert_fault_context(error: &StorageError, fault: DestroyFaultPoint, partial: bool) {
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Destroy);
    assert_eq!(error.protocol_stage, ProtocolStage::Lifecycle);
    assert_eq!(error.os_code, Some(5));
    let context = error.destroy_failure.as_ref().expect("destroy context");
    assert_eq!(context.os_code, Some(5));
    assert_eq!(context.partially_deleted, partial);
    match fault {
        DestroyFaultPoint::Inventory => {
            assert!(matches!(&context.failed_object, ManagedObject::Format));
            assert!(matches!(&context.stage, DestroyStage::Inventory));
        }
        DestroyFaultPoint::DatabaseIdentity => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::DatabaseIdentity
            ));
            assert!(matches!(&context.stage, DestroyStage::Inventory));
        }
        DestroyFaultPoint::VLogFileRemove => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::VLogFile { file_id: 0 }
            ));
            assert!(matches!(&context.stage, DestroyStage::RemoveFile));
        }
        DestroyFaultPoint::VLogDirectorySync => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::VLogDirectory
            ));
            assert!(matches!(&context.stage, DestroyStage::SyncDirectory));
        }
        DestroyFaultPoint::VLogDirectoryRemove => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::VLogDirectory
            ));
            assert!(matches!(&context.stage, DestroyStage::RemoveTree));
        }
        DestroyFaultPoint::VLogRootSync => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::VLogDirectory
            ));
            assert!(matches!(&context.stage, DestroyStage::SyncDirectory));
        }
        DestroyFaultPoint::IndexRemove | DestroyFaultPoint::IndexRemoveAfterEntry => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::IndexDirectory
            ));
            assert!(matches!(&context.stage, DestroyStage::RemoveTree));
        }
        DestroyFaultPoint::IndexRootSync => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::IndexDirectory
            ));
            assert!(matches!(&context.stage, DestroyStage::SyncDirectory));
        }
        DestroyFaultPoint::FormatTemporaryRemove => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::FormatTemporary
            ));
            assert!(matches!(&context.stage, DestroyStage::RemoveFile));
        }
        DestroyFaultPoint::FormatTemporarySync => {
            assert!(matches!(
                &context.failed_object,
                ManagedObject::FormatTemporary
            ));
            assert!(matches!(&context.stage, DestroyStage::SyncDirectory));
        }
        DestroyFaultPoint::FormatRemove => {
            assert!(matches!(&context.failed_object, ManagedObject::Format));
            assert!(matches!(&context.stage, DestroyStage::RemoveFile));
        }
        DestroyFaultPoint::FormatSync | DestroyFaultPoint::EmptyInventoryRootSync => {
            assert!(matches!(&context.failed_object, ManagedObject::Format));
            assert!(matches!(&context.stage, DestroyStage::SyncDirectory));
        }
        DestroyFaultPoint::None => panic!("None is not a fault"),
    }
}

fn verify_retry(root: &Path) -> TestResult {
    destroy_with_fault_for_test(root, &Options::default(), DestroyFaultPoint::None)?;
    assert!(root.is_dir());
    assert!(root.join("LOCK").is_file());
    assert_eq!(fs::read(root.join("unmanaged.txt"))?, b"keep");
    assert!(!root.join("FORMAT").exists());
    assert!(!root.join("FORMAT.tmp").exists());
    assert!(!root.join("index").exists());
    assert!(!root.join("vlog").exists());
    destroy_with_fault_for_test(root, &Options::default(), DestroyFaultPoint::None)?;
    Ok(())
}

#[test]
fn every_final_database_destroy_fault_reports_partial_state_and_retries() -> TestResult {
    let temporary = TempDir::new()?;
    let cases = [
        (DestroyFaultPoint::Inventory, false),
        (DestroyFaultPoint::DatabaseIdentity, false),
        (DestroyFaultPoint::VLogFileRemove, false),
        (DestroyFaultPoint::VLogDirectorySync, true),
        (DestroyFaultPoint::VLogDirectoryRemove, true),
        (DestroyFaultPoint::VLogRootSync, true),
        (DestroyFaultPoint::IndexRemove, true),
        (DestroyFaultPoint::IndexRootSync, true),
        (DestroyFaultPoint::FormatRemove, true),
        (DestroyFaultPoint::FormatSync, true),
    ];
    for (ordinal, (fault, partial)) in cases.into_iter().enumerate() {
        let root = temporary.path().join(format!("final-{ordinal}"));
        prepare_final_database(&root)?;
        let error = destroy_with_fault_for_test(&root, &Options::default(), fault)
            .expect_err("injected Destroy fault must fail");
        assert_fault_context(&error, fault, partial);
        assert_eq!(fs::read(root.join("unmanaged.txt"))?, b"keep");
        assert!(root.join("LOCK").is_file());
        verify_retry(&root)?;
    }
    Ok(())
}

#[test]
fn format_temporary_remove_and_sync_faults_are_retryable() -> TestResult {
    let temporary = TempDir::new()?;
    for (ordinal, fault) in [
        DestroyFaultPoint::FormatTemporaryRemove,
        DestroyFaultPoint::FormatTemporarySync,
    ]
    .into_iter()
    .enumerate()
    {
        let root = temporary.path().join(format!("temporary-{ordinal}"));
        prepare_temporary_database(&root)?;
        let error = destroy_with_fault_for_test(&root, &Options::default(), fault)
            .expect_err("injected FORMAT.tmp fault must fail");
        assert_fault_context(&error, fault, true);
        verify_retry(&root)?;
    }
    Ok(())
}

#[test]
fn missing_vlog_allows_retry_to_remove_an_index_that_can_no_longer_validate_identity() -> TestResult
{
    let temporary = TempDir::new()?;
    let root = temporary.path().join("partial-index");
    prepare_final_database(&root)?;

    let error =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::IndexRemove)
            .expect_err("the injected index removal fault must interrupt Destroy");
    assert_fault_context(&error, DestroyFaultPoint::IndexRemove, true);
    assert!(!root.join("vlog").exists());
    assert!(root.join("index").is_dir());
    assert!(root.join("FORMAT").is_file());

    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let system = database.keyspace("rustkv_system_metadata", || {
        KeyspaceCreateOptions::default().manual_journal_persist(true)
    })?;
    system.remove(b"database_identity")?;
    database.persist(PersistMode::SyncAll)?;
    drop(system);
    drop(database);

    verify_retry(&root)
}

#[test]
fn format_sync_retry_reissues_the_missing_root_directory_sync() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("format-sync-retry");
    prepare_final_database(&root)?;

    let first =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::FormatSync)
            .expect_err("the first root sync must fail after FORMAT removal");
    assert_fault_context(&first, DestroyFaultPoint::FormatSync, true);
    assert!(!root.join("FORMAT").exists());
    assert!(!root.join("index").exists());
    assert!(!root.join("vlog").exists());

    let retry = destroy_with_fault_for_test(
        &root,
        &Options::default(),
        DestroyFaultPoint::EmptyInventoryRootSync,
    )
    .expect_err("an empty retry must execute, not skip, the root sync");
    assert_fault_context(&retry, DestroyFaultPoint::EmptyInventoryRootSync, false);

    verify_retry(&root)
}

#[test]
fn vlog_root_sync_retry_reissues_the_barrier_before_index_removal() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("vlog-root-sync-retry");
    prepare_final_database(&root)?;

    let first =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::VLogRootSync)
            .expect_err("the first root sync must fail after vlog removal");
    assert_fault_context(&first, DestroyFaultPoint::VLogRootSync, true);
    assert!(!root.join("vlog").exists());
    assert!(root.join("index").is_dir());
    assert!(root.join("FORMAT").is_file());

    let retry =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::VLogRootSync)
            .expect_err("retry must reissue the vlog root sync before removing index");
    assert_fault_context(&retry, DestroyFaultPoint::VLogRootSync, false);
    assert!(root.join("index").is_dir());
    assert!(root.join("FORMAT").is_file());

    verify_retry(&root)
}

#[test]
fn index_root_sync_retry_reissues_the_barrier_before_format_removal() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("index-root-sync-retry");
    prepare_final_database(&root)?;

    let first =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::IndexRootSync)
            .expect_err("the first root sync must fail after index removal");
    assert_fault_context(&first, DestroyFaultPoint::IndexRootSync, true);
    assert!(!root.join("vlog").exists());
    assert!(!root.join("index").exists());
    assert!(root.join("FORMAT").is_file());

    let retry =
        destroy_with_fault_for_test(&root, &Options::default(), DestroyFaultPoint::IndexRootSync)
            .expect_err("retry must reissue the index root sync before removing FORMAT");
    assert_fault_context(&retry, DestroyFaultPoint::IndexRootSync, false);
    assert!(root.join("FORMAT").is_file());

    verify_retry(&root)
}

#[test]
fn recursive_index_failure_reports_deletions_made_inside_the_tree() -> TestResult {
    let temporary = TempDir::new()?;
    let root = temporary.path().join("partial-index-tree");
    prepare_final_database(&root)?;
    fs::remove_dir_all(root.join("vlog"))?;

    let error = destroy_with_fault_for_test(
        &root,
        &Options::default(),
        DestroyFaultPoint::IndexRemoveAfterEntry,
    )
    .expect_err("the injected recursive index removal must fail");
    assert_fault_context(&error, DestroyFaultPoint::IndexRemoveAfterEntry, true);
    assert!(root.join("FORMAT").is_file());
    assert!(root.join("index").is_dir());

    verify_retry(&root)
}

#[test]
fn read_only_shadow_drop_retries_after_reported_remove_failure() -> TestResult {
    let (error, retained_after_failure, exists_after_drop) =
        index::shadow_remove_retry_probe_for_test()?;
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.os_code, Some(5));
    assert!(retained_after_failure);
    assert!(!exists_after_drop);
    Ok(())
}
