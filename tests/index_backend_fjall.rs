#![allow(dead_code)]

use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use fjall::{Database, KeyspaceCreateOptions};
use tempfile::TempDir;

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
pub(crate) mod vlog_format;

mod vlog {
    pub(crate) use crate::vlog_format as format;
}

#[path = "../src/commit/descriptor.rs"]
mod descriptor;

pub struct Snapshot;

#[path = "../src/options.rs"]
mod options;

#[path = "../src/index/mod.rs"]
mod index;

use index::*;
use options::{Compression, Options};

type TestResult = std::result::Result<(), Box<dyn Error + Send + Sync>>;

const USER_INDEX_NAME: &str = "rustkv_user_index";
const TX_METADATA_NAME: &str = "rustkv_txn_metadata";
const SYSTEM_METADATA_NAME: &str = "rustkv_system_metadata";
const CRASH_CHILD_ENV: &str = "RUSTKV_INDEX_BACKEND_CHILD";
const CRASH_PATH_ENV: &str = "RUSTKV_INDEX_BACKEND_PATH";
const CRASH_MODE_ENV: &str = "RUSTKV_INDEX_BACKEND_MODE";
const CRASH_EXIT_CODE: i32 = 47;

fn default_index_options() -> FjallIndexOptions {
    Options::default().fjall_index_options()
}

fn encoded_initial_metadata(uuid_byte: u8) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let identity = descriptor::DatabaseIdentity {
        identity_format_version: 0,
        database_format_version: 0,
        database_uuid: [uuid_byte; 16],
        keyspace_layout_version: 0,
    }
    .encode()
    .unwrap()
    .to_vec();
    let head_seq = descriptor::encode_head_seq(0).to_vec();
    let frontier = descriptor::DurableFrontier {
        durable_seq: 0,
        durable_vlog_end: descriptor::DurableVLogEnd::Empty,
    }
    .encode()
    .unwrap()
    .to_vec();
    (identity, head_seq, frontier)
}

fn initialization_batch(uuid_byte: u8) -> IndexAtomicBatch {
    let (identity, head_seq, frontier) = encoded_initial_metadata(uuid_byte);
    IndexAtomicBatch::initialize_database(identity, head_seq, frontier).unwrap()
}

fn initialize(backend: &FjallBackend, uuid_byte: u8) -> Vec<u8> {
    let (identity, head_seq, frontier) = encoded_initial_metadata(uuid_byte);
    let batch =
        IndexAtomicBatch::initialize_database(identity.clone(), head_seq, frontier).unwrap();
    backend
        .commit_atomic(batch, IndexCommitMode::SyncAll)
        .unwrap();
    identity
}

fn encoded_pointer(file_id: u32, value_len: u16) -> Vec<u8> {
    vlog_format::ValuePointer {
        format_version: 0,
        file_id,
        record_offset: 64,
        record_len: 64,
        value_len,
    }
    .encode()
    .unwrap()
    .to_vec()
}

fn transaction_batch(seq: u64, pointer: Vec<u8>) -> IndexAtomicBatch {
    let mut batch = IndexAtomicBatch::new();
    batch
        .try_push(IndexMutation::PutUser {
            user_key: b"user-key".to_vec(),
            encoded_pointer: pointer,
        })
        .unwrap();
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key: b"tx/current".to_vec(),
            value: format!("descriptor-{seq}").into_bytes(),
        })
        .unwrap();
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: b"head_seq".to_vec(),
            value: seq.to_le_bytes().to_vec(),
        })
        .unwrap();
    batch
}

fn assert_send_sync<T: Send + Sync>() {}

fn expect_backend_error(result: Result<FjallBackend>) -> StorageError {
    match result {
        Ok(_) => panic!("backend open should fail"),
        Err(error) => error,
    }
}

fn directory_shape(root: &Path) -> io::Result<Vec<(PathBuf, u8)>> {
    fn visit(root: &Path, current: &Path, shape: &mut Vec<(PathBuf, u8)>) -> io::Result<()> {
        for entry in std::fs::read_dir(current)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let kind = if file_type.is_file() {
                0
            } else if file_type.is_dir() {
                1
            } else if file_type.is_symlink() {
                2
            } else {
                3
            };
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("visited path must stay below the inventory root")
                .to_path_buf();
            shape.push((relative, kind));
            if file_type.is_dir() {
                visit(root, &path, shape)?;
            }
        }
        Ok(())
    }

    let mut shape = Vec::new();
    visit(root, root, &mut shape)?;
    shape.sort();
    Ok(shape)
}

fn assert_open_rejected_without_shape_change(
    path: &Path,
    expected_kind: StorageErrorKind,
) -> TestResult {
    let before = directory_shape(path)?;
    let error = expect_backend_error(FjallBackend::open_existing(path, default_index_options()));
    assert_eq!(error.kind, expected_kind);
    assert_eq!(directory_shape(path)?, before);
    Ok(())
}

#[test]
fn fixed_topology_and_public_options_map_to_fjall() -> TestResult {
    assert_send_sync::<FjallBackend>();

    let folder = TempDir::new()?;
    let index_path = folder.path().join("index");
    let mut options = Options::default();
    options.write_buffer_size = 8 * 1_024 * 1_024;
    options.max_open_files = 32;
    options.block_cache_size = 0;
    options.block_size = 8 * 1_024;
    options.block_restart_interval = 12;
    options.max_file_size = 4 * 1_024 * 1_024;
    options.compression = Compression::Lz4;
    let expected = options.fjall_index_options();

    let backend = FjallBackend::create(&index_path, expected)?;
    assert_eq!(backend.cache_capacity(), 0);
    assert_eq!(
        backend.user_keyspace_configuration(),
        TestUserKeyspaceConfiguration {
            block_sizes: vec![8 * 1_024],
            block_restart_intervals: vec![12],
            compressions: vec![TestCompression::Lz4],
            compaction_strategy: "LeveledCompaction".to_owned(),
            table_target_size: Some(4 * 1_024 * 1_024),
        }
    );
    assert_eq!(
        backend.keyspace_names(),
        vec![
            SYSTEM_METADATA_NAME.to_owned(),
            TX_METADATA_NAME.to_owned(),
            USER_INDEX_NAME.to_owned(),
        ]
    );
    assert!(backend.all_keyspaces_disable_kv_separation());

    let persisted_keyspace_configuration = backend.user_keyspace_configuration();
    drop(backend);

    let mut reopen_options = Options::default();
    reopen_options.block_cache_size = 2 * 1_024 * 1_024;
    let reopened = FjallBackend::open_existing(&index_path, reopen_options.fjall_index_options())?;
    assert_eq!(reopened.cache_capacity(), 2 * 1_024 * 1_024);
    assert_eq!(
        reopened.user_keyspace_configuration(),
        persisted_keyspace_configuration,
        "reopen must retain Fjall's persisted creation-time Keyspace settings"
    );
    drop(reopened);

    let no_compression = FjallBackend::create(
        &folder.path().join("no-compression"),
        default_index_options(),
    )?;
    assert_eq!(
        no_compression.user_keyspace_configuration().compressions,
        vec![TestCompression::None]
    );
    drop(no_compression);

    let invalid_path = folder.path().join("invalid-options");
    let mut invalid = expected;
    invalid.max_open_files = 9;
    let error = expect_backend_error(FjallBackend::create(&invalid_path, invalid));
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert!(!invalid_path.exists());
    Ok(())
}

#[test]
fn block_size_accepts_fjall_upper_boundary_and_rejects_larger_values() -> TestResult {
    let folder = TempDir::new()?;
    let maximum = 4 * 1_024 * 1_024;
    let mut options = Options::default();
    options.block_size = maximum;

    let backend = FjallBackend::create(
        &folder.path().join("maximum-block-size"),
        options.fjall_index_options(),
    )?;
    assert_eq!(
        backend.user_keyspace_configuration().block_sizes,
        vec![maximum as u32]
    );
    drop(backend);

    options.block_size = maximum + 1;
    let invalid_path = folder.path().join("oversized-block");
    let error = expect_backend_error(FjallBackend::create(
        &invalid_path,
        options.fjall_index_options(),
    ));
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert!(!invalid_path.exists());
    Ok(())
}

#[test]
fn max_open_files_controls_the_real_shared_descriptor_cache() -> TestResult {
    let folder = TempDir::new()?;
    let index_path = folder.path().join("index");
    let mut create_options = Options::default();
    create_options.max_open_files = 64;
    let backend = FjallBackend::create(&index_path, create_options.fjall_index_options())?;
    initialize(&backend, 0x4a);

    for index in 0_u32..24 {
        let mut batch = IndexAtomicBatch::new();
        batch
            .try_push(IndexMutation::PutUser {
                user_key: format!("descriptor-cache-{index:03}").into_bytes(),
                encoded_pointer: encoded_pointer(index, 1),
            })
            .unwrap();
        backend
            .commit_atomic(batch, IndexCommitMode::Buffer)
            .unwrap();
        backend.rotate_user_memtable_and_wait()?;
    }
    drop(backend);

    let mut reopen_options = Options::default();
    reopen_options.max_open_files = 10;
    let reopened = FjallBackend::open_existing(&index_path, reopen_options.fjall_index_options())?;
    let table_count = reopened.user_table_count();
    assert!(table_count > 10);
    for index in 0_u32..24 {
        assert_eq!(
            reopened.get_user(format!("descriptor-cache-{index:03}").as_bytes(), None)?,
            Some(encoded_pointer(index, 1))
        );
    }
    let entries = reopened.iter_user(None)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 24);
    let low_limit_cache_size = reopened.descriptor_cache_size();
    assert!(low_limit_cache_size < table_count);
    drop(reopened);

    reopen_options.max_open_files = 64;
    let reopened = FjallBackend::open_existing(&index_path, reopen_options.fjall_index_options())?;
    let entries = reopened.iter_user(None)?.collect::<Result<Vec<_>>>()?;
    assert_eq!(entries.len(), 24);
    assert!(reopened.descriptor_cache_size() > low_limit_cache_size);
    Ok(())
}

#[test]
fn max_file_size_rejects_values_that_overflow_fjall_leveled_arithmetic() -> TestResult {
    let folder = TempDir::new()?;
    let maximum = FjallBackend::max_table_target_size();
    let accepted = usize::try_from(maximum).unwrap_or(usize::MAX);
    let mut options = Options::default();
    options.max_file_size = accepted;
    let backend = FjallBackend::create(
        &folder.path().join("maximum-safe"),
        options.fjall_index_options(),
    )?;
    assert_eq!(
        backend.user_keyspace_configuration().table_target_size,
        Some(accepted as u64)
    );
    drop(backend);

    if let Some(too_large) = maximum
        .checked_add(1)
        .and_then(|value| usize::try_from(value).ok())
    {
        let invalid_path = folder.path().join("overflowing");
        options.max_file_size = too_large;
        let error = expect_backend_error(FjallBackend::create(
            &invalid_path,
            options.fjall_index_options(),
        ));
        assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
        assert!(!invalid_path.exists());
    }
    Ok(())
}

#[test]
fn open_existing_never_creates_database_or_missing_keyspaces() -> TestResult {
    let folder = TempDir::new()?;
    let missing = folder.path().join("missing-index");

    let mut invalid_options = default_index_options();
    invalid_options.max_open_files = 9;
    let error = expect_backend_error(FjallBackend::open_existing(&missing, invalid_options));
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert!(!missing.exists());

    let error = expect_backend_error(FjallBackend::open_existing(
        &missing,
        default_index_options(),
    ));
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert!(!missing.exists());

    let partial = folder.path().join("partial-index");
    let database = Database::builder(&partial)
        .manual_journal_persist(true)
        .open()?;
    database.keyspace(USER_INDEX_NAME, KeyspaceCreateOptions::default)?;
    drop(database);

    let error = expect_backend_error(FjallBackend::create(&partial, default_index_options()));
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);

    let error = expect_backend_error(FjallBackend::open_existing(
        &partial,
        default_index_options(),
    ));
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);

    let database = Database::builder(&partial)
        .manual_journal_persist(true)
        .open()?;
    let names = database
        .list_keyspace_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![USER_INDEX_NAME.to_owned()]);
    Ok(())
}

#[test]
fn open_existing_rejects_damaged_fjall_layout_before_recovery_can_mutate_it() -> TestResult {
    let folder = TempDir::new()?;

    let missing_journal = folder.path().join("missing-journal");
    let backend = FjallBackend::create(&missing_journal, default_index_options())?;
    initialize(&backend, 0x4b);
    drop(backend);
    for entry in std::fs::read_dir(&missing_journal)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|ext| ext == "jnl") {
            std::fs::remove_file(entry.path())?;
        }
    }
    assert_open_rejected_without_shape_change(&missing_journal, StorageErrorKind::Corruption)?;

    let missing_current = folder.path().join("missing-current");
    let backend = FjallBackend::create(&missing_current, default_index_options())?;
    initialize(&backend, 0x4c);
    drop(backend);
    let current = missing_current.join("keyspaces/1/current");
    std::fs::rename(
        &current,
        missing_current.join("keyspaces/1/current.missing"),
    )?;
    assert_open_rejected_without_shape_change(&missing_current, StorageErrorKind::Corruption)?;

    let missing_manifest = folder.path().join("missing-manifest");
    let backend = FjallBackend::create(&missing_manifest, default_index_options())?;
    initialize(&backend, 0x4d);
    drop(backend);
    let tree = missing_manifest.join("keyspaces/1");
    let current = std::fs::read(tree.join("current"))?;
    let version = u64::from_le_bytes(current[..8].try_into()?);
    std::fs::rename(
        tree.join(format!("v{version}")),
        tree.join("active-manifest.missing"),
    )?;
    assert_open_rejected_without_shape_change(&missing_manifest, StorageErrorKind::Corruption)?;

    let invalid_keyspace_name = folder.path().join("invalid-keyspace-name");
    let backend = FjallBackend::create(&invalid_keyspace_name, default_index_options())?;
    initialize(&backend, 0x4e);
    drop(backend);
    std::fs::create_dir(invalid_keyspace_name.join("keyspaces/not-an-id"))?;
    assert_open_rejected_without_shape_change(
        &invalid_keyspace_name,
        StorageErrorKind::InvalidLayout,
    )?;

    let journal_directory = folder.path().join("journal-directory");
    let backend = FjallBackend::create(&journal_directory, default_index_options())?;
    initialize(&backend, 0x4f);
    drop(backend);
    std::fs::create_dir(journal_directory.join("999.jnl"))?;
    assert_open_rejected_without_shape_change(&journal_directory, StorageErrorKind::InvalidLayout)?;
    Ok(())
}

#[test]
fn write_buffer_size_mapping_rotates_the_real_user_memtable() -> TestResult {
    let folder = TempDir::new()?;
    let mut options = Options::default();
    options.write_buffer_size = 1;
    let backend =
        FjallBackend::create(&folder.path().join("index"), options.fjall_index_options())?;
    initialize(&backend, 0x40);

    let mut batch = IndexAtomicBatch::new();
    batch
        .try_push(IndexMutation::PutUser {
            user_key: vec![b'k'; 4 * 1_024],
            encoded_pointer: encoded_pointer(1, 1),
        })
        .unwrap();
    backend
        .commit_atomic(batch, IndexCommitMode::Buffer)
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while backend.user_table_count() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        backend.user_table_count() > 0,
        "the one-byte configured memtable limit should trigger a table flush"
    );
    Ok(())
}

#[test]
fn fjall_errors_are_classified_without_leaking_backend_details() -> TestResult {
    let folder = TempDir::new()?;
    let index_path = folder.path().join("index");
    let backend = FjallBackend::create(&index_path, default_index_options())?;

    let error = expect_backend_error(FjallBackend::open_existing(
        &index_path,
        default_index_options(),
    ));
    assert_eq!(error.kind, StorageErrorKind::Busy);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
    assert!(error.message.is_empty());
    assert!(error.source.is_none());
    drop(backend);
    Ok(())
}

#[test]
fn synthetic_fjall_errors_cover_the_conservative_mapping() {
    for (input, expected_kind, expected_retry, expected_os_code) in [
        (
            TestFjallError::DirectIo,
            StorageErrorKind::Io,
            RetryAdvice::FixEnvironmentAndReopen,
            Some(5),
        ),
        (
            TestFjallError::NestedIo,
            StorageErrorKind::Io,
            RetryAdvice::FixEnvironmentAndReopen,
            Some(5),
        ),
        (
            TestFjallError::StorageCorruption,
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::Decompress,
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::InvalidTrailer,
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::InvalidTag,
            StorageErrorKind::Corruption,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::InvalidVersion,
            StorageErrorKind::IncompatibleFormat,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::Poisoned,
            StorageErrorKind::StoragePoisoned,
            RetryAdvice::ReopenAndVerify,
            None,
        ),
        (
            TestFjallError::KeyspaceDeleted,
            StorageErrorKind::InvalidLayout,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
        (
            TestFjallError::Locked,
            StorageErrorKind::Busy,
            RetryAdvice::RetrySameInstance,
            None,
        ),
        (
            TestFjallError::Unrecoverable,
            StorageErrorKind::Unrecoverable,
            RetryAdvice::RestoreOrRepair,
            None,
        ),
    ] {
        let error = FjallBackend::classify_error_for_test(input);
        assert_eq!(error.kind, expected_kind);
        assert_eq!(error.retry_advice, expected_retry);
        assert_eq!(error.os_code, expected_os_code);
        assert_eq!(error.operation, Operation::Open);
        assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
        assert_eq!(error.write_outcome, None);
        assert!(error.message.is_empty());
        assert!(error.source.is_none());
    }
}

#[test]
fn initialization_is_atomic_sync_all_and_identity_survives_reopen() -> TestResult {
    let folder = TempDir::new()?;
    let index_path = folder.path().join("index");
    let backend = FjallBackend::create(&index_path, default_index_options())?;
    assert_eq!(backend.get_database_identity()?, None);
    let error = backend
        .commit_atomic(
            transaction_batch(1, encoded_pointer(1, 1)),
            IndexCommitMode::Buffer,
        )
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!backend.last_commit_entered());

    let (identity, head_seq, frontier) = encoded_initial_metadata(0x41);
    let initialization =
        IndexAtomicBatch::initialize_database(identity.clone(), head_seq.clone(), frontier.clone())
            .unwrap();
    backend
        .commit_atomic(initialization, IndexCommitMode::SyncAll)
        .unwrap();
    assert_eq!(backend.get_database_identity()?, Some(identity.clone()));
    assert_eq!(
        backend.get_internal(InternalIndexSpace::System, b"head_seq")?,
        Some(head_seq)
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::System, b"durable_frontier")?,
        Some(frontier)
    );
    drop(backend);

    let reopened = FjallBackend::open_existing(&index_path, default_index_options())?;
    assert_eq!(reopened.get_database_identity()?, Some(identity));
    let error = reopened
        .commit_atomic(initialization_batch(0x42), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!reopened.last_commit_entered());
    Ok(())
}

#[test]
fn atomic_batches_and_snapshot_cover_all_three_keyspaces() -> TestResult {
    let folder = TempDir::new()?;
    let backend = FjallBackend::create(&folder.path().join("index"), default_index_options())?;
    initialize(&backend, 0x42);

    let old_pointer = encoded_pointer(1, 1);
    let overwritten_pointer = encoded_pointer(2, 2);
    let mut first = transaction_batch(1, old_pointer);
    first
        .try_push(IndexMutation::PutUser {
            user_key: b"user-key".to_vec(),
            encoded_pointer: overwritten_pointer.clone(),
        })
        .unwrap();
    backend
        .commit_atomic(first, IndexCommitMode::Buffer)
        .unwrap();
    assert_eq!(
        backend.get_user(b"user-key", None)?,
        Some(overwritten_pointer.clone())
    );

    let snapshot = backend.snapshot()?;
    let new_pointer = encoded_pointer(3, 3);
    backend
        .commit_atomic(
            transaction_batch(2, new_pointer.clone()),
            IndexCommitMode::Buffer,
        )
        .unwrap();

    assert_eq!(
        backend.get_user(b"user-key", Some(&snapshot))?,
        Some(overwritten_pointer)
    );
    assert_eq!(backend.get_user(b"user-key", None)?, Some(new_pointer));
    assert_eq!(
        backend.get_internal_at_snapshot(
            &snapshot,
            InternalIndexSpace::Transaction,
            b"tx/current",
        )?,
        Some(b"descriptor-1".to_vec())
    );
    assert_eq!(
        backend.get_internal_at_snapshot(&snapshot, InternalIndexSpace::System, b"head_seq")?,
        Some(1_u64.to_le_bytes().to_vec())
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::Transaction, b"tx/current")?,
        Some(b"descriptor-2".to_vec())
    );
    Ok(())
}

#[test]
fn delete_mutations_remove_user_and_internal_state_and_are_idempotent() -> TestResult {
    let folder = TempDir::new()?;
    let backend = FjallBackend::create(&folder.path().join("index"), default_index_options())?;
    let identity = initialize(&backend, 0x49);
    let pointer = encoded_pointer(9, 8);

    let mut seed = IndexAtomicBatch::new();
    seed.try_push(IndexMutation::PutUser {
        user_key: b"delete-user".to_vec(),
        encoded_pointer: pointer.clone(),
    })
    .unwrap();
    seed.try_push(IndexMutation::PutInternal {
        space: InternalIndexSpace::Transaction,
        key: b"tx/delete".to_vec(),
        value: b"tx-state".to_vec(),
    })
    .unwrap();
    seed.try_push(IndexMutation::PutInternal {
        space: InternalIndexSpace::System,
        key: b"recovery_state".to_vec(),
        value: b"system-state".to_vec(),
    })
    .unwrap();
    backend
        .commit_atomic(seed, IndexCommitMode::Buffer)
        .unwrap();
    let snapshot = backend.snapshot()?;

    let mut deletes = IndexAtomicBatch::new();
    deletes
        .try_push(IndexMutation::DeleteUser {
            user_key: b"delete-user".to_vec(),
        })
        .unwrap();
    deletes
        .try_push(IndexMutation::DeleteInternal {
            space: InternalIndexSpace::Transaction,
            key: b"tx/delete".to_vec(),
        })
        .unwrap();
    deletes
        .try_push(IndexMutation::DeleteInternal {
            space: InternalIndexSpace::System,
            key: b"recovery_state".to_vec(),
        })
        .unwrap();

    backend
        .commit_atomic(deletes.clone(), IndexCommitMode::Buffer)
        .unwrap();
    assert_eq!(backend.get_user(b"delete-user", None)?, None);
    assert_eq!(
        backend.get_internal(InternalIndexSpace::Transaction, b"tx/delete")?,
        None
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::System, b"recovery_state")?,
        None
    );
    assert_eq!(backend.get_database_identity()?, Some(identity));
    assert_eq!(
        backend.get_user(b"delete-user", Some(&snapshot))?,
        Some(pointer)
    );

    backend
        .commit_atomic(deletes, IndexCommitMode::Buffer)
        .unwrap();
    assert_eq!(backend.get_user(b"delete-user", None)?, None);
    Ok(())
}

#[test]
fn internal_scan_is_ordered_streaming_and_propagates_iteration_errors() -> TestResult {
    let folder = TempDir::new()?;
    let backend = FjallBackend::create(&folder.path().join("index"), default_index_options())?;
    initialize(&backend, 0x43);

    let mut batch = IndexAtomicBatch::new();
    for (key, value) in [(b"c", b"3"), (b"a", b"1"), (b"b", b"2")] {
        batch
            .try_push(IndexMutation::PutInternal {
                space: InternalIndexSpace::Transaction,
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .unwrap();
    }
    backend
        .commit_atomic(batch, IndexCommitMode::Buffer)
        .unwrap();
    backend.inject_iterator_error_after(Some(1));

    let range = InternalKeyRange::new(Some(b"a".to_vec()), Some(b"d".to_vec())).unwrap();
    let mut iterator = backend.scan_internal(InternalIndexSpace::Transaction, range)?;
    assert_eq!(iterator.next().unwrap()?.key, b"a");
    let error = iterator.next().unwrap().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(iterator.next().unwrap()?.key, b"b");
    assert_eq!(iterator.next().unwrap()?.key, b"c");
    assert!(iterator.next().is_none());
    Ok(())
}

#[test]
fn identity_mutations_are_rejected_before_fjall_commit() -> TestResult {
    let folder = TempDir::new()?;
    let backend = FjallBackend::create(&folder.path().join("index"), default_index_options())?;
    let identity = initialize(&backend, 0x44);

    for operation in [
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: b"database_identity".to_vec(),
            value: vec![0x99; 32],
        },
        IndexMutation::DeleteInternal {
            space: InternalIndexSpace::System,
            key: b"database_identity".to_vec(),
        },
    ] {
        let batch = IndexAtomicBatch::from_operations_unchecked_for_test(vec![operation]);
        let error = backend
            .commit_atomic(batch, IndexCommitMode::Buffer)
            .unwrap_err();
        assert_eq!(error.apply_state, IndexApplyState::NotApplied);
        assert!(!backend.last_commit_entered());
        assert_eq!(backend.get_database_identity()?, Some(identity.clone()));
    }
    Ok(())
}

#[test]
fn commit_errors_use_conservative_apply_state_boundary() -> TestResult {
    let folder = TempDir::new()?;
    let backend = FjallBackend::create(&folder.path().join("index"), default_index_options())?;
    initialize(&backend, 0x45);

    backend.set_commit_failure(TestCommitFailure::BeforeCommit);
    let error = backend
        .commit_atomic(
            transaction_batch(1, encoded_pointer(1, 1)),
            IndexCommitMode::Buffer,
        )
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!backend.last_commit_entered());
    assert_eq!(backend.get_user(b"user-key", None)?, None);

    let committed_pointer = encoded_pointer(1, 1);
    backend.set_commit_failure(TestCommitFailure::AfterCommitReturned);
    let error = backend
        .commit_atomic(
            transaction_batch(1, committed_pointer.clone()),
            IndexCommitMode::Buffer,
        )
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::Unknown);
    assert!(backend.last_commit_entered());
    assert_eq!(
        backend.get_user(b"user-key", None)?,
        Some(committed_pointer)
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::Transaction, b"tx/current")?,
        Some(b"descriptor-1".to_vec())
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::System, b"head_seq")?,
        Some(1_u64.to_le_bytes().to_vec())
    );
    Ok(())
}

#[test]
fn snapshots_and_iterators_are_owned_and_reject_foreign_snapshots() -> TestResult {
    let first_folder = TempDir::new()?;
    let second_folder = TempDir::new()?;
    let first = FjallBackend::create(&first_folder.path().join("index"), default_index_options())?;
    let second =
        FjallBackend::create(&second_folder.path().join("index"), default_index_options())?;
    initialize(&first, 0x46);
    initialize(&second, 0x47);

    first
        .commit_atomic(
            transaction_batch(1, encoded_pointer(1, 1)),
            IndexCommitMode::Buffer,
        )
        .unwrap();
    let snapshot = first.snapshot()?;
    let error = second.get_user(b"user-key", Some(&snapshot)).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);

    let mut iterator = first.iter_user(Some(&snapshot))?;
    drop(first);
    let entry = iterator.next().unwrap()?;
    assert_eq!(entry.key, b"user-key");
    assert!(iterator.next().is_none());
    Ok(())
}

#[test]
fn buffer_batch_recovers_without_running_drop() -> TestResult {
    run_crash_child_and_verify("buffer")
}

#[test]
fn sync_all_batch_recovers_without_running_drop() -> TestResult {
    run_crash_child_and_verify("sync_all")
}

#[test]
fn database_and_all_keyspaces_honor_manual_journal_persist() -> TestResult {
    for (mode, key, space) in [
        ("database_manual", b"manual-database".as_slice(), None),
        ("user_manual", b"manual-user".as_slice(), None),
        (
            "transaction_manual",
            b"manual-transaction".as_slice(),
            Some(InternalIndexSpace::Transaction),
        ),
        (
            "system_manual",
            b"manual-system".as_slice(),
            Some(InternalIndexSpace::System),
        ),
    ] {
        let folder = TempDir::new()?;
        let index_path = folder.path().join("index");
        let status = Command::new(std::env::current_exe()?)
            .args(["--exact", "fjall_backend_crash_child", "--nocapture"])
            .env(CRASH_CHILD_ENV, "1")
            .env(CRASH_PATH_ENV, &index_path)
            .env(CRASH_MODE_ENV, mode)
            .status()?;
        assert_eq!(status.code(), Some(CRASH_EXIT_CODE), "child mode {mode}");

        let backend = FjallBackend::open_existing(&index_path, default_index_options())?;
        let recovered = match space {
            Some(space) => backend.get_internal(space, key)?,
            None => backend.get_user(key, None)?,
        };
        assert_eq!(
            recovered, None,
            "{mode} became durable without an explicit persist call"
        );
    }
    Ok(())
}

fn run_crash_child_and_verify(mode: &str) -> TestResult {
    let folder = TempDir::new()?;
    let index_path = folder.path().join("index");
    let status = Command::new(std::env::current_exe()?)
        .args(["--exact", "fjall_backend_crash_child", "--nocapture"])
        .env(CRASH_CHILD_ENV, "1")
        .env(CRASH_PATH_ENV, &index_path)
        .env(CRASH_MODE_ENV, mode)
        .status()?;
    assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

    let backend = FjallBackend::open_existing(&index_path, default_index_options())?;
    assert!(backend.get_database_identity()?.is_some());
    assert_eq!(
        backend.get_user(b"user-key", None)?,
        Some(encoded_pointer(1, 1))
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::Transaction, b"tx/current")?,
        Some(b"descriptor-1".to_vec())
    );
    assert_eq!(
        backend.get_internal(InternalIndexSpace::System, b"head_seq")?,
        Some(1_u64.to_le_bytes().to_vec())
    );
    Ok(())
}

#[test]
fn fjall_backend_crash_child() -> TestResult {
    if std::env::var_os(CRASH_CHILD_ENV).is_none() {
        return Ok(());
    }
    let path = PathBuf::from(
        std::env::var_os(CRASH_PATH_ENV).expect("child index path should be provided"),
    );
    let mode = std::env::var(CRASH_MODE_ENV).expect("child commit mode should be provided");
    run_child(&path, &mode)
}

fn run_child(path: &Path, mode: &str) -> TestResult {
    let backend = FjallBackend::create(path, default_index_options())?;
    initialize(&backend, 0x48);
    match mode {
        "buffer" => backend
            .commit_atomic(
                transaction_batch(1, encoded_pointer(1, 1)),
                IndexCommitMode::Buffer,
            )
            .unwrap(),
        "sync_all" => backend
            .commit_atomic(
                transaction_batch(1, encoded_pointer(1, 1)),
                IndexCommitMode::SyncAll,
            )
            .unwrap(),
        "database_manual" => {
            backend.commit_database_batch_without_durability(b"manual-database", b"value")?
        }
        "user_manual" => {
            backend.insert_without_keyspace_durability(None, b"manual-user", b"value")?
        }
        "transaction_manual" => backend.insert_without_keyspace_durability(
            Some(InternalIndexSpace::Transaction),
            b"manual-transaction",
            b"value",
        )?,
        "system_manual" => backend.insert_without_keyspace_durability(
            Some(InternalIndexSpace::System),
            b"manual-system",
            b"value",
        )?,
        other => panic!("unknown child mode: {other}"),
    }

    let live_value = match mode {
        "database_manual" => Some(backend.get_user(b"manual-database", None)?),
        "user_manual" => Some(backend.get_user(b"manual-user", None)?),
        "transaction_manual" => {
            Some(backend.get_internal(InternalIndexSpace::Transaction, b"manual-transaction")?)
        }
        "system_manual" => {
            Some(backend.get_internal(InternalIndexSpace::System, b"manual-system")?)
        }
        "buffer" | "sync_all" => None,
        _ => unreachable!("mode was validated above"),
    };
    if let Some(live_value) = live_value {
        assert_eq!(live_value, Some(b"value".to_vec()));
    }
    std::process::exit(CRASH_EXIT_CODE);
}
