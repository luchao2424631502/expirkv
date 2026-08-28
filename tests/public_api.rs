use std::error::Error;
use std::path::Path;

use rustkv::{
    Compression, CursorState, Db, DbIterator, DbStats, DestroyFailureContext, DestroyStage,
    InstanceState, KeyRange, LatchedErrorSummary, ManagedObject, Operation, Options, ProtocolStage,
    RangeCursor, ReadOptions, Result, RetryAdvice, Snapshot, StorageError, StorageErrorKind,
    VLogPosition, WriteBatch, WriteOptions, WriteOutcome,
};
use tempfile::TempDir;

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
fn assert_error<T: Error>() {}

#[test]
fn public_handle_traits_match_the_contract() {
    assert_clone_send_sync::<Db>();
    assert_clone_send_sync::<Snapshot>();
    assert_error::<StorageError>();
}

#[test]
fn options_defaults_match_the_contract() {
    let options = Options::default();
    assert!(!options.create_if_missing);
    assert!(!options.error_if_exists);
    assert_eq!(options.write_buffer_size, 4 * 1024 * 1024);
    assert_eq!(options.max_open_files, 1000);
    assert_eq!(options.block_cache_size, 8 * 1024 * 1024);
    assert_eq!(options.block_size, 4 * 1024);
    assert_eq!(options.block_restart_interval, 16);
    assert_eq!(options.max_file_size, 2 * 1024 * 1024);
    assert_eq!(options.compression, Compression::NoCompression);
    assert_eq!(options.vlog_read_handle_cache_capacity, 64);

    let write_options = WriteOptions::default();
    assert!(!write_options.sync);

    let read_options = ReadOptions::default();
    assert!(read_options.snapshot.is_none());
}

#[test]
fn public_struct_fields_have_the_frozen_shape() {
    let _ = Options {
        create_if_missing: false,
        error_if_exists: false,
        write_buffer_size: 1,
        max_open_files: 2,
        block_cache_size: 3,
        block_size: 4,
        block_restart_interval: 5,
        max_file_size: 6,
        compression: Compression::Lz4,
        vlog_read_handle_cache_capacity: 7,
    };
    let _ = WriteOptions { sync: true };
    let _ = ReadOptions { snapshot: None };
    let _ = KeyRange {
        start: Some(b"a"),
        end: Some(b"z"),
    };

    let position = VLogPosition {
        file_id: 4,
        offset: 8,
    };
    let summary = LatchedErrorSummary {
        kind: StorageErrorKind::Io,
        operation: Operation::Background,
        protocol_stage: ProtocolStage::Maintenance,
        retry_advice: RetryAdvice::FixEnvironmentAndReopen,
        os_code: Some(5),
        commit_seq: Some(6),
        vlog_file_id: Some(7),
        vlog_offset: Some(8),
    };
    let _ = DbStats {
        schema_version: 1,
        instance_state: InstanceState::Healthy,
        state_epoch: 0,
        first_latched_error: Some(summary),
        head_seq: 0,
        durable_seq: 0,
        durability_lag: 0,
        durable_vlog_end: Some(position),
        active_vlog_file_id: None,
        vlog_file_count: 0,
        vlog_logical_bytes: 0,
    };

    let destroy_failure = DestroyFailureContext {
        failed_object: ManagedObject::VLogFile { file_id: 9 },
        stage: DestroyStage::RemoveFile,
        partially_deleted: true,
        os_code: Some(13),
    };
    let error = StorageError {
        schema_version: 1,
        kind: StorageErrorKind::Io,
        operation: Operation::Destroy,
        protocol_stage: ProtocolStage::Lifecycle,
        write_outcome: None,
        instance_state: None,
        retry_advice: RetryAdvice::FixEnvironmentAndReopen,
        os_code: Some(13),
        commit_seq: None,
        tx_uuid: None,
        vlog_file_id: Some(9),
        vlog_offset: None,
        destroy_failure: Some(destroy_failure),
        message: String::new(),
        source: None,
    };
    assert!(error.destroy_failure.is_some());
}

#[test]
fn all_frozen_enum_variants_are_public() {
    let _ = [
        CursorState::Unpositioned,
        CursorState::Valid,
        CursorState::Exhausted,
        CursorState::Failed,
    ];
    let _ = [WriteOutcome::NotCommitted, WriteOutcome::CommitUnknown];
    let _ = [
        InstanceState::Healthy,
        InstanceState::WriteStopped,
        InstanceState::Poisoned,
    ];
    let _ = [
        RetryAdvice::FixRequestAndRetrySameInstance,
        RetryAdvice::RetrySameInstance,
        RetryAdvice::FixEnvironmentAndReopen,
        RetryAdvice::ReopenAndVerify,
        RetryAdvice::RestoreOrRepair,
        RetryAdvice::DoNotRetry,
    ];
    let _ = [
        StorageErrorKind::InvalidArgument,
        StorageErrorKind::NotFound,
        StorageErrorKind::Busy,
        StorageErrorKind::Unsupported,
        StorageErrorKind::ResourceExhausted,
        StorageErrorKind::CapacityExceeded,
        StorageErrorKind::Io,
        StorageErrorKind::Corruption,
        StorageErrorKind::InvalidLayout,
        StorageErrorKind::IncompatibleFormat,
        StorageErrorKind::StorageWriteStopped,
        StorageErrorKind::StoragePoisoned,
        StorageErrorKind::Unrecoverable,
    ];
    let _ = [
        Operation::Open,
        Operation::Put,
        Operation::Delete,
        Operation::WriteBatch,
        Operation::Get,
        Operation::Snapshot,
        Operation::Iterator,
        Operation::Range,
        Operation::Sync,
        Operation::Destroy,
        Operation::Drop,
        Operation::Background,
        Operation::Recovery,
    ];
    let _ = [
        ProtocolStage::Admission,
        ProtocolStage::Preflight,
        ProtocolStage::VLogAppend,
        ProtocolStage::VLogSync,
        ProtocolStage::IndexCommit,
        ProtocolStage::DurableFrontier,
        ProtocolStage::Read,
        ProtocolStage::Recovery,
        ProtocolStage::Maintenance,
        ProtocolStage::Lifecycle,
    ];
    let _ = [
        ManagedObject::Lock,
        ManagedObject::Format,
        ManagedObject::FormatTemporary,
        ManagedObject::DatabaseIdentity,
        ManagedObject::IndexDirectory,
        ManagedObject::VLogDirectory,
        ManagedObject::VLogFile { file_id: 0 },
    ];
    let _ = [
        DestroyStage::AcquireLock,
        DestroyStage::Inventory,
        DestroyStage::RemoveFile,
        DestroyStage::RemoveTree,
        DestroyStage::SyncDirectory,
    ];
}

#[test]
fn write_batch_validates_boundaries_and_preserves_state_on_failure() {
    let mut batch = WriteBatch::new();
    assert!(batch.is_empty());

    batch.put(b"k", []).unwrap();
    batch.put(b"k", vec![0; 59_998]).unwrap();
    batch.put(b"k", vec![0; 59_999]).unwrap();
    assert_eq!(batch.len(), 3);

    let error = batch.put(b"k", vec![0; 60_000]).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(error.operation, Operation::WriteBatch);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);
    assert_eq!(error.write_outcome, Some(WriteOutcome::NotCommitted));
    assert_eq!(error.instance_state, None);
    assert_eq!(
        error.retry_advice,
        RetryAdvice::FixRequestAndRetrySameInstance
    );
    assert_eq!(batch.len(), 3);

    let error = batch.delete([]).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(batch.len(), 3);

    batch.clear();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
}

#[test]
fn errors_do_not_copy_user_bytes_into_public_text_or_source() {
    let secret_key = b"private-user-key";
    let mut batch = WriteBatch::new();
    let error = batch.put([], secret_key).unwrap_err();

    assert!(
        !error
            .message
            .as_bytes()
            .windows(secret_key.len())
            .any(|window| window == secret_key)
    );
    assert!(error.source.is_none());
    assert!(error.destroy_failure.is_none());
}

#[test]
fn open_reports_missing_and_delayed_destroy_remains_unsupported() {
    let options = Options::default();
    let folder = TempDir::new().unwrap();
    let missing_path = folder.path().join("missing-db");
    let open_error = match Db::open(&options, &missing_path) {
        Ok(_) => panic!("open must not create a database without create_if_missing"),
        Err(error) => error,
    };
    assert_eq!(open_error.kind, StorageErrorKind::NotFound);
    assert_eq!(open_error.operation, Operation::Open);
    assert_eq!(open_error.write_outcome, None);
    assert_eq!(open_error.instance_state, None);

    let destroy_error = match Db::destroy(Path::new(&missing_path), &options) {
        Ok(()) => panic!("stage 1 must not report a fake destroy success"),
        Err(error) => error,
    };
    assert_eq!(destroy_error.kind, StorageErrorKind::Unsupported);
    assert_eq!(destroy_error.operation, Operation::Destroy);
    assert_eq!(destroy_error.write_outcome, None);
    assert!(destroy_error.destroy_failure.is_none());
}

#[allow(dead_code)]
fn compile_all_db_method_signatures(
    db: &Db,
    write_options: &WriteOptions,
    read_options: &ReadOptions<'_>,
    batch: &WriteBatch,
) {
    let _: Result<()> = db.put(write_options, b"key", b"value");
    let _: Result<Option<Vec<u8>>> = db.get(read_options, b"key");
    let _: Result<()> = db.delete(write_options, b"key");
    let _: Result<()> = db.write(write_options, batch);
    let _: Result<Snapshot> = db.snapshot();
    let _: Result<DbIterator> = db.iter(read_options);
    let _: Result<RangeCursor> = db.range(
        read_options,
        KeyRange {
            start: None,
            end: None,
        },
        usize::MAX,
    );
    let _: DbStats = db.stats();
}

#[allow(dead_code)]
fn compile_all_cursor_method_signatures(iterator: &mut DbIterator, range: &mut RangeCursor) {
    let _: bool = iterator.valid();
    iterator.seek_to_first();
    iterator.seek_to_last();
    iterator.seek(b"");
    iterator.next();
    iterator.prev();
    let _: Option<&[u8]> = iterator.key();
    let _: Option<&[u8]> = iterator.value();
    let _: std::result::Result<(), &StorageError> = iterator.status();

    let _: bool = range.valid();
    let _: Option<&[u8]> = range.key();
    let _: Option<&[u8]> = range.value();
    range.next();
    let _: std::result::Result<(), &StorageError> = range.status();
}
