use std::path::Path;

use rustkv::{
    Db, InstanceState, KeyRange, Operation, Options, ProtocolStage, ReadOptions, StorageErrorKind,
    WriteBatch, WriteOptions,
};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn read(db: &Db, key: &[u8]) -> rustkv::Result<Option<Vec<u8>>> {
    db.get(&ReadOptions::default(), key)
}

fn expect_error<T>(result: rustkv::Result<T>) -> rustkv::StorageError {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error,
    }
}

#[test]
fn create_open_and_error_if_exists_matrix_is_real() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");

    let missing = expect_error(Db::open(&Options::default(), &root));
    assert_eq!(missing.kind, StorageErrorKind::NotFound);
    assert_eq!(missing.operation, Operation::Open);
    assert!(!root.exists());

    let db = Db::open(&create_options(), &root)?;
    assert_eq!(db.stats().instance_state, InstanceState::Healthy);

    let busy = expect_error(Db::open(&Options::default(), &root));
    assert_eq!(busy.kind, StorageErrorKind::Busy);
    drop(db);

    let db = Db::open(&Options::default(), &root)?;
    drop(db);

    let exists = expect_error(Db::open(
        &Options {
            error_if_exists: true,
            ..Options::default()
        },
        &root,
    ));
    assert_eq!(exists.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(exists.operation, Operation::Open);
    Ok(())
}

#[test]
fn put_get_delete_overwrite_and_rewrite_use_the_public_path() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let asynchronous = WriteOptions::default();

    assert_eq!(read(&db, b"missing")?, None);
    db.delete(&asynchronous, b"missing")?;
    assert_eq!(read(&db, b"missing")?, None);

    db.put(&asynchronous, b"key", b"first")?;
    assert_eq!(read(&db, b"key")?, Some(b"first".to_vec()));
    db.put(&asynchronous, b"key", b"second")?;
    assert_eq!(read(&db, b"key")?, Some(b"second".to_vec()));
    db.delete(&asynchronous, b"key")?;
    assert_eq!(read(&db, b"key")?, None);
    db.put(&asynchronous, b"key", b"reborn")?;
    assert_eq!(read(&db, b"key")?, Some(b"reborn".to_vec()));
    Ok(())
}

#[test]
fn binary_empty_and_sixty_thousand_byte_boundaries_are_enforced() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let write = WriteOptions::default();

    let binary_key = [0, 0xff, b'k', 0x80];
    let binary_value = [0xff, 0, 0x7f, 0x80];
    db.put(&write, &binary_key, &binary_value)?;
    assert_eq!(read(&db, &binary_key)?, Some(binary_value.to_vec()));

    db.put(&write, b"empty-value", b"")?;
    assert_eq!(read(&db, b"empty-value")?, Some(Vec::new()));

    let value_59_999 = vec![7_u8; 59_999];
    db.put(&write, b"v", &value_59_999)?;
    assert_eq!(read(&db, b"v")?, Some(value_59_999));

    let key_60_000 = vec![b'k'; 60_000];
    db.put(&write, &key_60_000, b"")?;
    assert_eq!(read(&db, &key_60_000)?, Some(Vec::new()));

    let too_large_value = db.put(&write, b"v", &vec![0; 60_000]).unwrap_err();
    assert_eq!(too_large_value.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(too_large_value.operation, Operation::Put);
    assert_eq!(too_large_value.protocol_stage, ProtocolStage::Preflight);

    let too_large_key = db.put(&write, &vec![b'k'; 60_001], b"").unwrap_err();
    assert_eq!(too_large_key.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(
        read(&db, b"v")?.as_deref(),
        Some(vec![7_u8; 59_999].as_slice())
    );
    Ok(())
}

#[test]
fn mixed_batch_preserves_order_and_publishes_only_final_states() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    db.put(&WriteOptions::default(), b"deleted", b"old")?;

    let mut batch = WriteBatch::new();
    batch.put(b"same", b"one")?;
    batch.put(b"same", b"two")?;
    batch.delete(b"same")?;
    batch.put(b"same", b"final")?;
    batch.delete(b"deleted")?;
    batch.delete(b"never-existed")?;
    batch.put(b"empty", b"")?;
    db.write(&WriteOptions::default(), &batch)?;

    assert_eq!(read(&db, b"same")?, Some(b"final".to_vec()));
    assert_eq!(read(&db, b"deleted")?, None);
    assert_eq!(read(&db, b"never-existed")?, None);
    assert_eq!(read(&db, b"empty")?, Some(Vec::new()));
    Ok(())
}

#[test]
fn empty_batches_and_sync_barrier_update_real_stats() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let empty = WriteBatch::new();

    let initial = db.stats();
    assert_eq!(initial.instance_state, InstanceState::Healthy);
    assert_eq!(initial.head_seq, 0);
    assert_eq!(initial.durable_seq, 0);
    assert_eq!(initial.durability_lag, 0);
    assert!(initial.durable_vlog_end.is_none());
    assert!(initial.active_vlog_file_id.is_none());
    assert_eq!(initial.vlog_file_count, 0);
    assert_eq!(initial.vlog_logical_bytes, 0);

    db.write(&WriteOptions::default(), &empty)?;
    let after_empty = db.stats();
    assert_eq!(after_empty.head_seq, 0);
    assert_eq!(after_empty.vlog_logical_bytes, 0);

    db.put(&WriteOptions::default(), b"a", b"one")?;
    db.put(&WriteOptions::default(), b"b", b"two")?;
    let buffered = db.stats();
    assert_eq!(buffered.head_seq, 2);
    assert_eq!(buffered.durable_seq, 0);
    assert_eq!(buffered.durability_lag, 2);
    assert_eq!(buffered.active_vlog_file_id, Some(0));
    assert_eq!(buffered.vlog_file_count, 1);
    assert!(buffered.vlog_logical_bytes > 0);

    db.write(&WriteOptions { sync: true }, &empty)?;
    let durable = db.stats();
    assert_eq!(durable.head_seq, 2);
    assert_eq!(durable.durable_seq, 2);
    assert_eq!(durable.durability_lag, 0);
    assert!(durable.durable_vlog_end.is_some());
    assert_eq!(durable.vlog_logical_bytes, buffered.vlog_logical_bytes);

    db.put(&WriteOptions { sync: true }, b"c", b"three")?;
    let direct_sync = db.stats();
    assert_eq!(direct_sync.head_seq, 3);
    assert_eq!(direct_sync.durable_seq, 3);
    assert_eq!(direct_sync.durability_lag, 0);
    Ok(())
}

#[test]
fn stage15_read_views_are_available_while_destroy_remains_unsupported() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    db.put(&WriteOptions::default(), b"key", b"snapshot-value")?;

    let snapshot = db.snapshot()?;
    db.put(&WriteOptions::default(), b"key", b"current-value")?;
    let read_options = ReadOptions {
        snapshot: Some(&snapshot),
    };
    assert_eq!(
        db.get(&read_options, b"key")?,
        Some(b"snapshot-value".to_vec())
    );

    let mut iterator = db.iter(&read_options)?;
    assert!(!iterator.valid());
    assert!(iterator.status().is_ok());
    iterator.seek_to_first();
    assert_eq!(iterator.key(), Some(b"key".as_slice()));
    assert_eq!(iterator.value(), Some(b"snapshot-value".as_slice()));

    let range = db.range(
        &read_options,
        KeyRange {
            start: Some(b"a"),
            end: Some(b"z"),
        },
        10,
    )?;
    assert!(range.valid());
    assert_eq!(range.key(), Some(b"key".as_slice()));
    assert_eq!(range.value(), Some(b"snapshot-value".as_slice()));
    assert!(range.status().is_ok());

    let destroy = Db::destroy(Path::new(&root), &Options::default()).unwrap_err();
    assert_eq!(destroy.kind, StorageErrorKind::Unsupported);
    assert_eq!(destroy.operation, Operation::Destroy);
    Ok(())
}

#[test]
fn clones_share_visibility_and_keep_the_root_lock() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let first = Db::open(&create_options(), &root)?;
    let second = first.clone();

    first.put(&WriteOptions::default(), b"shared", b"value")?;
    assert_eq!(read(&second, b"shared")?, Some(b"value".to_vec()));
    drop(first);
    assert_eq!(
        expect_error(Db::open(&Options::default(), &root)).kind,
        StorageErrorKind::Busy
    );
    drop(second);
    assert!(Db::open(&Options::default(), &root).is_ok());
    Ok(())
}
