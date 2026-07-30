use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable,
    compaction::Leveled,
    config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy},
};
use std::{
    error::Error,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    process::Command,
    sync::{Arc, Barrier},
    thread,
};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

const KEYSPACE_NAME: &str = "rustkv_index";
const CRASH_CHILD_ENV: &str = "RUSTKV_FJALL_CRASH_CHILD";
const CRASH_PATH_ENV: &str = "RUSTKV_FJALL_CRASH_PATH";
const CRASH_MODE_ENV: &str = "RUSTKV_FJALL_CRASH_MODE";
const CRASH_EXIT_CODE: i32 = 23;

fn open_default(path: &Path) -> fjall::Result<(Database, Keyspace)> {
    let db = Database::builder(path).open()?;
    let keyspace = db.keyspace(KEYSPACE_NAME, KeyspaceCreateOptions::default)?;
    Ok((db, keyspace))
}

fn bytes(value: Option<fjall::UserValue>) -> Option<Vec<u8>> {
    value.map(|value| value.to_vec())
}

fn encode_spike_user_key(user_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(user_key.len() + 1);
    encoded.push(0);
    encoded.extend_from_slice(user_key);
    encoded
}

fn collect_pairs(
    iter: impl Iterator<Item = fjall::Guard>,
) -> fjall::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    iter.map(|item| {
        let (key, value) = item.into_inner()?;
        Ok((key.to_vec(), value.to_vec()))
    })
    .collect()
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn basic_put_get_delete_empty_bytes_and_reopen() -> TestResult {
    let folder = TempDir::new()?;

    {
        let (db, keyspace) = open_default(folder.path())?;

        keyspace.insert(b"alpha", b"value")?;
        assert_eq!(bytes(keyspace.get(b"alpha")?), Some(b"value".to_vec()));

        keyspace.insert(b"empty-value", Vec::<u8>::new())?;
        assert_eq!(bytes(keyspace.get(b"empty-value")?), Some(Vec::new()));

        keyspace.remove(b"missing")?;
        keyspace.remove(b"alpha")?;
        assert_eq!(keyspace.get(b"alpha")?, None);

        db.persist(PersistMode::SyncAll)?;
    }

    let (_db, keyspace) = open_default(folder.path())?;
    assert_eq!(bytes(keyspace.get(b"empty-value")?), Some(Vec::new()));
    assert_eq!(keyspace.get(b"alpha")?, None);

    Ok(())
}

#[test]
fn empty_key_requires_order_preserving_index_adapter_encoding() -> TestResult {
    {
        let direct_folder = TempDir::new()?;
        let (_db, direct_keyspace) = open_default(direct_folder.path())?;
        let direct_empty_key = catch_unwind(AssertUnwindSafe(|| {
            direct_keyspace.insert(Vec::<u8>::new(), b"value")
        }));
        assert!(
            direct_empty_key.is_err(),
            "Fjall 3.1.8 is expected to reject a directly inserted empty key"
        );
    }

    let folder = TempDir::new()?;
    let (_db, keyspace) = open_default(folder.path())?;
    for key in [b"".as_slice(), b"\x00", b"a", b"\xff"] {
        keyspace.insert(encode_spike_user_key(key), key)?;
    }

    assert_eq!(
        bytes(keyspace.get(encode_spike_user_key(b""))?),
        Some(Vec::new())
    );

    let encoded_keys = collect_pairs(keyspace.iter())?
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    assert_eq!(
        encoded_keys,
        vec![
            encode_spike_user_key(b""),
            encode_spike_user_key(b"\x00"),
            encode_spike_user_key(b"a"),
            encode_spike_user_key(b"\xff"),
        ]
    );

    Ok(())
}

#[test]
fn write_batch_is_atomic_at_commit_and_preserves_operation_order() -> TestResult {
    let folder = TempDir::new()?;
    let (db, keyspace) = open_default(folder.path())?;

    let mut batch = db.batch().durability(Some(PersistMode::Buffer));
    batch.insert(&keyspace, b"k", b"first");
    batch.insert(&keyspace, b"other", b"value");
    batch.remove(&keyspace, b"k");
    batch.insert(&keyspace, b"k", b"last");

    assert_eq!(keyspace.get(b"k")?, None);
    assert_eq!(keyspace.get(b"other")?, None);

    batch.commit()?;

    assert_eq!(bytes(keyspace.get(b"k")?), Some(b"last".to_vec()));
    assert_eq!(bytes(keyspace.get(b"other")?), Some(b"value".to_vec()));

    Ok(())
}

#[test]
fn snapshot_is_stable_cloneable_and_thread_shareable() -> TestResult {
    assert_send_sync::<fjall::Snapshot>();

    let folder = TempDir::new()?;
    let (db, keyspace) = open_default(folder.path())?;

    keyspace.insert(b"k", b"old")?;
    keyspace.insert(b"deleted_later", b"kept_by_snapshot")?;

    let snapshot = db.snapshot();
    keyspace.insert(b"k", b"new")?;
    keyspace.remove(b"deleted_later")?;
    keyspace.insert(b"created_later", b"new_value")?;

    assert_eq!(bytes(snapshot.get(&keyspace, b"k")?), Some(b"old".to_vec()));
    assert_eq!(
        bytes(snapshot.get(&keyspace, b"deleted_later")?),
        Some(b"kept_by_snapshot".to_vec())
    );
    assert_eq!(snapshot.get(&keyspace, b"created_later")?, None);

    let snapshot_clone = snapshot.clone();
    let keyspace_clone = keyspace.clone();
    let value_from_thread = thread::spawn(move || snapshot_clone.get(&keyspace_clone, b"k"))
        .join()
        .expect("snapshot reader thread should not panic")?;

    assert_eq!(bytes(value_from_thread), Some(b"old".to_vec()));

    Ok(())
}

#[test]
fn iterator_supports_order_reverse_and_seek_via_range() -> TestResult {
    let folder = TempDir::new()?;
    let (_db, keyspace) = open_default(folder.path())?;

    for (key, value) in [
        (b"c".as_slice(), b"3".as_slice()),
        (b"a".as_slice(), b"1".as_slice()),
        (b"b".as_slice(), b"2".as_slice()),
        (b"\xff".as_slice(), b"4".as_slice()),
    ] {
        keyspace.insert(key, value)?;
    }

    let forward = collect_pairs(keyspace.iter())?;
    assert_eq!(
        forward
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b", b"c", b"\xff"]
    );

    let reverse = collect_pairs(keyspace.iter().rev())?;
    assert_eq!(
        reverse
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"\xff".as_slice(), b"c", b"b", b"a"]
    );

    let seek_from_b = collect_pairs(keyspace.range(b"b".as_slice()..))?;
    assert_eq!(
        seek_from_b
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"b".as_slice(), b"c", b"\xff"]
    );

    Ok(())
}

#[test]
fn database_and_keyspace_are_send_sync_and_support_concurrent_access() -> TestResult {
    assert_send_sync::<Database>();
    assert_send_sync::<Keyspace>();

    let folder = TempDir::new()?;
    let (db, keyspace) = open_default(folder.path())?;
    let thread_count = 8;
    let barrier = Arc::new(Barrier::new(thread_count));

    let mut handles = Vec::with_capacity(thread_count);
    for thread_id in 0..thread_count {
        let db = db.clone();
        let keyspace = keyspace.clone();
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || -> fjall::Result<()> {
            barrier.wait();

            for item_id in 0..100 {
                let key = format!("thread-{thread_id:02}-item-{item_id:03}");
                let value = format!("value-{thread_id:02}-{item_id:03}");
                keyspace.insert(key.as_bytes(), value.as_bytes())?;
                assert_eq!(
                    bytes(keyspace.get(key.as_bytes())?),
                    Some(value.into_bytes())
                );
            }

            db.persist(PersistMode::Buffer)
        }));
    }

    for handle in handles {
        handle
            .join()
            .expect("concurrent Fjall worker should not panic")?;
    }

    assert_eq!(keyspace.len()?, thread_count * 100);

    Ok(())
}

#[test]
fn planned_builder_and_keyspace_options_are_constructible() -> TestResult {
    let folder = TempDir::new()?;
    let db = Database::builder(folder.path())
        .cache_size(8 * 1_024 * 1_024)
        .max_cached_files(Some(1_000))
        .worker_threads(1)
        .manual_journal_persist(true)
        .open()?;

    assert_eq!(db.cache_capacity(), 8 * 1_024 * 1_024);

    let keyspace = db.keyspace(KEYSPACE_NAME, || {
        KeyspaceCreateOptions::default()
            .max_memtable_size(4 * 1_024 * 1_024)
            .data_block_size_policy(BlockSizePolicy::all(4 * 1_024))
            .data_block_restart_interval_policy(RestartIntervalPolicy::all(16))
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::Lz4))
            .compaction_strategy(Arc::new(
                Leveled::default().with_table_target_size(2 * 1_024 * 1_024),
            ))
            .manual_journal_persist(true)
    })?;

    keyspace.insert(b"configured", b"value")?;
    db.persist(PersistMode::SyncData)?;

    let zero_cache_folder = TempDir::new()?;
    let zero_cache_db = Database::builder(zero_cache_folder.path())
        .cache_size(0)
        .max_cached_files(Some(10))
        .open()?;
    assert_eq!(zero_cache_db.cache_capacity(), 0);

    Ok(())
}

#[test]
fn foreground_reads_and_writes_survive_forced_flush_and_compaction() -> TestResult {
    let folder = TempDir::new()?;
    let db = Database::builder(folder.path()).worker_threads(2).open()?;
    let keyspace = db.keyspace(KEYSPACE_NAME, || {
        KeyspaceCreateOptions::default().max_memtable_size(64 * 1_024)
    })?;

    let value = vec![0x5a; 2 * 1_024];
    for id in 0..256_u32 {
        keyspace.insert(id.to_be_bytes(), &value)?;
    }

    keyspace.rotate_memtable_and_wait()?;

    let compacting_keyspace = keyspace.clone();
    let compaction = thread::spawn(move || compacting_keyspace.major_compact());

    for id in 0..256_u32 {
        assert_eq!(bytes(keyspace.get(id.to_be_bytes())?), Some(value.clone()));
    }

    compaction
        .join()
        .expect("major compaction thread should not panic")?;
    db.persist(PersistMode::SyncAll)?;

    Ok(())
}

#[test]
fn locked_database_returns_a_classifiable_error() -> TestResult {
    let folder = TempDir::new()?;
    let (_db, _keyspace) = open_default(folder.path())?;

    let error = match Database::builder(folder.path()).open() {
        Ok(_) => panic!("opening an already locked database should fail"),
        Err(error) => error,
    };
    assert!(matches!(error, fjall::Error::Locked));

    Ok(())
}

#[test]
fn persist_modes_recover_after_process_exit_without_drop() -> TestResult {
    for mode in ["buffer", "sync_data", "sync_all"] {
        let folder = TempDir::new()?;
        let status = Command::new(std::env::current_exe()?)
            .args(["--exact", "fjall_persist_child", "--nocapture"])
            .env(CRASH_CHILD_ENV, "1")
            .env(CRASH_PATH_ENV, folder.path())
            .env(CRASH_MODE_ENV, mode)
            .status()?;

        assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

        let (_db, keyspace) = open_default(folder.path())?;
        assert_eq!(
            bytes(keyspace.get(b"persisted")?),
            Some(mode.as_bytes().to_vec())
        );
    }

    Ok(())
}

#[test]
fn fjall_persist_child() -> TestResult {
    if std::env::var_os(CRASH_CHILD_ENV).is_none() {
        return Ok(());
    }

    let path = std::env::var_os(CRASH_PATH_ENV).expect("crash child path must be provided");
    let mode_name = std::env::var(CRASH_MODE_ENV).expect("persist mode must be provided");
    let mode = match mode_name.as_str() {
        "buffer" => PersistMode::Buffer,
        "sync_data" => PersistMode::SyncData,
        "sync_all" => PersistMode::SyncAll,
        _ => panic!("unknown persist mode: {mode_name}"),
    };

    let db = Database::builder(Path::new(&path))
        .manual_journal_persist(true)
        .open()?;
    let keyspace = db.keyspace(KEYSPACE_NAME, || {
        KeyspaceCreateOptions::default().manual_journal_persist(true)
    })?;
    keyspace.insert(b"persisted", mode_name.as_bytes())?;
    db.persist(mode)?;

    std::process::exit(CRASH_EXIT_CODE);
}
