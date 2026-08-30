use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

use rustkv::{
    Db, InstanceState, KeyRange, Operation, Options, ReadOptions, StorageErrorKind, WriteBatch,
    WriteOptions,
};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn read(
    db: &Db,
    snapshot: Option<&rustkv::Snapshot>,
    key: &[u8],
) -> rustkv::Result<Option<Vec<u8>>> {
    db.get(&ReadOptions { snapshot }, key)
}

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}

fn expect_error<T>(result: rustkv::Result<T>) -> rustkv::StorageError {
    match result {
        Ok(_) => panic!("operation unexpectedly succeeded"),
        Err(error) => error,
    }
}

#[test]
fn snapshot_is_owned_cloneable_and_stable_across_all_write_shapes() -> TestResult {
    assert_clone_send_sync::<rustkv::Snapshot>();
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    db.put(&WriteOptions::default(), b"overwritten", b"old")?;
    db.put(&WriteOptions::default(), b"deleted", b"present")?;
    db.put(&WriteOptions::default(), b"unchanged", b"stable")?;

    let snapshot = db.snapshot()?;
    let snapshot_clone = snapshot.clone();

    db.put(&WriteOptions::default(), b"overwritten", b"new")?;
    db.delete(&WriteOptions::default(), b"deleted")?;
    let mut batch = WriteBatch::new();
    batch.put(b"new-key", b"new-value")?;
    batch.delete(b"unchanged")?;
    db.write(&WriteOptions::default(), &batch)?;

    assert_eq!(
        read(&db, Some(&snapshot), b"overwritten")?,
        Some(b"old".to_vec())
    );
    assert_eq!(
        read(&db, Some(&snapshot_clone), b"deleted")?,
        Some(b"present".to_vec())
    );
    assert_eq!(
        read(&db, Some(&snapshot), b"unchanged")?,
        Some(b"stable".to_vec())
    );
    assert_eq!(read(&db, Some(&snapshot), b"new-key")?, None);

    assert_eq!(read(&db, None, b"overwritten")?, Some(b"new".to_vec()));
    assert_eq!(read(&db, None, b"deleted")?, None);
    assert_eq!(read(&db, None, b"unchanged")?, None);
    assert_eq!(read(&db, None, b"new-key")?, Some(b"new-value".to_vec()));
    Ok(())
}

#[test]
fn snapshot_view_outlives_the_creating_db_handle() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let creator = Db::open(&create_options(), &root)?;
    creator.put(&WriteOptions::default(), b"a", b"one")?;
    creator.put(&WriteOptions::default(), b"b", b"two")?;
    let snapshot = creator.snapshot()?;
    let surviving_db = creator.clone();
    let mut iterator = creator.iter(&ReadOptions {
        snapshot: Some(&snapshot),
    })?;
    drop(creator);

    surviving_db.put(&WriteOptions::default(), b"a", b"new")?;
    assert_eq!(
        read(&surviving_db, Some(&snapshot), b"a")?,
        Some(b"one".to_vec())
    );
    drop(surviving_db);

    iterator.seek_to_first();
    assert!(iterator.valid());
    assert_eq!(iterator.key(), Some(b"a".as_slice()));
    assert_eq!(iterator.value(), Some(b"one".as_slice()));
    iterator.next();
    assert_eq!(iterator.key(), Some(b"b".as_slice()));
    assert_eq!(iterator.value(), Some(b"two".as_slice()));
    Ok(())
}

#[test]
fn snapshot_from_another_db_is_rejected_by_get_iter_and_range() -> TestResult {
    let folder = TempDir::new()?;
    let first = Db::open(&create_options(), folder.path().join("first"))?;
    let second = Db::open(&create_options(), folder.path().join("second"))?;
    first.put(&WriteOptions::default(), b"key", b"first")?;
    second.put(&WriteOptions::default(), b"key", b"second")?;
    let foreign = first.snapshot()?;
    let options = ReadOptions {
        snapshot: Some(&foreign),
    };

    let get_error = second.get(&options, b"key").unwrap_err();
    assert_eq!(get_error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(get_error.operation, Operation::Get);
    assert_eq!(get_error.instance_state, Some(InstanceState::Healthy));

    let iter_error = match second.iter(&options) {
        Ok(_) => panic!("foreign snapshot iterator unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(iter_error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(iter_error.operation, Operation::Iterator);

    let range_error = match second.range(
        &options,
        KeyRange {
            start: None,
            end: None,
        },
        0,
    ) {
        Ok(_) => panic!("foreign snapshot range unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(range_error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(range_error.operation, Operation::Range);
    Ok(())
}

#[test]
fn snapshot_iterator_and_range_each_keep_the_database_root_locked() -> TestResult {
    let folder = TempDir::new()?;

    let snapshot_root = folder.path().join("snapshot-db");
    let snapshot = {
        let db = Db::open(&create_options(), &snapshot_root)?;
        db.put(&WriteOptions::default(), b"key", b"value")?;
        db.snapshot()?
    };
    assert_eq!(
        expect_error(Db::open(&Options::default(), &snapshot_root)).kind,
        StorageErrorKind::Busy
    );
    drop(snapshot);
    drop(Db::open(&Options::default(), &snapshot_root)?);

    let iterator_root = folder.path().join("iterator-db");
    let iterator = {
        let db = Db::open(&create_options(), &iterator_root)?;
        db.put(&WriteOptions::default(), b"key", b"value")?;
        db.iter(&ReadOptions::default())?
    };
    assert_eq!(
        expect_error(Db::open(&Options::default(), &iterator_root)).kind,
        StorageErrorKind::Busy
    );
    drop(iterator);
    drop(Db::open(&Options::default(), &iterator_root)?);

    let range_root = folder.path().join("range-db");
    let range = {
        let db = Db::open(&create_options(), &range_root)?;
        db.put(&WriteOptions::default(), b"key", b"value")?;
        db.range(
            &ReadOptions::default(),
            KeyRange {
                start: None,
                end: None,
            },
            1,
        )?
    };
    assert_eq!(
        expect_error(Db::open(&Options::default(), &range_root)).kind,
        StorageErrorKind::Busy
    );
    drop(range);
    drop(Db::open(&Options::default(), &range_root)?);
    Ok(())
}

#[test]
fn snapshot_iterator_and_range_remain_fixed_during_concurrent_overwrite_and_delete() -> TestResult {
    const KEY_COUNT: usize = 64;
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let keys = (0..KEY_COUNT)
        .map(|index| format!("key-{index:03}").into_bytes())
        .collect::<Vec<_>>();
    for key in &keys {
        db.put(&WriteOptions::default(), key, b"old")?;
    }

    let snapshot = db.snapshot()?;
    let iterator = db.iter(&ReadOptions {
        snapshot: Some(&snapshot),
    })?;
    let range = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        usize::MAX,
    )?;

    let start = Arc::new(Barrier::new(3));
    let (done_tx, done_rx) = mpsc::channel();

    let writer_db = db.clone();
    let writer_keys = keys.clone();
    let writer_start = Arc::clone(&start);
    let writer_done = done_tx.clone();
    let writer = thread::spawn(move || {
        writer_start.wait();
        for (index, key) in writer_keys.iter().enumerate() {
            if index % 2 == 0 {
                writer_db.delete(&WriteOptions::default(), key).unwrap();
            } else {
                writer_db
                    .put(&WriteOptions::default(), key, b"new")
                    .unwrap();
            }
        }
        drop(writer_db);
        writer_done.send(("writer", None)).unwrap();
    });

    let iterator_start = Arc::clone(&start);
    let iterator_done = done_tx.clone();
    let iterator_reader = thread::spawn(move || {
        let mut iterator = iterator;
        iterator_start.wait();
        iterator.seek_to_first();
        let mut seen = Vec::new();
        while iterator.valid() {
            assert_eq!(iterator.value(), Some(b"old".as_slice()));
            seen.push(iterator.key().unwrap().to_vec());
            iterator.next();
        }
        assert!(iterator.status().is_ok());
        drop(iterator);
        iterator_done.send(("iterator", Some(seen))).unwrap();
    });

    let range_start = Arc::clone(&start);
    let range_done = done_tx;
    let range_reader = thread::spawn(move || {
        let mut range = range;
        range_start.wait();
        let mut seen = Vec::new();
        while range.valid() {
            assert_eq!(range.value(), Some(b"old".as_slice()));
            seen.push(range.key().unwrap().to_vec());
            range.next();
        }
        assert!(range.status().is_ok());
        drop(range);
        range_done.send(("range", Some(seen))).unwrap();
    });

    let mut completed = Vec::new();
    let mut iterator_seen = None;
    let mut range_seen = None;
    for _ in 0..3 {
        let (name, seen) = done_rx.recv_timeout(Duration::from_secs(10))?;
        completed.push(name);
        match name {
            "iterator" => iterator_seen = seen,
            "range" => range_seen = seen,
            "writer" => assert!(seen.is_none()),
            _ => panic!("unknown worker: {name}"),
        }
    }
    completed.sort_unstable();
    assert_eq!(completed, ["iterator", "range", "writer"]);

    assert_eq!(iterator_seen, Some(keys.clone()));
    assert_eq!(range_seen, Some(keys.clone()));
    drop(writer);
    drop(iterator_reader);
    drop(range_reader);
    for key in &keys {
        assert_eq!(
            db.get(
                &ReadOptions {
                    snapshot: Some(&snapshot)
                },
                key
            )?,
            Some(b"old".to_vec())
        );
    }
    Ok(())
}
