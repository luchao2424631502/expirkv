use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use rustkv::{
    Db, InstanceState, KeyRange, Operation, Options, ReadOptions, StorageErrorKind, WriteOptions,
};
use tempfile::TempDir;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const USER_INDEX_NAME: &str = "rustkv_user_index";

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn vlog_path(root: &Path) -> std::path::PathBuf {
    root.join("vlog").join("D000000.data")
}

fn corrupt_unique_bytes(path: &Path, needle: &[u8]) -> TestResult {
    let bytes = fs::read(path)?;
    let matches = bytes
        .windows(needle.len())
        .enumerate()
        .filter_map(|(offset, current)| (current == needle).then_some(offset))
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "corruption marker must be unique");
    let offset = u64::try_from(matches[0])?;
    let file = OpenOptions::new().write(true).open(path)?;
    let replacement = [needle[0] ^ 0xff];
    assert_eq!(file.write_at(&replacement, offset)?, replacement.len());
    file.sync_all()?;
    Ok(())
}

fn rewrite_user_pointer(root: &Path, target: &[u8], pointer: Vec<u8>) -> TestResult {
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let user = database.keyspace(USER_INDEX_NAME, KeyspaceCreateOptions::default)?;
    user.insert(target, pointer)?;
    database.persist(PersistMode::SyncAll)?;
    Ok(())
}

fn read_user_pointer(root: &Path, key: &[u8]) -> TestResult<Vec<u8>> {
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let user = database.keyspace(USER_INDEX_NAME, KeyspaceCreateOptions::default)?;
    Ok(user
        .get(key)?
        .ok_or("missing test pointer")?
        .as_ref()
        .to_vec())
}

#[test]
fn iterator_crc_failure_is_terminal_poisons_runtime_and_blocks_new_views() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let marker = vec![0x93; 257];
    db.put(&WriteOptions { sync: true }, b"crc-key", &marker)?;
    let snapshot = db.snapshot()?;
    let mut iterator = db.iter(&ReadOptions {
        snapshot: Some(&snapshot),
    })?;
    corrupt_unique_bytes(&vlog_path(&root), &marker)?;

    iterator.seek_to_first();
    assert!(!iterator.valid());
    assert_eq!(iterator.key(), None);
    assert_eq!(iterator.value(), None);
    let error = iterator.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Iterator);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));

    iterator.seek_to_first();
    iterator.next();
    iterator.prev();
    assert_eq!(
        iterator.status().unwrap_err().kind,
        StorageErrorKind::Corruption
    );
    assert_eq!(db.stats().instance_state, InstanceState::Poisoned);

    let snapshot_error = match db.snapshot() {
        Ok(_) => panic!("poisoned database created a snapshot"),
        Err(error) => error,
    };
    assert_eq!(snapshot_error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(snapshot_error.operation, Operation::Snapshot);

    let iterator_error = match db.iter(&ReadOptions::default()) {
        Ok(_) => panic!("poisoned database created an iterator"),
        Err(error) => error,
    };
    assert_eq!(iterator_error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(iterator_error.operation, Operation::Iterator);

    let range_error = match db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        1,
    ) {
        Ok(_) => panic!("poisoned database created a range"),
        Err(error) => error,
    };
    assert_eq!(range_error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(range_error.operation, Operation::Range);

    let get_error = db
        .get(
            &ReadOptions {
                snapshot: Some(&snapshot),
            },
            b"crc-key",
        )
        .unwrap_err();
    assert_eq!(get_error.kind, StorageErrorKind::StoragePoisoned);
    Ok(())
}

#[test]
fn valid_iterator_and_range_fail_on_the_first_move_after_external_poison() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let damaged_value = vec![0xd7; 283];
    db.put(&WriteOptions { sync: true }, b"a", b"first")?;
    db.put(&WriteOptions { sync: true }, b"b", b"second")?;
    db.put(&WriteOptions { sync: true }, b"z-poison", &damaged_value)?;

    let mut iterator = db.iter(&ReadOptions::default())?;
    iterator.seek_to_first();
    assert!(iterator.valid());
    assert_eq!(iterator.key(), Some(b"a".as_slice()));
    assert_eq!(iterator.value(), Some(b"first".as_slice()));

    let mut range = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        10,
    )?;
    assert!(range.valid());
    assert_eq!(range.key(), Some(b"a".as_slice()));
    assert_eq!(range.value(), Some(b"first".as_slice()));

    corrupt_unique_bytes(&vlog_path(&root), &damaged_value)?;
    let poison = db.get(&ReadOptions::default(), b"z-poison").unwrap_err();
    assert_eq!(poison.kind, StorageErrorKind::Corruption);
    assert_eq!(poison.operation, Operation::Get);
    assert_eq!(poison.instance_state, Some(InstanceState::Poisoned));

    iterator.next();
    assert!(!iterator.valid());
    assert_eq!(iterator.key(), None);
    assert_eq!(iterator.value(), None);
    let iterator_error = iterator.status().unwrap_err();
    assert_eq!(iterator_error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(iterator_error.operation, Operation::Iterator);
    assert_eq!(iterator_error.instance_state, Some(InstanceState::Poisoned));

    range.next();
    assert!(!range.valid());
    assert_eq!(range.key(), None);
    assert_eq!(range.value(), None);
    let range_error = range.status().unwrap_err();
    assert_eq!(range_error.kind, StorageErrorKind::StoragePoisoned);
    assert_eq!(range_error.operation, Operation::Range);
    assert_eq!(range_error.instance_state, Some(InstanceState::Poisoned));
    Ok(())
}

#[test]
fn range_crc_failure_returns_a_failed_cursor_with_the_original_error() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let marker = vec![0x5c; 193];
    db.put(&WriteOptions { sync: true }, b"range-key", &marker)?;
    corrupt_unique_bytes(&vlog_path(&root), &marker)?;

    let cursor = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        10,
    )?;
    assert!(!cursor.valid());
    let error = cursor.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Range);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    Ok(())
}

#[test]
fn malformed_pointer_and_missing_file_never_become_not_found() -> TestResult {
    let malformed_folder = TempDir::new()?;
    let malformed_root = malformed_folder.path().join("db");
    {
        let db = Db::open(&create_options(), &malformed_root)?;
        db.put(&WriteOptions { sync: true }, b"pointer-key", b"value")?;
    }
    rewrite_user_pointer(&malformed_root, b"pointer-key", vec![0; 15])?;
    let malformed = Db::open(&Options::default(), &malformed_root)?;
    let mut iterator = malformed.iter(&ReadOptions::default())?;
    iterator.seek_to_first();
    assert_eq!(
        iterator.status().unwrap_err().kind,
        StorageErrorKind::Corruption
    );
    assert_eq!(malformed.stats().instance_state, InstanceState::Poisoned);

    let missing_folder = TempDir::new()?;
    let missing_root = missing_folder.path().join("db");
    let missing = Db::open(&create_options(), &missing_root)?;
    missing.put(&WriteOptions { sync: true }, b"missing-file", b"value")?;
    fs::remove_file(vlog_path(&missing_root))?;
    let mut iterator = missing.iter(&ReadOptions::default())?;
    iterator.seek_to_first();
    let error = iterator.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_ne!(error.kind, StorageErrorKind::NotFound);
    Ok(())
}

#[test]
fn pointer_to_another_keys_valid_record_detects_user_key_mismatch() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    {
        let db = Db::open(&create_options(), &root)?;
        db.put(&WriteOptions { sync: true }, b"a-source", b"source-value")?;
        db.put(&WriteOptions { sync: true }, b"b-target", b"target-value")?;
    }
    let source_pointer = read_user_pointer(&root, b"a-source")?;
    rewrite_user_pointer(&root, b"b-target", source_pointer)?;

    let db = Db::open(&Options::default(), &root)?;
    let mut iterator = db.iter(&ReadOptions::default())?;
    iterator.seek(b"b-target");
    let error = iterator.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Iterator);
    assert_eq!(db.stats().instance_state, InstanceState::Poisoned);
    Ok(())
}

#[test]
fn iterator_next_and_prev_fail_on_the_first_damaged_current_record() -> TestResult {
    for move_forward in [true, false] {
        let folder = TempDir::new()?;
        let root = folder.path().join("db");
        let db = Db::open(&create_options(), &root)?;
        let damaged_value = if move_forward {
            vec![0xa7; 307]
        } else {
            vec![0xb8; 311]
        };
        db.put(&WriteOptions { sync: true }, b"a", b"first")?;
        db.put(&WriteOptions { sync: true }, b"b", &damaged_value)?;
        db.put(&WriteOptions { sync: true }, b"c", b"last")?;
        let mut iterator = db.iter(&ReadOptions::default())?;
        corrupt_unique_bytes(&vlog_path(&root), &damaged_value)?;

        if move_forward {
            iterator.seek(b"a");
            assert_eq!(iterator.key(), Some(b"a".as_slice()));
            iterator.next();
        } else {
            iterator.seek_to_last();
            assert_eq!(iterator.key(), Some(b"c".as_slice()));
            iterator.prev();
        }

        assert!(!iterator.valid());
        assert_eq!(iterator.key(), None);
        assert_eq!(iterator.value(), None);
        let error = iterator.status().unwrap_err();
        assert_eq!(error.kind, StorageErrorKind::Corruption);
        assert_eq!(error.operation, Operation::Iterator);
        assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    }
    Ok(())
}

#[test]
fn range_next_fails_when_an_in_range_later_record_is_damaged() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let damaged_value = vec![0xc9; 313];
    db.put(&WriteOptions { sync: true }, b"a", b"first")?;
    db.put(&WriteOptions { sync: true }, b"b", &damaged_value)?;
    let mut range = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        usize::MAX,
    )?;
    assert_eq!(range.key(), Some(b"a".as_slice()));
    corrupt_unique_bytes(&vlog_path(&root), &damaged_value)?;

    range.next();
    assert!(!range.valid());
    assert_eq!(range.key(), None);
    assert_eq!(range.value(), None);
    let error = range.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Range);
    assert_eq!(error.instance_state, Some(InstanceState::Poisoned));
    Ok(())
}

#[test]
fn range_never_materializes_a_damaged_record_at_its_exclusive_end() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let damaged_value = vec![0xda; 317];
    db.put(&WriteOptions { sync: true }, b"a", b"inside")?;
    db.put(&WriteOptions { sync: true }, b"c", &damaged_value)?;
    corrupt_unique_bytes(&vlog_path(&root), &damaged_value)?;

    let mut prefix = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"a"),
            end: Some(b"c"),
        },
        usize::MAX,
    )?;
    assert_eq!(prefix.key(), Some(b"a".as_slice()));
    assert_eq!(prefix.value(), Some(b"inside".as_slice()));
    prefix.next();
    assert!(!prefix.valid());
    assert!(prefix.status().is_ok());
    assert_eq!(db.stats().instance_state, InstanceState::Healthy);

    let empty = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"b"),
            end: Some(b"c"),
        },
        usize::MAX,
    )?;
    assert!(!empty.valid());
    assert!(empty.status().is_ok());
    assert_eq!(db.stats().instance_state, InstanceState::Healthy);
    Ok(())
}

#[test]
fn minimum_exclusive_end_returns_empty_without_reading_the_first_record() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let db = Db::open(&create_options(), &root)?;
    let damaged_value = vec![0xeb; 319];
    db.put(&WriteOptions { sync: true }, b"a", &damaged_value)?;
    corrupt_unique_bytes(&vlog_path(&root), &damaged_value)?;

    let empty = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: Some(b""),
        },
        usize::MAX,
    )?;
    assert!(!empty.valid());
    assert_eq!(empty.key(), None);
    assert_eq!(empty.value(), None);
    assert!(empty.status().is_ok());
    assert_eq!(db.stats().instance_state, InstanceState::Healthy);
    Ok(())
}

#[test]
fn range_rejects_malformed_pointer_missing_file_and_record_key_mismatch() -> TestResult {
    let malformed_folder = TempDir::new()?;
    let malformed_root = malformed_folder.path().join("db");
    {
        let db = Db::open(&create_options(), &malformed_root)?;
        db.put(&WriteOptions { sync: true }, b"malformed", b"value")?;
    }
    rewrite_user_pointer(&malformed_root, b"malformed", vec![0; 15])?;
    let malformed = Db::open(&Options::default(), &malformed_root)?;
    let malformed_range = malformed.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        1,
    )?;
    assert_eq!(
        malformed_range.status().unwrap_err().kind,
        StorageErrorKind::Corruption
    );
    assert_eq!(
        malformed_range.status().unwrap_err().operation,
        Operation::Range
    );

    let missing_folder = TempDir::new()?;
    let missing_root = missing_folder.path().join("db");
    let missing = Db::open(&create_options(), &missing_root)?;
    missing.put(&WriteOptions { sync: true }, b"missing", b"value")?;
    fs::remove_file(vlog_path(&missing_root))?;
    let missing_range = missing.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        1,
    )?;
    let missing_error = missing_range.status().unwrap_err();
    assert_eq!(missing_error.kind, StorageErrorKind::Corruption);
    assert_eq!(missing_error.operation, Operation::Range);

    let mismatch_folder = TempDir::new()?;
    let mismatch_root = mismatch_folder.path().join("db");
    {
        let db = Db::open(&create_options(), &mismatch_root)?;
        db.put(&WriteOptions { sync: true }, b"a-source", b"source")?;
        db.put(&WriteOptions { sync: true }, b"b-target", b"target")?;
    }
    let source_pointer = read_user_pointer(&mismatch_root, b"a-source")?;
    rewrite_user_pointer(&mismatch_root, b"b-target", source_pointer)?;
    let mismatch = Db::open(&Options::default(), &mismatch_root)?;
    let mismatch_range = mismatch.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"b-target"),
            end: None,
        },
        1,
    )?;
    let mismatch_error = mismatch_range.status().unwrap_err();
    assert_eq!(mismatch_error.kind, StorageErrorKind::Corruption);
    assert_eq!(mismatch_error.operation, Operation::Range);
    assert_eq!(mismatch.stats().instance_state, InstanceState::Poisoned);
    Ok(())
}
