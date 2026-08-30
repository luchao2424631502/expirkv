use rustkv::{Db, Options, ReadOptions, StorageErrorKind, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn populate(db: &Db) -> rustkv::Result<()> {
    for (key, value) in [
        (b"a".as_slice(), b"one".as_slice()),
        (b"c".as_slice(), b"three".as_slice()),
        (b"e".as_slice(), b"five".as_slice()),
    ] {
        db.put(&WriteOptions::default(), key, value)?;
    }
    Ok(())
}

fn assert_current(iterator: &rustkv::DbIterator, key: &[u8], value: &[u8]) {
    assert!(iterator.valid());
    assert_eq!(iterator.key(), Some(key));
    assert_eq!(iterator.value(), Some(value));
    assert!(iterator.status().is_ok());
}

fn assert_normal_invalid(iterator: &rustkv::DbIterator) {
    assert!(!iterator.valid());
    assert_eq!(iterator.key(), None);
    assert_eq!(iterator.value(), None);
    assert!(iterator.status().is_ok());
}

#[test]
fn cursor_state_seek_movement_and_direction_switch_follow_leveldb_semantics() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    populate(&db)?;
    let mut iterator = db.iter(&ReadOptions::default())?;

    assert_normal_invalid(&iterator);
    iterator.next();
    iterator.prev();
    assert_normal_invalid(&iterator);

    iterator.seek_to_first();
    assert_current(&iterator, b"a", b"one");
    iterator.next();
    assert_current(&iterator, b"c", b"three");
    iterator.prev();
    assert_current(&iterator, b"a", b"one");
    iterator.prev();
    assert_normal_invalid(&iterator);

    iterator.seek_to_last();
    assert_current(&iterator, b"e", b"five");
    iterator.prev();
    assert_current(&iterator, b"c", b"three");
    iterator.next();
    assert_current(&iterator, b"e", b"five");
    iterator.next();
    assert_normal_invalid(&iterator);

    iterator.seek(b"c");
    assert_current(&iterator, b"c", b"three");
    iterator.seek(b"b");
    assert_current(&iterator, b"c", b"three");
    iterator.seek(b"");
    assert_current(&iterator, b"a", b"one");
    iterator.seek(b"z");
    assert_normal_invalid(&iterator);
    iterator.seek_to_first();
    assert_current(&iterator, b"a", b"one");
    Ok(())
}

#[test]
fn iterator_uses_a_fixed_implicit_or_explicit_snapshot() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    populate(&db)?;
    let snapshot = db.snapshot()?;
    let mut implicit = db.iter(&ReadOptions::default())?;
    let mut explicit = db.iter(&ReadOptions {
        snapshot: Some(&snapshot),
    })?;

    db.put(&WriteOptions::default(), b"b", b"two")?;
    db.delete(&WriteOptions::default(), b"c")?;
    db.put(&WriteOptions::default(), b"e", b"new-five")?;

    for iterator in [&mut implicit, &mut explicit] {
        iterator.seek_to_first();
        assert_current(iterator, b"a", b"one");
        iterator.next();
        assert_current(iterator, b"c", b"three");
        iterator.next();
        assert_current(iterator, b"e", b"five");
        iterator.next();
        assert_normal_invalid(iterator);
    }
    Ok(())
}

#[test]
fn empty_and_failed_cursors_obey_terminal_state_rules() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let mut empty = db.iter(&ReadOptions::default())?;
    empty.seek_to_first();
    assert_normal_invalid(&empty);
    empty.seek_to_last();
    assert_normal_invalid(&empty);

    let too_large = vec![b'x'; 60_001];
    empty.seek(&too_large);
    assert!(!empty.valid());
    let error = empty.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    let kind = error.kind;
    empty.seek_to_first();
    empty.seek_to_last();
    empty.seek(b"");
    empty.next();
    empty.prev();
    assert_eq!(empty.status().unwrap_err().kind, kind);
    assert_eq!(empty.key(), None);
    assert_eq!(empty.value(), None);
    Ok(())
}

#[test]
fn seek_accepts_the_exact_sixty_thousand_byte_boundary() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let maximum_target = vec![b'k'; 60_000];
    db.put(&WriteOptions::default(), b"a", b"short")?;
    db.put(&WriteOptions::default(), &maximum_target, b"")?;

    let mut iterator = db.iter(&ReadOptions::default())?;
    iterator.seek(&maximum_target);
    assert_current(&iterator, &maximum_target, b"");
    iterator.next();
    assert_normal_invalid(&iterator);
    iterator.seek(&maximum_target);
    assert_current(&iterator, &maximum_target, b"");
    Ok(())
}
