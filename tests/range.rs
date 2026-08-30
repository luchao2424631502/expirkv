use rustkv::{Db, KeyRange, Operation, Options, ReadOptions, StorageErrorKind, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn populate(db: &Db) -> rustkv::Result<()> {
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        db.put(&WriteOptions::default(), key, key)?;
    }
    Ok(())
}

fn collect(mut cursor: rustkv::RangeCursor) -> (Vec<(Vec<u8>, Vec<u8>)>, Option<StorageErrorKind>) {
    let mut entries = Vec::new();
    while cursor.valid() {
        entries.push((
            cursor.key().unwrap().to_vec(),
            cursor.value().unwrap().to_vec(),
        ));
        cursor.next();
    }
    let error = cursor.status().err().map(|error| error.kind);
    (entries, error)
}

fn keys(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    entries.iter().map(|(key, _)| key.clone()).collect()
}

#[test]
fn range_is_half_open_streaming_bounded_and_supports_empty_sort_boundaries() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    populate(&db)?;

    let (all, error) = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: None,
        },
        usize::MAX,
    )?);
    assert_eq!(keys(&all), [b"a", b"b", b"c", b"d", b"e"]);
    assert_eq!(error, None);

    let (middle, _) = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"b"),
            end: Some(b"e"),
        },
        usize::MAX,
    )?);
    assert_eq!(keys(&middle), [b"b", b"c", b"d"]);

    let (lower_bound, _) = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"bb"),
            end: None,
        },
        2,
    )?);
    assert_eq!(keys(&lower_bound), [b"c", b"d"]);

    let (empty_start, _) = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b""),
            end: Some(b"c"),
        },
        usize::MAX,
    )?);
    assert_eq!(keys(&empty_start), [b"a", b"b"]);

    let (empty_end, _) = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: Some(b""),
        },
        usize::MAX,
    )?);
    assert!(empty_end.is_empty());
    Ok(())
}

#[test]
fn empty_reversed_and_limited_ranges_end_normally_without_movement() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    populate(&db)?;

    for (range, limit) in [
        (
            KeyRange {
                start: Some(b"c"),
                end: Some(b"c"),
            },
            usize::MAX,
        ),
        (
            KeyRange {
                start: Some(b"d"),
                end: Some(b"b"),
            },
            usize::MAX,
        ),
        (
            KeyRange {
                start: None,
                end: None,
            },
            0,
        ),
    ] {
        let mut cursor = db.range(&ReadOptions::default(), range, limit)?;
        assert!(!cursor.valid());
        assert_eq!(cursor.key(), None);
        assert_eq!(cursor.value(), None);
        assert!(cursor.status().is_ok());
        cursor.next();
        assert!(cursor.status().is_ok());
    }
    Ok(())
}

#[test]
fn implicit_and_explicit_range_views_are_fixed() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    populate(&db)?;
    let snapshot = db.snapshot()?;
    let mut implicit = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"b"),
            end: Some(b"e"),
        },
        10,
    )?;
    let mut explicit = db.range(
        &ReadOptions {
            snapshot: Some(&snapshot),
        },
        KeyRange {
            start: Some(b"b"),
            end: Some(b"e"),
        },
        10,
    )?;

    db.delete(&WriteOptions::default(), b"b")?;
    db.put(&WriteOptions::default(), b"bb", b"bb")?;
    db.put(&WriteOptions::default(), b"c", b"new")?;

    for cursor in [&mut implicit, &mut explicit] {
        let mut seen = Vec::new();
        while cursor.valid() {
            seen.push((
                cursor.key().unwrap().to_vec(),
                cursor.value().unwrap().to_vec(),
            ));
            cursor.next();
        }
        assert_eq!(
            seen,
            [
                (b"b".to_vec(), b"b".to_vec()),
                (b"c".to_vec(), b"c".to_vec()),
                (b"d".to_vec(), b"d".to_vec())
            ]
        );
        assert!(cursor.status().is_ok());
    }
    Ok(())
}

#[test]
fn range_boundaries_accept_exactly_sixty_thousand_bytes_and_reject_more() -> TestResult {
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let maximum = vec![b'k'; 60_000];
    db.put(&WriteOptions::default(), b"a", b"short")?;
    db.put(&WriteOptions::default(), &maximum, b"")?;

    let maximum_start = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(&maximum),
            end: None,
        },
        1,
    )?;
    assert!(maximum_start.valid());
    assert_eq!(maximum_start.key(), Some(maximum.as_slice()));
    assert_eq!(maximum_start.value(), Some(b"".as_slice()));

    let maximum_end = collect(db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: Some(&maximum),
        },
        usize::MAX,
    )?);
    assert_eq!(keys(&maximum_end.0), [b"a"]);
    assert_eq!(maximum_end.1, None);

    let too_large = vec![b'x'; 60_001];
    let error = match db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(&too_large),
            end: None,
        },
        1,
    ) {
        Ok(_) => panic!("oversized range boundary unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(error.operation, Operation::Range);

    let end_error = match db.range(
        &ReadOptions::default(),
        KeyRange {
            start: None,
            end: Some(&too_large),
        },
        0,
    ) {
        Ok(_) => panic!("oversized end boundary unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(end_error.kind, StorageErrorKind::InvalidArgument);
    assert_eq!(end_error.operation, Operation::Range);
    Ok(())
}
