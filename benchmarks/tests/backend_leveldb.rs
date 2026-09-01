use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{
    BackendKind, BackendOperation, BatchItem, BenchBackend, BenchConfig, EXPECTED_LEVELDB_VERSION,
    LevelDbBackend, ScanRequest, linked_leveldb_version,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn leveldb_backend_is_send_sync_and_links_exact_version() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LevelDbBackend>();
    assert_eq!(linked_leveldb_version(), EXPECTED_LEVELDB_VERSION);
}

#[test]
fn leveldb_get_and_not_found_loop_has_clean_error_state() -> TestResult {
    let temporary = TestDirectory::new("get-loop");
    let backend = LevelDbBackend::open(temporary.path().join("db"), &test_config())?;
    backend.put(b"present", b"value")?;
    for _ in 0..2_000 {
        let present = backend.get(b"present")?;
        assert!(present.found);
        assert_eq!(present.value_length, 5);
        let missing = backend.get(b"missing")?;
        assert!(!missing.found);
        assert_eq!(missing.value_length, 0);
    }
    Ok(())
}

#[test]
fn leveldb_invalid_layout_error_preserves_backend_operation_and_text() -> TestResult {
    let temporary = TestDirectory::new("invalid-layout");
    let file_path = temporary.path().join("not-a-database-directory");
    std::fs::write(&file_path, b"regular file")?;
    let error = match LevelDbBackend::open(&file_path, &test_config()) {
        Ok(_) => panic!("LevelDB unexpectedly opened a regular file as a database"),
        Err(error) => error,
    };
    assert_eq!(error.backend(), BackendKind::LevelDb);
    assert_eq!(error.operation(), BackendOperation::Open);
    assert!(!error.source_text().is_empty());
    assert!(error.to_string().contains(error.source_text()));
    Ok(())
}

#[cfg(unix)]
#[test]
fn leveldb_rejects_an_interior_nul_path_before_entering_the_c_api() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let path = PathBuf::from(OsString::from_vec(b"invalid\0leveldb-path".to_vec()));
    let error = match LevelDbBackend::open(&path, &test_config()) {
        Ok(_) => panic!("LevelDB unexpectedly accepted an interior-NUL path"),
        Err(error) => error,
    };
    assert_eq!(error.backend(), BackendKind::LevelDb);
    assert_eq!(error.operation(), BackendOperation::Open);
    assert!(error.source_text().contains("interior NUL"));
}

#[test]
fn linked_binary_exposes_exactly_two_benchmark_aggregate_symbols() -> TestResult {
    let temporary = TestDirectory::new("symbols");
    let backend = LevelDbBackend::open(temporary.path().join("db"), &test_config())?;
    backend.write_batch(&[BatchItem::Put {
        key: b"key",
        value: b"value",
    }])?;
    backend.iterator_scan(ScanRequest::timed(b"", 1, 5))?;

    let output = Command::new("nm")
        .arg("-g")
        .arg(std::env::current_exe()?)
        .output()?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    let mut symbols: Vec<_> = stdout
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .map(|symbol| symbol.trim_start_matches('_'))
        .filter(|symbol| symbol.starts_with("bench_leveldb_"))
        .collect();
    symbols.sort_unstable();
    symbols.dedup();
    assert_eq!(
        symbols,
        ["bench_leveldb_iterator_scan", "bench_leveldb_write_batch"]
    );
    Ok(())
}

#[test]
fn c_aggregate_source_has_only_two_exports_and_one_batch_commit() {
    let header = include_str!("../native/leveldb_aggregate.h");
    let source = include_str!("../native/leveldb_aggregate.c");
    assert_eq!(header.matches("void bench_leveldb_").count(), 2);
    assert_eq!(source.matches("void bench_leveldb_").count(), 2);
    assert!(!source.contains("strlen("));
    assert_eq!(
        source
            .matches("leveldb_write(db, options, batch, error)")
            .count(),
        1
    );
}

fn test_config() -> BenchConfig {
    BenchConfig::test_only(100, 10, 10, 16, 8)
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kv-bench-b2-leveldb-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique temporary directory must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
