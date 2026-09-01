use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{BackendKind, BackendOperation, BenchBackend, BenchConfig, RustKvBackend};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn rustkv_backend_is_send_sync_and_uses_only_the_frozen_trait() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RustKvBackend>();
    let _: Option<&dyn BenchBackend> = None;
}

#[test]
fn rustkv_invalid_layout_error_preserves_backend_operation_and_text() -> TestResult {
    let temporary = TestDirectory::new("invalid-layout");
    let file_path = temporary.path().join("not-a-database-directory");
    std::fs::write(&file_path, b"regular file")?;
    let error = match RustKvBackend::open(&file_path, &test_config()) {
        Ok(_) => panic!("RustKV unexpectedly opened a regular file as a database"),
        Err(error) => error,
    };
    assert_eq!(error.backend(), BackendKind::RustKv);
    assert_eq!(error.operation(), BackendOperation::Open);
    assert!(!error.source_text().is_empty());
    assert!(error.to_string().contains(error.source_text()));
    Ok(())
}

#[test]
fn rustkv_range_path_statically_uses_iter_and_batch_submits_once() {
    let source = include_str!("../src/backend/rustkv.rs");
    assert!(source.contains(".iter(&self.read_options)"));
    assert!(!source.contains(".range("));
    assert_eq!(
        source
            .matches(".write(&self.write_options, &batch)")
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
            "kv-bench-b2-rustkv-{label}-{}-{time}-{sequence}",
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
