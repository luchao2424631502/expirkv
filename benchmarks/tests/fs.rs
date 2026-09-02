use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kv_bench::{BackendKind, BenchmarkWorkspace, FsErrorKind};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn workspace_never_adopts_existing_relative_or_root_paths() -> TestResult {
    let area = TestArea::new("workspace-boundaries");
    assert_eq!(
        BenchmarkWorkspace::create("relative-workspace")
            .unwrap_err()
            .kind(),
        FsErrorKind::InvalidPath
    );
    assert_eq!(
        BenchmarkWorkspace::create(Path::new("/"))
            .unwrap_err()
            .kind(),
        FsErrorKind::InvalidPath
    );
    let existing = area.path().join("existing");
    std::fs::create_dir(&existing)?;
    assert_eq!(
        BenchmarkWorkspace::create(&existing).unwrap_err().kind(),
        FsErrorKind::AlreadyExists
    );
    Ok(())
}

#[test]
fn run_names_are_confined_and_preexisting_destinations_are_never_overwritten() -> TestResult {
    let area = TestArea::new("run-names");
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
    for invalid in ["", ".", "../escape", "slash/name", "white space"] {
        assert_eq!(
            workspace
                .create_empty_run(BackendKind::RustKv, invalid)
                .unwrap_err()
                .kind(),
            FsErrorKind::InvalidPath
        );
    }
    let first = workspace.create_empty_run(BackendKind::RustKv, "same-label")?;
    std::fs::write(first.path_for_test().join("sentinel"), b"first")?;
    assert_eq!(
        workspace
            .create_empty_run(BackendKind::RustKv, "same-label")
            .unwrap_err()
            .kind(),
        FsErrorKind::AlreadyExists
    );
    assert_eq!(
        std::fs::read(first.path_for_test().join("sentinel"))?,
        b"first"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn cleanup_rejects_symlinks_and_preserves_unregistered_siblings() -> TestResult {
    use std::os::unix::fs::symlink;

    let area = TestArea::new("cleanup");
    let sibling = area.path().join("user-sentinel");
    std::fs::write(&sibling, b"preserve")?;
    let outside = area.path().join("outside");
    std::fs::write(&outside, b"outside")?;
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;

    let unsafe_run = workspace.create_empty_run(BackendKind::LevelDb, "unsafe")?;
    symlink(&outside, unsafe_run.path_for_test().join("link"))?;
    assert_eq!(
        workspace.cleanup_run(&unsafe_run).unwrap_err().kind(),
        FsErrorKind::UnsafeLayout
    );
    assert_eq!(std::fs::read(&outside)?, b"outside");

    let clean_run = workspace.create_empty_run(BackendKind::RustKv, "clean")?;
    std::fs::write(clean_run.path_for_test().join("owned"), b"data")?;
    workspace.cleanup_run(&clean_run)?;
    assert!(!clean_run.path_for_test().exists());
    assert_eq!(std::fs::read(&sibling)?, b"preserve");
    assert_eq!(std::fs::read(&outside)?, b"outside");
    Ok(())
}

#[test]
fn a_directory_token_cannot_cross_workspace_registries() -> TestResult {
    let area = TestArea::new("registries");
    let first = BenchmarkWorkspace::create(area.path().join("first"))?;
    let second = BenchmarkWorkspace::create(area.path().join("second"))?;
    let run = first.create_empty_run(BackendKind::RustKv, "owned-by-first")?;
    assert_eq!(
        second.cleanup_run(&run).unwrap_err().kind(),
        FsErrorKind::Unregistered
    );
    assert!(run.path_for_test().is_dir());
    Ok(())
}

#[test]
fn a_replaced_directory_root_is_never_treated_as_the_registered_database() -> TestResult {
    let area = TestArea::new("replaced-root");
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
    let run = workspace.create_empty_run(BackendKind::RustKv, "replace-me")?;
    std::fs::remove_dir(run.path_for_test())?;
    std::fs::create_dir(run.path_for_test())?;
    std::fs::write(run.path_for_test().join("foreign"), b"do-not-delete")?;

    assert_eq!(
        workspace.cleanup_run(&run).unwrap_err().kind(),
        FsErrorKind::UnsafeLayout
    );
    assert_eq!(
        std::fs::read(run.path_for_test().join("foreign"))?,
        b"do-not-delete"
    );
    Ok(())
}

#[test]
fn a_stale_token_cannot_delete_a_new_run_that_reuses_the_same_label() -> TestResult {
    let area = TestArea::new("stale-token");
    let workspace = BenchmarkWorkspace::create(area.path().join("workspace"))?;
    let stale = workspace.create_empty_run(BackendKind::LevelDb, "reused")?;
    workspace.cleanup_run(&stale)?;

    let current = workspace.create_empty_run(BackendKind::LevelDb, "reused")?;
    std::fs::write(current.path_for_test().join("current"), b"preserve")?;
    assert_eq!(
        workspace.cleanup_run(&stale).unwrap_err().kind(),
        FsErrorKind::Unregistered
    );
    assert_eq!(
        std::fs::read(current.path_for_test().join("current"))?,
        b"preserve"
    );
    workspace.cleanup_run(&current)?;
    Ok(())
}

struct TestArea {
    path: PathBuf,
}

impl TestArea {
    fn new(label: &str) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after Unix epoch")
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "kv-bench-b5-fs-{label}-{}-{time}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("unique test area must be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestArea {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
