#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/lock.rs"]
mod lock;

use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use lock::RootLock;
use tempfile::{Builder, TempDir};

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

const CHILD_PATH_ENV: &str = "RUSTKV_ROOT_LOCK_CHILD_PATH";
const CHILD_EXIT_WITH_LOCK_ENV: &str = "RUSTKV_ROOT_LOCK_CHILD_EXIT_WITH_LOCK";
const CHILD_EXIT_CODE: i32 = 87;

fn run_child_bounded(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = command.spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "root-lock child exceeded 10 seconds",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn missing_root_is_not_created_without_permission() -> TestResult {
    let parent = TempDir::new()?;
    let path = parent.path().join("missing");

    assert!(RootLock::acquire(&path, false)?.is_none());
    assert!(!path.exists());
    Ok(())
}

#[test]
fn newly_created_root_parent_sync_failure_prevents_lock_acquisition() -> TestResult {
    let parent = TempDir::new()?;
    let root = parent.path().join("db");

    let error = match RootLock::acquire_with_parent_sync_failure_for_test(&root) {
        Ok(_) => panic!("root acquisition succeeded without syncing its parent directory"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Open);
    assert_eq!(error.protocol_stage, ProtocolStage::Preflight);

    // mkdir has happened, but failure is reported before LOCK is created and
    // before a usable RootLock can escape to the caller.
    assert!(root.is_dir());
    assert!(!root.join("LOCK").exists());
    assert!(RootLock::acquire(&root, false)?.is_some());
    Ok(())
}

#[test]
fn process_table_collapses_absolute_relative_dot_and_dotdot_aliases() -> TestResult {
    let folder = Builder::new().prefix("rustkv-root-lock-").tempdir_in(".")?;
    fs::create_dir(folder.path().join("alias"))?;
    let relative_root = folder.path().join("db");
    let absolute_root = fs::canonicalize(folder.path())?.join("db");
    let first = RootLock::acquire(&absolute_root, true)?.expect("created root lock");

    for alias in [
        relative_root.clone(),
        folder.path().join(".").join("db"),
        folder.path().join("alias").join("..").join("db"),
    ] {
        let error = match RootLock::acquire(&alias, false) {
            Ok(_) => panic!("path alias acquired a second root lock"),
            Err(error) => error,
        };
        assert_eq!(error.kind, StorageErrorKind::Busy);
    }

    drop(first);
    assert!(RootLock::acquire(&relative_root, false)?.is_some());
    Ok(())
}

#[test]
fn root_symlink_alias_resolves_to_the_same_identity() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let alias = folder.path().join("db-alias");
    let first = RootLock::acquire(&root, true)?.expect("root lock");
    std::os::unix::fs::symlink(&root, &alias)?;

    let error = match RootLock::acquire(&alias, false) {
        Ok(_) => panic!("root symlink bypassed the process lock table"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Busy);
    assert_eq!(first.identity().canonical_path, fs::canonicalize(&alias)?);
    Ok(())
}

#[test]
fn lock_managed_object_rejects_symlinks_and_directories() -> TestResult {
    let folder = TempDir::new()?;
    let symlink_root = folder.path().join("symlink-root");
    fs::create_dir(&symlink_root)?;
    let outside = folder.path().join("outside-lock");
    fs::write(&outside, b"outside")?;
    std::os::unix::fs::symlink(&outside, symlink_root.join("LOCK"))?;
    let error = match RootLock::acquire(&symlink_root, false) {
        Ok(_) => panic!("LOCK symlink was followed"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    assert_eq!(fs::read(&outside)?, b"outside");

    let directory_root = folder.path().join("directory-root");
    fs::create_dir(&directory_root)?;
    fs::create_dir(directory_root.join("LOCK"))?;
    let error = match RootLock::acquire(&directory_root, false) {
        Ok(_) => panic!("LOCK directory was accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::InvalidLayout);
    Ok(())
}

#[test]
fn cross_process_root_lock_is_nonblocking_and_exclusive() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let _lock = RootLock::acquire(&root, true)?.expect("parent root lock");

    let status = run_child_bounded(
        Command::new(std::env::current_exe()?)
            .args(["--exact", "root_lock_child", "--nocapture"])
            .env(CHILD_PATH_ENV, &root),
    )?;
    assert!(status.success(), "child failed with {status}");
    Ok(())
}

#[test]
fn process_exit_releases_the_root_lock() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");

    let status = run_child_bounded(
        Command::new(std::env::current_exe()?)
            .args(["--exact", "root_lock_child", "--nocapture"])
            .env(CHILD_PATH_ENV, &root)
            .env(CHILD_EXIT_WITH_LOCK_ENV, "1"),
    )?;
    assert_eq!(status.code(), Some(CHILD_EXIT_CODE));
    assert!(RootLock::acquire(&root, false)?.is_some());
    Ok(())
}

#[test]
fn root_lock_child() -> TestResult {
    let Some(path) = std::env::var_os(CHILD_PATH_ENV).map(PathBuf::from) else {
        return Ok(());
    };
    if std::env::var_os(CHILD_EXIT_WITH_LOCK_ENV).is_some() {
        let _lock = RootLock::acquire(&path, true)?.expect("child root lock");
        std::process::exit(CHILD_EXIT_CODE);
    }
    let error = match RootLock::acquire(&path, false) {
        Ok(_) => panic!("child acquired a root already locked by its parent"),
        Err(error) => error,
    };
    assert_eq!(error.kind, StorageErrorKind::Busy);
    Ok(())
}
