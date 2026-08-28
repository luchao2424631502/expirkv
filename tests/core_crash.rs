use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use rustkv::{Db, Options, ReadOptions, WriteBatch, WriteOptions};
use tempfile::TempDir;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CHILD_MODE: &str = "RUSTKV_CORE_CRASH_CHILD";
const CHILD_PATH: &str = "RUSTKV_CORE_CRASH_PATH";
const VLOG_PREPARE_MODE: &str = "vlog-prepare";
const FJALL_COMMIT_MODE: &str = "fjall-commit";
const OPEN_RECOVERY_MODE: &str = "open-recovery";

const VLOG_POINT: &str = "POINT VLOG_PREPARE";
const FJALL_POINT: &str = "POINT FJALL_COMMIT";
const RECOVERY_POINT: &str = "POINT RECOVERY_TRIM";
const WRITE_COMPLETED: &str = "WRITE_COMPLETED";
const OPEN_COMPLETED: &str = "OPEN_COMPLETED";

const SIGKILL: i32 = 9;
const SIGSTOP: i32 = 19;
const CHILD_MONITOR_TIMEOUT: Duration = Duration::from_secs(20);
const PARENT_POINT_TIMEOUT: Duration = Duration::from_secs(30);
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_micros(100);
const VLOG_BATCH_ITEMS: usize = 256;
const VLOG_VALUE_LEN: usize = 59_000;
const FJALL_BATCH_ITEMS: usize = 8_192;
const FJALL_VALUE_LEN: usize = 256;

unsafe extern "C" {
    fn getpid() -> i32;
    fn kill(pid: i32, signal: i32) -> i32;
}

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        // Keep the observation batches below Fjall's memtable limit so an
        // unrelated flush cannot masquerade as the commit journal mutation.
        write_buffer_size: 64 * 1024 * 1024,
        ..Options::default()
    }
}

fn read(db: &Db, key: &[u8]) -> rustkv::Result<Option<Vec<u8>>> {
    db.get(&ReadOptions::default(), key)
}

fn vlog_key(item: usize) -> Vec<u8> {
    format!("vlog-item-{item:04}").into_bytes()
}

fn vlog_value(item: usize) -> Vec<u8> {
    let mut value = vec![0x5a; VLOG_VALUE_LEN];
    value[0] = u8::try_from(item).expect("VLog item fits in u8");
    value
}

fn fjall_key(item: usize) -> Vec<u8> {
    format!("fjall-item-{item:05}").into_bytes()
}

fn fjall_value(item: usize) -> Vec<u8> {
    let mut value = vec![0xa5; FJALL_VALUE_LEN];
    value[0..8].copy_from_slice(&u64::try_from(item).expect("item fits in u64").to_le_bytes());
    value
}

fn vlog_batch() -> TestResult<WriteBatch> {
    let mut batch = WriteBatch::new();
    for item in 0..VLOG_BATCH_ITEMS {
        batch.put(vlog_key(item), vlog_value(item))?;
    }
    Ok(batch)
}

fn fjall_batch() -> TestResult<WriteBatch> {
    let mut batch = WriteBatch::new();
    for item in 0..FJALL_BATCH_ITEMS {
        batch.put(fjall_key(item), fjall_value(item))?;
    }
    Ok(batch)
}

#[test]
fn process_child() -> TestResult {
    let Some(mode) = env::var_os(CHILD_MODE) else {
        return Ok(());
    };
    let root = PathBuf::from(env::var_os(CHILD_PATH).ok_or("missing child database path")?);
    match mode.to_str().ok_or("invalid child mode")? {
        VLOG_PREPARE_MODE => run_vlog_prepare_child(&root),
        FJALL_COMMIT_MODE => run_fjall_commit_child(&root),
        OPEN_RECOVERY_MODE => run_open_recovery_child(&root),
        _ => Err("unknown child mode".into()),
    }
}

fn run_vlog_prepare_child(root: &Path) -> TestResult {
    let db = create_stable_database(root)?;
    let baseline = vlog_logical_bytes(root)?;
    let minimum_complete_payload = u64::try_from(VLOG_BATCH_ITEMS * VLOG_VALUE_LEN)?;

    let monitored_root = root.to_path_buf();
    thread::spawn(move || {
        let deadline = Instant::now() + CHILD_MONITOR_TIMEOUT;
        while Instant::now() < deadline {
            if let Ok(current) = vlog_logical_bytes(&monitored_root) {
                let growth = current.saturating_sub(baseline);
                if growth >= 128 * 1024 && growth < minimum_complete_payload {
                    stop_at_observed_point(&format!("{VLOG_POINT} growth={growth}"));
                }
            }
            thread::sleep(POLL_INTERVAL);
        }
        child_monitor_failed("VLog Prepare point was not observed");
    });

    db.write(&WriteOptions::default(), &vlog_batch()?)?;
    announce(WRITE_COMPLETED)?;
    thread::sleep(CHILD_MONITOR_TIMEOUT);
    Err("VLog write completed before the observation point".into())
}

fn run_fjall_commit_child(root: &Path) -> TestResult {
    let db = create_stable_database(root)?;
    let index_path = root.join("index");
    let baseline_index = wait_for_quiet_tree(&index_path)?;
    let baseline_vlog = vlog_logical_bytes(root)?;
    let minimum_complete_payload = u64::try_from(FJALL_BATCH_ITEMS * FJALL_VALUE_LEN)?;
    let write_completed = Arc::new(AtomicBool::new(false));

    let monitored_root = root.to_path_buf();
    let monitored_index = index_path;
    let monitor_completed = Arc::clone(&write_completed);
    thread::spawn(move || {
        let deadline = Instant::now() + CHILD_MONITOR_TIMEOUT;
        while Instant::now() < deadline {
            let vlog_is_complete = vlog_logical_bytes(&monitored_root)
                .map(|current| current.saturating_sub(baseline_vlog) >= minimum_complete_payload)
                .unwrap_or(false);
            let index_has_changed = tree_fingerprint(&monitored_index)
                .map(|current| current != baseline_index)
                .unwrap_or(false);
            if vlog_is_complete && index_has_changed && !monitor_completed.load(Ordering::Acquire) {
                stop_at_observed_point(FJALL_POINT);
            }
            thread::sleep(POLL_INTERVAL);
        }
        child_monitor_failed("Fjall commit point was not observed");
    });

    db.write(&WriteOptions { sync: true }, &fjall_batch()?)?;
    write_completed.store(true, Ordering::Release);
    announce(WRITE_COMPLETED)?;
    thread::sleep(CHILD_MONITOR_TIMEOUT);
    Err("Fjall commit completed before the observation point".into())
}

fn run_open_recovery_child(root: &Path) -> TestResult {
    let partial_tail = vlog_logical_bytes(root)?;
    let open_completed = Arc::new(AtomicBool::new(false));
    let monitored_root = root.to_path_buf();
    let monitor_completed = Arc::clone(&open_completed);
    thread::spawn(move || {
        let deadline = Instant::now() + CHILD_MONITOR_TIMEOUT;
        while Instant::now() < deadline {
            let trimmed = vlog_logical_bytes(&monitored_root)
                .map(|current| current < partial_tail)
                .unwrap_or(false);
            if trimmed && !monitor_completed.load(Ordering::Acquire) {
                stop_at_observed_point(RECOVERY_POINT);
            }
            thread::sleep(POLL_INTERVAL);
        }
        child_monitor_failed("Recovery Trim point was not observed");
    });

    let _db = Db::open(&Options::default(), root)?;
    open_completed.store(true, Ordering::Release);
    announce(OPEN_COMPLETED)?;
    thread::sleep(CHILD_MONITOR_TIMEOUT);
    Err("Open completed before the recovery observation point".into())
}

fn create_stable_database(root: &Path) -> TestResult<Db> {
    let db = Db::open(&create_options(), root)?;
    db.put(&WriteOptions { sync: true }, b"stable", b"prefix")?;
    let _ = wait_for_quiet_tree(&root.join("index"))?;
    Ok(db)
}

fn announce(marker: &str) -> io::Result<()> {
    println!("{marker}");
    io::stdout().flush()
}

fn stop_at_observed_point(marker: &str) -> ! {
    if announce(marker).is_err() {
        std::process::exit(72);
    }
    // SAFETY: getpid identifies this child process. SIGSTOP freezes every
    // thread at the just-observed physical point until the parent sends
    // SIGKILL; it does not dereference memory.
    let stopped = unsafe { kill(getpid(), SIGSTOP) } == 0;
    if !stopped {
        std::process::exit(73);
    }
    // SIGSTOP only returns if an external actor resumes the process.
    std::process::exit(74);
}

fn child_monitor_failed(message: &str) -> ! {
    let _ = writeln!(io::stderr(), "{message}");
    std::process::exit(75);
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TreeFingerprint(BTreeMap<PathBuf, (u64, u128)>);

fn tree_fingerprint(root: &Path) -> io::Result<TreeFingerprint> {
    let mut entries = BTreeMap::new();
    collect_tree_fingerprint(root, root, &mut entries)?;
    Ok(TreeFingerprint(entries))
}

fn collect_tree_fingerprint(
    root: &Path,
    current: &Path,
    entries: &mut BTreeMap<PathBuf, (u64, u128)>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_tree_fingerprint(root, &path, entries)?;
        } else if metadata.is_file() {
            let modified = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            entries.insert(
                path.strip_prefix(root)
                    .map_err(io::Error::other)?
                    .to_path_buf(),
                (metadata.len(), modified),
            );
        }
    }
    Ok(())
}

fn wait_for_quiet_tree(root: &Path) -> io::Result<TreeFingerprint> {
    const QUIET_FOR: Duration = Duration::from_millis(200);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = tree_fingerprint(root)?;
    let mut unchanged_since = Instant::now();
    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
        let current = tree_fingerprint(root)?;
        if current == last {
            if unchanged_since.elapsed() >= QUIET_FOR {
                return Ok(current);
            }
        } else {
            last = current;
            unchanged_since = Instant::now();
        }
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        "index tree did not become quiet",
    ))
}

fn vlog_logical_bytes(root: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(root.join("vlog"))? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total = total.checked_add(metadata.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "VLog length overflow")
            })?;
        }
    }
    Ok(total)
}

fn spawn_child(mode: &str, root: &Path) -> io::Result<Child> {
    Command::new(env::current_exe()?)
        .arg("--exact")
        .arg("process_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, root)
        .stdout(Stdio::piped())
        .spawn()
}

fn start_line_reader(child: &mut Child) -> io::Result<mpsc::Receiver<String>> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("missing child stdout"))?;
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if sender.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    Ok(receiver)
}

fn wait_for_point(
    child: &mut Child,
    receiver: &mpsc::Receiver<String>,
    point: &str,
    forbidden_completion: &str,
) -> TestResult<String> {
    let deadline = Instant::now() + PARENT_POINT_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            terminate_and_wait(child)?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {point}"),
            )
            .into());
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) if line.starts_with(point) => return Ok(line),
            Ok(line) if line == forbidden_completion => {
                terminate_and_wait(child)?;
                return Err(io::Error::other(format!(
                    "{forbidden_completion} was observed before {point}"
                ))
                .into());
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                terminate_and_wait(child)?;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for {point}"),
                )
                .into());
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = wait_for_exit(child, CHILD_EXIT_TIMEOUT)?;
                return Err(
                    io::Error::other(format!("child exited as {status} before {point}")).into(),
                );
            }
        }
    }
}

fn terminate_and_wait(child: &mut Child) -> io::Result<ExitStatus> {
    terminate(child)?;
    wait_for_exit(child, CHILD_EXIT_TIMEOUT)
}

fn terminate(child: &Child) -> io::Result<()> {
    let pid = i32::try_from(child.id()).map_err(io::Error::other)?;
    // SAFETY: pid names the child created by this test and SIGKILL does not
    // retain pointers or access this process's memory.
    if unsafe { kill(pid, SIGKILL) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "child did not exit before deadline",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn kill_at_point(mode: &str, root: &Path, point: &str, completed: &str) -> TestResult<String> {
    let mut child = spawn_child(mode, root)?;
    let receiver = start_line_reader(&mut child)?;
    let evidence = wait_for_point(&mut child, &receiver, point, completed)?;
    let status = terminate_and_wait(&mut child)?;
    if status.success() {
        return Err(io::Error::other("SIGKILL child unexpectedly succeeded").into());
    }
    Ok(evidence)
}

fn assert_vlog_batch_absent(db: &Db) -> TestResult {
    for item in 0..VLOG_BATCH_ITEMS {
        assert_eq!(read(db, &vlog_key(item))?, None);
    }
    Ok(())
}

#[test]
fn sigkill_at_observed_partial_vlog_prepare_trims_the_transaction() -> TestResult {
    if env::var_os(CHILD_MODE).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let evidence = kill_at_point(VLOG_PREPARE_MODE, &root, VLOG_POINT, WRITE_COMPLETED)?;
    assert!(evidence.contains("growth="));

    let reopened = Db::open(&Options::default(), &root)?;
    assert_eq!(read(&reopened, b"stable")?, Some(b"prefix".to_vec()));
    assert_vlog_batch_absent(&reopened)?;
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, 1);
    assert_eq!(stats.durable_seq, 1);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}

#[test]
fn sigkill_after_observed_fjall_commit_io_recovers_an_atomic_batch() -> TestResult {
    if env::var_os(CHILD_MODE).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let _ = kill_at_point(FJALL_COMMIT_MODE, &root, FJALL_POINT, WRITE_COMPLETED)?;

    let reopened = Db::open(&Options::default(), &root)?;
    assert_eq!(read(&reopened, b"stable")?, Some(b"prefix".to_vec()));
    let transaction_present = read(&reopened, &fjall_key(0))?.is_some();
    for item in 0..FJALL_BATCH_ITEMS {
        let actual = read(&reopened, &fjall_key(item))?;
        if transaction_present {
            assert_eq!(actual, Some(fjall_value(item)));
        } else {
            assert_eq!(actual, None);
        }
    }
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, 1 + u64::from(transaction_present));
    assert_eq!(stats.head_seq, stats.durable_seq);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}

#[test]
fn sigkill_after_observed_recovery_trim_is_reopenable() -> TestResult {
    if env::var_os(CHILD_MODE).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    let root = folder.path().join("db");

    let _ = kill_at_point(VLOG_PREPARE_MODE, &root, VLOG_POINT, WRITE_COMPLETED)?;
    let partial_tail = vlog_logical_bytes(&root)?;
    let _ = kill_at_point(OPEN_RECOVERY_MODE, &root, RECOVERY_POINT, OPEN_COMPLETED)?;
    assert!(vlog_logical_bytes(&root)? < partial_tail);

    let reopened = Db::open(&Options::default(), &root)?;
    assert_eq!(read(&reopened, b"stable")?, Some(b"prefix".to_vec()));
    assert_vlog_batch_absent(&reopened)?;
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, 1);
    assert_eq!(stats.durable_seq, 1);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}
