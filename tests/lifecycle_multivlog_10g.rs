//! Explicit stage-16 lifecycle, cleanup, and Destroy coverage over the shared
//! production-geometry 10 GiB fixture.
//!
//! Run with:
//! `cargo test --test lifecycle_multivlog_10g -- --ignored --exact ten_gib_cleanup_lifecycle_and_destroy --nocapture`

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use rustkv::{
    Db, DestroyStage, InstanceState, ManagedObject, Options, StorageErrorKind, WriteOptions,
};
use tempfile::TempDir;

#[path = "core_multivlog_10g.rs"]
mod multivlog;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const CHILD_MODE_ENV: &str = "RUSTKV_STAGE16_HEAVY_CHILD";
const CHILD_PATH_ENV: &str = "RUSTKV_STAGE16_HEAVY_PATH";
const FIXTURE_MODE: &str = "fixture";
const STARTED_WRITE_MODE: &str = "started-write";
const FIXTURE_READY: &str = "FIXTURE_READY";
const CLEANUP_DROP_VERIFIED: &str = "CLEANUP_DROP_VERIFIED";
const WRITE_BEGIN: &str = "WRITE_BEGIN";
const WRITE_FINISHED: &str = "WRITE_FINISHED";
const WRITE_DROP_FINISHED: &str = "WRITE_DROP_FINISHED";
const FIXTURE_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DROP_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(5);
const CLEANUP_DROP_ATTEMPT_WINDOWS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_secs(1),
    Duration::from_secs(4),
    Duration::from_secs(16),
];
const TRANSACTION_KEYSPACE: &str = "rustkv_txn_metadata";

#[cfg(target_os = "linux")]
const SIGSTOP: i32 = 19;
#[cfg(target_os = "linux")]
const SIGCONT: i32 = 18;
#[cfg(target_os = "macos")]
const SIGSTOP: i32 = 17;
#[cfg(target_os = "macos")]
const SIGCONT: i32 = 19;

unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

fn announce(marker: &str) -> io::Result<()> {
    println!("{marker}");
    io::stdout().flush()
}

fn fixture_child(root: &Path) -> TestResult {
    let (db, prior_durable_seq, prior_head_seq) = multivlog::build_stage16_cleanup_fixture(root)?;
    drop(db);

    // First prove that the worker made progress during the 10 GiB write, then
    // remove the remaining stable Descriptors while no RustKV instance is open.
    // Reopening below gives the Drop-overlap phase one unambiguous captured
    // frontier instead of leaving it queued behind older cleanup passes.
    verify_worker_progress_and_clear_stable_descriptors(root, prior_durable_seq)?;

    let mut cleanup_range = None;
    let mut window_index = 0_usize;
    for attempt in 0..8 {
        let overlap_window = CLEANUP_DROP_ATTEMPT_WINDOWS[window_index];
        let db = Db::open(&Options::default(), root)?;
        let reopened = db.stats();
        if attempt == 0 {
            assert_eq!(reopened.head_seq, prior_head_seq);
            assert!(
                (prior_durable_seq..=prior_head_seq).contains(&reopened.durable_seq),
                "reopen published a frontier outside the previously accepted prefix"
            );
        }

        let (cleanup_first_seq, cleanup_last_seq) = match cleanup_range {
            Some(range) => range,
            None => {
                let range = multivlog::append_stage16_cleanup_backlog(&db)?;
                db.write(&WriteOptions { sync: true }, &rustkv::WriteBatch::new())?;
                assert_eq!(db.stats().durable_seq, range.1);
                cleanup_range = Some(range);
                range
            }
        };
        let durable_seq = db.stats().durable_seq;
        assert!(durable_seq >= cleanup_last_seq);
        let tail_key = format!("stage16-final-buffered-tail-{attempt}");
        db.put(
            &WriteOptions::default(),
            tail_key.as_bytes(),
            b"must-keep-final-descriptor",
        )?;
        let head_seq = db.stats().head_seq;
        assert_eq!(head_seq, durable_seq + 1);

        thread::sleep(overlap_window);
        assert_eq!(db.stats().instance_state, InstanceState::Healthy);
        drop(db);

        let (meta, mutations) = descriptor_inventory(root)?;
        assert!(
            meta.contains(&head_seq) && mutations.contains(&(head_seq, 0)),
            "cleanup crossed the captured durable frontier during a Drop attempt"
        );
        let remaining_meta = meta.range(cleanup_first_seq..=cleanup_last_seq).count();
        let remaining_mutations = mutations
            .range((cleanup_first_seq, 0)..=(cleanup_last_seq, u64::MAX))
            .count();
        let transaction_count = usize::try_from(cleanup_last_seq - cleanup_first_seq + 1)?;
        let remaining = remaining_meta + remaining_mutations;
        let complete = transaction_count
            .checked_mul(2)
            .ok_or("cleanup backlog count overflow")?;

        if (1..complete).contains(&remaining) {
            announce(&format!(
                "{FIXTURE_READY} {durable_seq} {head_seq} {cleanup_first_seq} {cleanup_last_seq}"
            ))?;
            announce(CLEANUP_DROP_VERIFIED)?;
            return Ok(());
        }
        if remaining == 0 {
            // This attempt observed a fully completed pass. Build a new range
            // on the next reopen instead of accepting an idle-worker Drop.
            cleanup_range = None;
            window_index = 0;
        } else if remaining == complete {
            window_index = (window_index + 1).min(CLEANUP_DROP_ATTEMPT_WINDOWS.len() - 1);
        } else if remaining != complete {
            return Err("cleanup backlog has an impossible Descriptor count".into());
        }
    }

    Err("could not observe Drop interrupting an active cleanup pass".into())
}

fn started_write_child(root: &Path) -> TestResult {
    let db = Db::open(&Options::default(), root)?;
    let batch = multivlog::stage16_started_write_batch()?;
    announce(WRITE_BEGIN)?;
    db.write(&WriteOptions::default(), &batch)?;
    announce(WRITE_FINISHED)?;
    drop(db);
    announce(WRITE_DROP_FINISHED)?;
    Ok(())
}

#[test]
fn stage16_heavy_child() -> TestResult {
    let Some(mode) = env::var_os(CHILD_MODE_ENV) else {
        return Ok(());
    };
    let root = PathBuf::from(env::var_os(CHILD_PATH_ENV).ok_or("missing heavy database path")?);
    match mode.to_str().ok_or("invalid heavy child mode")? {
        FIXTURE_MODE => fixture_child(&root),
        STARTED_WRITE_MODE => started_write_child(&root),
        _ => Err("unknown stage-16 heavy child mode".into()),
    }
}

fn spawn_child(mode: &str, root: &Path) -> io::Result<Child> {
    Command::new(env::current_exe()?)
        .args(["--exact", "stage16_heavy_child", "--nocapture"])
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_PATH_ENV, root)
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

fn wait_for_line(
    child: &mut Child,
    receiver: &mpsc::Receiver<String>,
    prefix: &str,
    timeout: Duration,
) -> TestResult<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            terminate_child(child)?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {prefix}"),
            )
            .into());
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) if line == prefix || line.starts_with(&format!("{prefix} ")) => {
                return Ok(line);
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                terminate_child(child)?;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for {prefix}"),
                )
                .into());
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = wait_for_exit(child, EXIT_TIMEOUT)?;
                return Err(
                    io::Error::other(format!("child exited as {status} before {prefix}")).into(),
                );
            }
        }
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
                "stage-16 heavy child did not exit",
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn terminate_child(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_none() {
        let _ = child.kill();
    }
    let _ = wait_for_exit(child, EXIT_TIMEOUT)?;
    Ok(())
}

fn parse_fixture_ready(line: &str) -> TestResult<(u64, u64, u64, u64)> {
    let mut fields = line.split_whitespace();
    if fields.next() != Some(FIXTURE_READY) {
        return Err("unexpected fixture marker".into());
    }
    let durable_seq = fields.next().ok_or("missing durable sequence")?.parse()?;
    let head_seq = fields.next().ok_or("missing head sequence")?.parse()?;
    let cleanup_first_seq = fields
        .next()
        .ok_or("missing cleanup first sequence")?
        .parse()?;
    let cleanup_last_seq = fields
        .next()
        .ok_or("missing cleanup last sequence")?
        .parse()?;
    if fields.next().is_some() {
        return Err("trailing fixture marker fields".into());
    }
    Ok((durable_seq, head_seq, cleanup_first_seq, cleanup_last_seq))
}

fn descriptor_inventory(root: &Path) -> TestResult<(BTreeSet<u64>, BTreeSet<(u64, u64)>)> {
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let transaction = database.keyspace(TRANSACTION_KEYSPACE, || {
        KeyspaceCreateOptions::default().manual_journal_persist(true)
    })?;
    let mut meta = BTreeSet::new();
    let mut mutations = BTreeSet::new();
    for guard in transaction.iter() {
        let (key, _) = guard.into_inner()?;
        let key = key.as_ref();
        if key.len() == 11 && key.get(0..2) == Some(b"TX") && key[10] == 0 {
            meta.insert(u64::from_be_bytes(key[2..10].try_into()?));
        } else if key.len() == 19 && key.get(0..2) == Some(b"TX") && key[10] == 1 {
            mutations.insert((
                u64::from_be_bytes(key[2..10].try_into()?),
                u64::from_be_bytes(key[11..19].try_into()?),
            ));
        } else {
            return Err("unexpected transaction metadata key".into());
        }
    }
    drop(transaction);
    drop(database);
    Ok((meta, mutations))
}

fn verify_worker_progress_and_clear_stable_descriptors(
    root: &Path,
    durable_seq: u64,
) -> TestResult {
    let database = Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let transaction = database.keyspace(TRANSACTION_KEYSPACE, || {
        KeyspaceCreateOptions::default().manual_journal_persist(true)
    })?;
    let mut stable_keys = Vec::new();
    let mut early_meta_present = false;
    let mut unstable_meta_present = false;
    let unstable_seq = durable_seq
        .checked_add(1)
        .ok_or("stable Descriptor boundary overflow")?;

    for guard in transaction.iter() {
        let (key, _) = guard.into_inner()?;
        let key = key.as_ref();
        let seq = descriptor_sequence(key)?;
        if key.len() == 11 && seq == 1 {
            early_meta_present = true;
        }
        if key.len() == 11 && seq == unstable_seq {
            unstable_meta_present = true;
        }
        if seq <= durable_seq {
            stable_keys.push(key.to_vec());
        }
    }
    assert!(
        !early_meta_present,
        "the real cleanup worker made no progress during the 10 GiB write"
    );
    assert!(
        unstable_meta_present,
        "the buffered tail Descriptor disappeared across normal Drop"
    );

    for keys in stable_keys.chunks(4_096) {
        let mut batch = database.batch();
        for key in keys {
            batch.remove(&transaction, key);
        }
        batch.commit()?;
    }
    database.persist(PersistMode::SyncAll)?;
    transaction.rotate_memtable_and_wait()?;
    transaction.major_compact()?;

    for guard in transaction.iter() {
        let (key, _) = guard.into_inner()?;
        assert!(
            descriptor_sequence(key.as_ref())? > durable_seq,
            "stable Descriptor remained after test baseline cleanup"
        );
    }
    drop(transaction);
    drop(database);
    Ok(())
}

fn descriptor_sequence(key: &[u8]) -> TestResult<u64> {
    if key.get(0..2) != Some(b"TX")
        || !matches!((key.len(), key.get(10)), (11, Some(0)) | (19, Some(1)))
    {
        return Err("unexpected transaction metadata key".into());
    }
    Ok(u64::from_be_bytes(key[2..10].try_into()?))
}

fn total_vlog_bytes(root: &Path) -> io::Result<u64> {
    fs::read_dir(root.join("vlog"))?.try_fold(0_u64, |total, entry| {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('D') && name.ends_with(".data") {
            total
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| io::Error::other("VLog byte count overflow"))
        } else {
            Ok(total)
        }
    })
}

fn vlog_file_ids(root: &Path) -> io::Result<Vec<u32>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(root.join("vlog"))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(digits) = name
            .strip_prefix('D')
            .and_then(|name| name.strip_suffix(".data"))
        else {
            continue;
        };
        ids.push(digits.parse().map_err(io::Error::other)?);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn wait_for_vlog_growth(child: &mut Child, root: &Path, baseline: u64) -> TestResult {
    let deadline = Instant::now() + WRITE_TIMEOUT;
    loop {
        if total_vlog_bytes(root)? > baseline {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "writer child exited as {status} before physical VLog growth"
            ))
            .into());
        }
        if Instant::now() >= deadline {
            terminate_child(child)?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for a started physical write",
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_write_still_started(receiver: &mpsc::Receiver<String>) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) if line == WRITE_FINISHED => {
                return Err(io::Error::other(
                    "physical growth was observed only after the public write had returned",
                )
                .into());
            }
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => return Ok(()),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(
                    "writer stdout closed while checking the stopped write",
                )
                .into());
            }
        }
    }
}

fn signal_child(child: &Child, signal: i32) -> io::Result<()> {
    let pid = i32::try_from(child.id()).map_err(io::Error::other)?;
    // SAFETY: `pid` belongs to the live test child and the signal constants are
    // the platform SIGSTOP/SIGCONT values. No memory is dereferenced.
    if unsafe { kill(pid, signal) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[test]
#[ignore = "writes at least 10 GiB and exercises real background cleanup and Destroy"]
fn ten_gib_cleanup_lifecycle_and_destroy() -> TestResult {
    if env::var_os(CHILD_MODE_ENV).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let unmanaged = root.join("stage16-unmanaged.txt");

    let mut fixture = spawn_child(FIXTURE_MODE, &root)?;
    let fixture_lines = start_line_reader(&mut fixture)?;
    let ready = wait_for_line(&mut fixture, &fixture_lines, FIXTURE_READY, FIXTURE_TIMEOUT)?;
    let (durable_seq, head_seq, cleanup_first_seq, cleanup_last_seq) = parse_fixture_ready(&ready)?;
    assert_eq!(
        head_seq,
        durable_seq.checked_add(1).ok_or("sequence overflow")?
    );
    let _ = wait_for_line(
        &mut fixture,
        &fixture_lines,
        CLEANUP_DROP_VERIFIED,
        DROP_TIMEOUT,
    )?;
    assert!(wait_for_exit(&mut fixture, EXIT_TIMEOUT)?.success());

    let (meta, mutations) = descriptor_inventory(&root)?;
    let remaining_cleanup_meta = meta.range(cleanup_first_seq..=cleanup_last_seq).count();
    let remaining_cleanup_mutations = mutations
        .range((cleanup_first_seq, 0)..=(cleanup_last_seq, u64::MAX))
        .count();
    let cleanup_transaction_count = usize::try_from(cleanup_last_seq - cleanup_first_seq + 1)?;
    let remaining_cleanup_entries = remaining_cleanup_meta + remaining_cleanup_mutations;
    assert!(
        remaining_cleanup_entries > 0,
        "cleanup finished before Drop instead of being joined while active"
    );
    assert!(
        remaining_cleanup_entries < cleanup_transaction_count * 2,
        "cleanup had not deleted any backlog entry before Drop"
    );
    assert!(
        remaining_cleanup_meta > 0,
        "cleanup unexpectedly removed every backlog Meta before Drop"
    );
    assert!(durable_seq > 1);
    assert!(
        meta.contains(&head_seq),
        "cleanup crossed its captured durable frontier and deleted the buffered tail Meta"
    );
    assert!(
        mutations.contains(&(head_seq, 0)),
        "cleanup crossed its captured durable frontier and deleted the buffered tail Mutation"
    );

    let files = vlog_file_ids(&root)?;
    assert!(files.len() >= 3);
    assert_eq!(files[0], 0);
    assert_eq!(files[1], 1);
    assert!(files.last().is_some_and(|file_id| *file_id >= 2));
    fs::write(&unmanaged, b"keep")?;

    let baseline = total_vlog_bytes(&root)?;
    let mut writer = spawn_child(STARTED_WRITE_MODE, &root)?;
    let writer_lines = start_line_reader(&mut writer)?;
    let _ = wait_for_line(&mut writer, &writer_lines, WRITE_BEGIN, WRITE_TIMEOUT)?;
    wait_for_vlog_growth(&mut writer, &root, baseline)?;
    signal_child(&writer, SIGSTOP)?;
    if let Err(error) = assert_write_still_started(&writer_lines) {
        terminate_child(&mut writer)?;
        return Err(error);
    }

    let destroy_while_started = Db::destroy(&root, &Options::default());
    if let Err(error) = signal_child(&writer, SIGCONT) {
        terminate_child(&mut writer)?;
        return Err(error.into());
    }
    let busy = destroy_while_started
        .expect_err("Destroy acquired LOCK while a started public write was paused");
    assert_eq!(busy.kind, StorageErrorKind::Busy);
    let context = busy.destroy_failure.expect("Destroy Busy context");
    assert!(matches!(context.failed_object, ManagedObject::Lock));
    assert!(matches!(context.stage, DestroyStage::AcquireLock));
    assert!(!context.partially_deleted);

    let _ = wait_for_line(&mut writer, &writer_lines, WRITE_FINISHED, WRITE_TIMEOUT)?;
    let _ = wait_for_line(
        &mut writer,
        &writer_lines,
        WRITE_DROP_FINISHED,
        DROP_TIMEOUT,
    )?;
    assert!(wait_for_exit(&mut writer, EXIT_TIMEOUT)?.success());

    Db::destroy(&root, &Options::default())?;
    assert!(root.is_dir());
    assert!(root.join("LOCK").is_file());
    assert_eq!(fs::read(&unmanaged)?, b"keep");
    assert!(!root.join("FORMAT").exists());
    assert!(!root.join("FORMAT.tmp").exists());
    assert!(!root.join("index").exists());
    assert!(!root.join("vlog").exists());
    Ok(())
}
