use std::env;
use std::io;
use std::process::{Child, Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use rustkv::{Db, Options, ReadOptions, WriteBatch, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

const CHILD_MODE: &str = "RUSTKV_CORE_RESTART_CHILD";
const CHILD_PATH: &str = "RUSTKV_CORE_RESTART_PATH";
const MANY_WRITES_CHILD_MODE: &str = "RUSTKV_MANY_WRITES_RESTART_CHILD";
const MANY_PUTS_MODE: &str = "many-puts";
const MANY_DELETES_MODE: &str = "many-deletes";
const CHILD_TIMEOUT: Duration = Duration::from_secs(15);
const MANY_WRITES_TIMEOUT: Duration = Duration::from_secs(30);
const MANY_WRITE_COUNT: u32 = 2_000;

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn read(db: &Db, key: &[u8]) -> rustkv::Result<Option<Vec<u8>>> {
    db.get(&ReadOptions::default(), key)
}

fn pressure_options(create_if_missing: bool) -> Options {
    Options {
        create_if_missing,
        write_buffer_size: 64 * 1024,
        ..Options::default()
    }
}

fn numbered_key(number: u32) -> [u8; 8] {
    let mut key = *b"key-0000";
    key[4..8].copy_from_slice(&number.to_be_bytes());
    key
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let kill_deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(status) = child.try_wait()? {
                    return Ok(status);
                }
                if Instant::now() >= kill_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "restart child did not exit after kill",
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn normal_drop_reopen_recovers_buffered_and_durable_writes() -> TestResult {
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    {
        let db = Db::open(&create_options(), &root)?;
        db.put(&WriteOptions::default(), b"buffered", b"one")?;
        db.put(&WriteOptions { sync: true }, b"durable", b"two")?;
        db.put(&WriteOptions::default(), b"deleted", b"old")?;
        db.delete(&WriteOptions::default(), b"deleted")?;
        db.put(&WriteOptions::default(), b"rewrite", b"before")?;
        db.delete(&WriteOptions::default(), b"rewrite")?;
        db.put(&WriteOptions::default(), b"rewrite", b"after")?;
    }

    let reopened = Db::open(&Options::default(), &root)?;
    assert_eq!(read(&reopened, b"buffered")?, Some(b"one".to_vec()));
    assert_eq!(read(&reopened, b"durable")?, Some(b"two".to_vec()));
    assert_eq!(read(&reopened, b"deleted")?, None);
    assert_eq!(read(&reopened, b"rewrite")?, Some(b"after".to_vec()));
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, 7);
    assert_eq!(stats.durable_seq, 7);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}

#[test]
fn no_drop_child() -> TestResult {
    if env::var_os(CHILD_MODE).is_none() {
        return Ok(());
    }
    let root = env::var_os(CHILD_PATH).ok_or("missing child database path")?;
    let db = Db::open(&create_options(), root)?;
    db.put(&WriteOptions::default(), b"prefix-a", b"a")?;
    db.put(&WriteOptions::default(), b"prefix-b", b"b")?;
    db.put(&WriteOptions { sync: true }, b"sync-tail", b"c")?;

    let mut batch = WriteBatch::new();
    batch.put(b"batch-a", b"one")?;
    batch.put(b"batch-b", b"two")?;
    batch.delete(b"batch-a")?;
    batch.put(b"batch-a", b"final")?;
    db.write(&WriteOptions::default(), &batch)?;
    std::process::exit(0);
}

#[test]
fn process_exit_without_drop_reopens_to_a_complete_prefix() -> TestResult {
    if env::var_os(CHILD_MODE).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let mut child = Command::new(env::current_exe()?)
        .arg("--exact")
        .arg("no_drop_child")
        .arg("--nocapture")
        .env(CHILD_MODE, "1")
        .env(CHILD_PATH, &root)
        .spawn()?;
    let status = wait_for_exit(&mut child, CHILD_TIMEOUT)?;
    assert!(status.success());

    let reopened = Db::open(&Options::default(), &root)?;
    assert_eq!(read(&reopened, b"prefix-a")?, Some(b"a".to_vec()));
    assert_eq!(read(&reopened, b"prefix-b")?, Some(b"b".to_vec()));
    assert_eq!(read(&reopened, b"sync-tail")?, Some(b"c".to_vec()));
    assert_eq!(read(&reopened, b"batch-a")?, Some(b"final".to_vec()));
    assert_eq!(read(&reopened, b"batch-b")?, Some(b"two".to_vec()));
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, 4);
    assert_eq!(stats.durable_seq, 4);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}

#[test]
fn many_sync_false_writes_restart_child() -> TestResult {
    let Some(mode) = env::var_os(MANY_WRITES_CHILD_MODE) else {
        return Ok(());
    };
    let root = env::var_os(CHILD_PATH).ok_or("missing child database path")?;
    match mode.to_str().ok_or("invalid many-writes child mode")? {
        MANY_PUTS_MODE => {
            {
                let db = Db::open(&pressure_options(true), &root)?;
                for number in 0..MANY_WRITE_COUNT {
                    db.put(
                        &WriteOptions::default(),
                        &numbered_key(number),
                        &vec![u8::try_from(number % 251)?; 256],
                    )?;
                }
            }
            let reopened = Db::open(&pressure_options(false), &root)?;
            for number in 0..MANY_WRITE_COUNT {
                assert_eq!(
                    read(&reopened, &numbered_key(number))?,
                    Some(vec![u8::try_from(number % 251)?; 256])
                );
            }
            let stats = reopened.stats();
            assert_eq!(stats.head_seq, u64::from(MANY_WRITE_COUNT));
            assert_eq!(stats.durable_seq, u64::from(MANY_WRITE_COUNT));
            assert_eq!(stats.durability_lag, 0);
        }
        MANY_DELETES_MODE => {
            {
                let db = Db::open(&pressure_options(true), &root)?;
                let mut seed = WriteBatch::new();
                for number in 0..MANY_WRITE_COUNT {
                    seed.put(&numbered_key(number), b"seed")?;
                }
                db.write(&WriteOptions { sync: true }, &seed)?;
                for number in 0..MANY_WRITE_COUNT {
                    db.delete(&WriteOptions::default(), &numbered_key(number))?;
                }
            }
            let reopened = Db::open(&pressure_options(false), &root)?;
            for number in 0..MANY_WRITE_COUNT {
                assert_eq!(read(&reopened, &numbered_key(number))?, None);
            }
            let stats = reopened.stats();
            assert_eq!(stats.head_seq, u64::from(MANY_WRITE_COUNT) + 1);
            assert_eq!(stats.durable_seq, u64::from(MANY_WRITE_COUNT) + 1);
            assert_eq!(stats.durability_lag, 0);
        }
        _ => return Err("unknown many-writes child mode".into()),
    }
    Ok(())
}

#[test]
fn many_sync_false_puts_and_deletes_reopen_with_the_expected_state() -> TestResult {
    if env::var_os(MANY_WRITES_CHILD_MODE).is_some() {
        return Ok(());
    }
    let folder = TempDir::new()?;
    for mode in [MANY_PUTS_MODE, MANY_DELETES_MODE] {
        let root = folder.path().join(mode);
        let mut child = Command::new(env::current_exe()?)
            .arg("--exact")
            .arg("many_sync_false_writes_restart_child")
            .arg("--nocapture")
            .env(MANY_WRITES_CHILD_MODE, mode)
            .env(CHILD_PATH, &root)
            .spawn()?;
        let status = wait_for_exit(&mut child, MANY_WRITES_TIMEOUT)?;
        assert!(
            status.success(),
            "{mode} recovery child failed or exceeded its timeout: {status}"
        );
    }
    Ok(())
}
