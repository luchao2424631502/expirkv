//! Explicitly enabled, end-to-end coverage for a database larger than two
//! production VLog files.
//!
//! Run with:
//! `cargo test --test core_multivlog_10g -- --ignored --exact ten_gib_multivlog_public_api_restart_and_crash_matrix --nocapture`
//!
//! The test writes at least 10 GiB of key/value payload with fixed 32-byte keys
//! and deterministically pseudo-random value lengths, so it is intentionally
//! ignored by the ordinary test suite. It also writes a compact generation
//! oracle (`expected-pairs.bin`) next to the database and regenerates every
//! complete value while comparing all live pairs after each restart or crash.
//! Precise VLog/Fjall/Recovery process-stop points live in `core_crash`; the
//! SIGKILL cases here are additional cross-file, large-database stress only.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rustkv::{Db, Options, ReadOptions, WriteBatch, WriteOptions};
use tempfile::TempDir;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Oracle = BTreeMap<Vec<u8>, ExpectedValue>;

#[derive(Debug)]
enum ExpectedValue {
    Raw(Vec<u8>),
    Generated { tag: u64, slot: usize, len: usize },
}

impl ExpectedValue {
    fn materialize(&self) -> Vec<u8> {
        match self {
            Self::Raw(value) => value.clone(),
            Self::Generated { tag, slot, len } => generated_value(*tag, *slot, *len),
        }
    }
}

const TEN_GIB: u64 = 10 * 1024 * 1024 * 1024;
const PRODUCTION_VLOG_FILE_BYTES: u64 = 1_u64 << 32;
const KEY_LEN: usize = 32;
const MAX_KEY_VALUE_BYTES: usize = 60_000;
const MAX_RANDOM_VALUE_LEN: usize = MAX_KEY_VALUE_BYTES - KEY_LEN;
const CRASH_VALUE_LEN: usize = 59_000;
const FILLER_KEYS_PER_BATCH: usize = 64;
const CRASH_COMPLETE_KEYS: usize = 32;
const CRASH_CANDIDATE_KEYS: usize = 512;
const ORACLE_MAGIC: &[u8; 8] = b"RKVORCL2";
const ORACLE_RAW: u8 = 0;
const ORACLE_GENERATED: u8 = 1;

const CHILD_MODE: &str = "RUSTKV_CORE_MULTIVLOG_CHILD";
const CHILD_PATH: &str = "RUSTKV_CORE_MULTIVLOG_PATH";
const NO_DROP_MODE: &str = "no-drop";
const CRASH_WRITER_MODE: &str = "crash-writer";
const PREPARE_RECOVERY_MODE: &str = "prepare-recovery";
const OPEN_RECOVERY_MODE: &str = "open-recovery";

const CRASH_COMPLETE_TAG: u64 = u64::MAX - 2;
const CRASH_CANDIDATE_TAG: u64 = u64::MAX - 1;
const RECOVERY_TAG: u64 = u64::MAX;
const CHILD_TIMEOUT: Duration = Duration::from_secs(120);
const CHILD_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn read(db: &Db, key: &[u8]) -> rustkv::Result<Option<Vec<u8>>> {
    db.get(&ReadOptions::default(), key)
}

fn key_32(label: impl AsRef<[u8]>) -> [u8; KEY_LEN] {
    let label = label.as_ref();
    assert!(!label.is_empty());
    assert!(label.len() <= KEY_LEN);
    let mut key = [0_u8; KEY_LEN];
    key[..label.len()].copy_from_slice(label);
    key
}

fn generated_value(tag: u64, slot: usize, len: usize) -> Vec<u8> {
    assert!((1..=MAX_RANDOM_VALUE_LEN).contains(&len));
    let slot = u64::try_from(slot).expect("test slot fits u64");
    let fill = tag
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(slot)
        .to_le_bytes()[0];
    let mut value = vec![fill; len];
    let identity = [
        tag.to_le_bytes(),
        slot.to_le_bytes(),
        (!tag ^ slot).to_le_bytes(),
    ]
    .concat();
    for (destination, source) in value.iter_mut().zip(identity) {
        *destination = source;
    }
    value
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn random_value_len(round: u64, slot: usize) -> usize {
    let sample = mix64(round ^ u64::try_from(slot).expect("test slot fits u64"));
    1 + usize::try_from(sample % MAX_RANDOM_VALUE_LEN as u64)
        .expect("bounded value length fits usize")
}

fn filler_key(round: u64, slot: usize) -> [u8; KEY_LEN] {
    key_32(format!("fill-{round:012}-{slot:03}"))
}

fn crash_complete_key(slot: usize) -> [u8; KEY_LEN] {
    key_32(format!("crash-complete-{slot:03}"))
}

fn crash_candidate_key(slot: usize) -> [u8; KEY_LEN] {
    key_32(format!("crash-candidate-{slot:03}"))
}

fn recovery_key(slot: usize) -> [u8; KEY_LEN] {
    key_32(format!("recovery-{slot:02}"))
}

fn apply_mixed_batch(db: &Db, oracle: &mut Oracle, file_id: u32) -> TestResult {
    let mixed_key = key_32("mixed-order-key");
    let mixed_empty_key = key_32("mixed-empty-value");
    let mixed_missing_key = key_32("mixed-never-existed");
    let final_value = format!("final-in-file-{file_id}").into_bytes();
    let mut batch = WriteBatch::new();
    batch.put(mixed_key, b"one")?;
    batch.put(mixed_key, b"two")?;
    batch.delete(mixed_key)?;
    batch.put(mixed_key, &final_value)?;
    batch.delete(mixed_missing_key)?;
    batch.put(mixed_empty_key, b"")?;
    db.write(&WriteOptions::default(), &batch)?;

    oracle.insert(mixed_key.to_vec(), ExpectedValue::Raw(final_value));
    oracle.remove(mixed_missing_key.as_slice());
    oracle.insert(mixed_empty_key.to_vec(), ExpectedValue::Raw(Vec::new()));
    assert_eq!(
        read(db, &mixed_key)?,
        oracle
            .get(mixed_key.as_slice())
            .map(ExpectedValue::materialize)
    );
    assert_eq!(read(db, &mixed_missing_key)?, None);
    assert_eq!(read(db, &mixed_empty_key)?, Some(Vec::new()));
    Ok(())
}

fn exercise_initial_crud(db: &Db, oracle: &mut Oracle) -> TestResult {
    let write = WriteOptions::default();
    let missing_key = key_32("missing");
    let crud_key = key_32("crud-cross-file");
    let stable_key = key_32("stable-prefix");
    assert_eq!(read(db, &missing_key)?, None);
    db.delete(&write, &missing_key)?;
    assert_eq!(read(db, &missing_key)?, None);

    db.put(&write, &crud_key, b"first")?;
    assert_eq!(read(db, &crud_key)?, Some(b"first".to_vec()));
    db.put(&write, &crud_key, b"second")?;
    assert_eq!(read(db, &crud_key)?, Some(b"second".to_vec()));
    db.delete(&write, &crud_key)?;
    assert_eq!(read(db, &crud_key)?, None);
    db.put(&write, &crud_key, b"reborn-in-file-0")?;
    oracle.insert(
        crud_key.to_vec(),
        ExpectedValue::Raw(b"reborn-in-file-0".to_vec()),
    );

    db.put(&WriteOptions { sync: true }, &stable_key, b"stable")?;
    oracle.insert(stable_key.to_vec(), ExpectedValue::Raw(b"stable".to_vec()));
    apply_mixed_batch(db, oracle, 0)
}

fn write_filler_batch(db: &Db, oracle: &mut Oracle, round: u64) -> TestResult<(u64, usize, usize)> {
    let mut batch = WriteBatch::new();
    let mut updates = Vec::with_capacity(FILLER_KEYS_PER_BATCH);
    let mut payload_bytes = 0_u64;
    let mut minimum_len = usize::MAX;
    let mut maximum_len = 0_usize;
    for slot in 0..FILLER_KEYS_PER_BATCH {
        let key = filler_key(round, slot);
        let value_len = random_value_len(round, slot);
        let value = generated_value(round, slot, value_len);
        batch.put(&key, &value)?;
        payload_bytes = payload_bytes
            .checked_add(u64::try_from(key.len() + value.len())?)
            .ok_or("payload byte count overflow")?;
        minimum_len = minimum_len.min(value_len);
        maximum_len = maximum_len.max(value_len);
        updates.push((
            key.to_vec(),
            ExpectedValue::Generated {
                tag: round,
                slot,
                len: value_len,
            },
        ));
    }
    db.write(&WriteOptions::default(), &batch)?;
    for (key, value) in updates {
        oracle.insert(key, value);
    }
    Ok((payload_bytes, minimum_len, maximum_len))
}

fn record_rollover_semantics(db: &Db, oracle: &mut Oracle, new_file_id: u32) -> TestResult {
    let anchor_key = key_32(format!("anchor-file-{new_file_id}"));
    let anchor_value = format!("value-in-file-{new_file_id}").into_bytes();
    db.put(&WriteOptions::default(), &anchor_key, &anchor_value)?;
    oracle.insert(anchor_key.to_vec(), ExpectedValue::Raw(anchor_value));

    let crud_key = key_32("crud-cross-file");
    let cross_value = format!("crud-overwrite-in-file-{new_file_id}").into_bytes();
    if new_file_id >= 2 {
        db.delete(&WriteOptions::default(), &crud_key)?;
        assert_eq!(read(db, &crud_key)?, None);
        oracle.remove(crud_key.as_slice());
    }
    db.put(&WriteOptions::default(), &crud_key, &cross_value)?;
    oracle.insert(crud_key.to_vec(), ExpectedValue::Raw(cross_value));
    apply_mixed_batch(db, oracle, new_file_id)
}

fn fill_through_ten_gib(db: &Db, oracle: &mut Oracle) -> TestResult {
    let mut round = 0_u64;
    let mut file_count = db.stats().vlog_file_count;
    let mut rollovers = Vec::new();
    let mut payload_bytes = 0_u64;
    let mut minimum_value_len = usize::MAX;
    let mut maximum_value_len = 0_usize;

    while payload_bytes < TEN_GIB {
        round = round.checked_add(1).ok_or("filler round overflow")?;
        let (batch_payload, batch_minimum, batch_maximum) = write_filler_batch(db, oracle, round)?;
        payload_bytes = payload_bytes
            .checked_add(batch_payload)
            .ok_or("payload byte count overflow")?;
        minimum_value_len = minimum_value_len.min(batch_minimum);
        maximum_value_len = maximum_value_len.max(batch_maximum);
        let stats = db.stats();

        if stats.vlog_file_count > file_count {
            assert_eq!(
                stats.vlog_file_count,
                file_count + 1,
                "one filler transaction must not skip a 4 GiB VLog file"
            );
            let new_file_id = stats.vlog_file_count - 1;
            rollovers.push((file_count - 1, new_file_id));

            // Every filler key is unique. The transaction that crossed this
            // boundary therefore retains pointers on both sides for all later
            // full-oracle verification passes.
            file_count = stats.vlog_file_count;
            record_rollover_semantics(db, oracle, new_file_id)?;
            println!(
                "ROLLED file {} -> {} at {} logical bytes",
                new_file_id - 1,
                new_file_id,
                stats.vlog_logical_bytes
            );
        }

        if round.is_multiple_of(128) {
            println!(
                "FILL {:.2} GiB, files={}, round={round}",
                payload_bytes as f64 / (1024_f64 * 1024_f64 * 1024_f64),
                stats.vlog_file_count
            );
        }
    }

    assert!(rollovers.contains(&(0, 1)), "file 0 never rolled to file 1");
    assert!(rollovers.contains(&(1, 2)), "file 1 never rolled to file 2");
    let stats = db.stats();
    assert!(payload_bytes >= TEN_GIB);
    assert!((1..=MAX_RANDOM_VALUE_LEN).contains(&minimum_value_len));
    assert!((1..=MAX_RANDOM_VALUE_LEN).contains(&maximum_value_len));
    assert!(minimum_value_len < maximum_value_len);
    assert!(stats.vlog_logical_bytes >= TEN_GIB);
    assert!(stats.vlog_file_count >= 3);
    assert!(
        stats
            .active_vlog_file_id
            .is_some_and(|file_id| file_id >= 2)
    );
    Ok(())
}

fn write_oracle(path: &Path, oracle: &Oracle) -> io::Result<()> {
    let mut output = BufWriter::new(File::create(path)?);
    output.write_all(ORACLE_MAGIC)?;
    output.write_all(
        &u64::try_from(oracle.len())
            .map_err(io::Error::other)?
            .to_le_bytes(),
    )?;
    for (key, expected) in oracle {
        if key.len() != KEY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oracle key is not exactly 32 bytes",
            ));
        }
        output.write_all(key)?;
        match expected {
            ExpectedValue::Raw(value) => {
                output.write_all(&[ORACLE_RAW])?;
                output.write_all(
                    &u32::try_from(value.len())
                        .map_err(io::Error::other)?
                        .to_le_bytes(),
                )?;
                output.write_all(value)?;
            }
            ExpectedValue::Generated { tag, slot, len } => {
                output.write_all(&[ORACLE_GENERATED])?;
                output.write_all(&tag.to_le_bytes())?;
                output.write_all(
                    &u64::try_from(*slot)
                        .map_err(io::Error::other)?
                        .to_le_bytes(),
                )?;
                output.write_all(&u32::try_from(*len).map_err(io::Error::other)?.to_le_bytes())?;
            }
        }
    }
    output.flush()?;
    output.get_ref().sync_all()
}

fn read_u32(input: &mut impl Read) -> io::Result<u32> {
    let mut encoded = [0_u8; 4];
    input.read_exact(&mut encoded)?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(input: &mut impl Read) -> io::Result<u64> {
    let mut encoded = [0_u8; 8];
    input.read_exact(&mut encoded)?;
    Ok(u64::from_le_bytes(encoded))
}

fn read_oracle(path: &Path) -> io::Result<Oracle> {
    let mut input = BufReader::new(File::open(path)?);
    let mut magic = [0_u8; 8];
    input.read_exact(&mut magic)?;
    if &magic != ORACLE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid oracle magic",
        ));
    }
    let mut encoded_count = [0_u8; 8];
    input.read_exact(&mut encoded_count)?;
    let count = usize::try_from(u64::from_le_bytes(encoded_count)).map_err(io::Error::other)?;
    let mut oracle = Oracle::new();
    for _ in 0..count {
        let mut key = vec![0_u8; KEY_LEN];
        input.read_exact(&mut key)?;
        let mut kind = [0_u8; 1];
        input.read_exact(&mut kind)?;
        let expected = match kind[0] {
            ORACLE_RAW => {
                let len = usize::try_from(read_u32(&mut input)?).map_err(io::Error::other)?;
                if len > MAX_RANDOM_VALUE_LEN {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "raw oracle value is too large",
                    ));
                }
                let mut value = vec![0_u8; len];
                input.read_exact(&mut value)?;
                ExpectedValue::Raw(value)
            }
            ORACLE_GENERATED => {
                let tag = read_u64(&mut input)?;
                let slot = usize::try_from(read_u64(&mut input)?).map_err(io::Error::other)?;
                let len = usize::try_from(read_u32(&mut input)?).map_err(io::Error::other)?;
                if !(1..=MAX_RANDOM_VALUE_LEN).contains(&len) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "generated oracle value length is invalid",
                    ));
                }
                ExpectedValue::Generated { tag, slot, len }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown oracle value kind",
                ));
            }
        };
        if oracle.insert(key, expected).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate oracle key",
            ));
        }
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing oracle bytes",
        ));
    }
    Ok(oracle)
}

fn verify_oracle(db: &Db, oracle_path: &Path) -> TestResult {
    let oracle = read_oracle(oracle_path)?;
    for (key, expected) in &oracle {
        assert_eq!(
            read(db, key)?,
            Some(expected.materialize()),
            "oracle mismatch for key {}",
            String::from_utf8_lossy(key)
        );
    }
    assert_eq!(read(db, &key_32("mixed-never-existed"))?, None);
    println!("VERIFIED {} live key/value pairs", oracle.len());
    Ok(())
}

fn assert_ten_gib_vlog_layout(root: &Path, db: &Db) -> TestResult {
    let vlog = root.join("vlog");
    let mut data_files = Vec::new();
    for entry in fs::read_dir(vlog)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('D') && name.ends_with(".data") {
            data_files.push((name.into_owned(), entry.metadata()?.len()));
        }
    }
    data_files.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    assert!(data_files.len() >= 3, "expected at least three VLog files");
    assert_eq!(
        data_files[0],
        ("D000000.data".to_owned(), PRODUCTION_VLOG_FILE_BYTES)
    );
    assert_eq!(
        data_files[1],
        ("D000001.data".to_owned(), PRODUCTION_VLOG_FILE_BYTES)
    );
    let physical_bytes = data_files.iter().try_fold(0_u64, |total, (_, len)| {
        total.checked_add(*len).ok_or("VLog byte count overflow")
    })?;
    assert!(physical_bytes >= TEN_GIB);

    let stats = db.stats();
    assert!(stats.vlog_logical_bytes >= TEN_GIB);
    assert_eq!(
        usize::try_from(stats.vlog_file_count)?,
        data_files.len(),
        "stats and directory inventory disagree"
    );
    Ok(())
}

fn spawn_child(mode: &str, root: &Path) -> io::Result<std::process::Child> {
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

fn wait_for_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let kill_deadline = Instant::now() + CHILD_EXIT_TIMEOUT;
            loop {
                if let Some(status) = child.try_wait()? {
                    return Ok(status);
                }
                if Instant::now() >= kill_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "heavy-test child did not exit after kill",
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn kill_and_wait(child: &mut Child) -> io::Result<ExitStatus> {
    child.kill()?;
    wait_for_exit(child, CHILD_EXIT_TIMEOUT)
}

fn wait_for_marker(
    child: &mut Child,
    receiver: &mpsc::Receiver<String>,
    marker: &str,
) -> TestResult<Vec<String>> {
    let deadline = Instant::now() + CHILD_TIMEOUT;
    let mut observed = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let _ = kill_and_wait(child);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {marker}"),
            )
            .into());
        }
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let reached = line == marker;
                observed.push(line);
                if reached {
                    return Ok(observed);
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = kill_and_wait(child);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for {marker}"),
                )
                .into());
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let status = wait_for_exit(child, CHILD_EXIT_TIMEOUT)?;
                return Err(
                    io::Error::other(format!("child exited as {status} before {marker}")).into(),
                );
            }
        }
    }
}

fn no_drop_child(root: &Path) -> TestResult {
    let db = Db::open(&Options::default(), root)?;
    let restart_a = key_32("restart-a");
    let restart_b = key_32("restart-b");
    let restart_sync_tail = key_32("restart-sync-tail");
    let restart_batch_a = key_32("restart-batch-a");
    let restart_batch_b = key_32("restart-batch-b");
    db.put(&WriteOptions::default(), &restart_a, b"a")?;
    db.put(&WriteOptions::default(), &restart_b, b"b")?;
    db.put(&WriteOptions { sync: true }, &restart_sync_tail, b"c")?;

    let mut batch = WriteBatch::new();
    batch.put(restart_batch_a, b"one")?;
    batch.put(restart_batch_b, b"two")?;
    batch.delete(restart_batch_a)?;
    batch.put(restart_batch_a, b"final")?;
    batch.delete(restart_b)?;
    db.write(&WriteOptions::default(), &batch)?;
    std::process::exit(0);
}

fn crash_writer_child(root: &Path) -> TestResult {
    let db = Db::open(&Options::default(), root)?;

    let mut completed = WriteBatch::new();
    for slot in 0..CRASH_COMPLETE_KEYS {
        completed.put(
            crash_complete_key(slot),
            generated_value(CRASH_COMPLETE_TAG, slot, 40_000),
        )?;
    }
    db.write(&WriteOptions::default(), &completed)?;
    println!("DONE complete");
    io::stdout().flush()?;

    let mut candidate = WriteBatch::new();
    for slot in 0..CRASH_CANDIDATE_KEYS {
        candidate.put(
            crash_candidate_key(slot),
            generated_value(CRASH_CANDIDATE_TAG, slot, CRASH_VALUE_LEN),
        )?;
    }
    println!("BEGIN candidate");
    io::stdout().flush()?;
    db.write(&WriteOptions::default(), &candidate)?;
    println!("COMMITTED candidate");
    io::stdout().flush()?;
    thread::sleep(Duration::from_secs(30));
    Ok(())
}

fn prepare_recovery_child(root: &Path) -> TestResult {
    let db = Db::open(&Options::default(), root)?;
    for slot in 0..12 {
        let key = recovery_key(slot);
        let value = generated_value(RECOVERY_TAG, slot, 40_000);
        db.put(&WriteOptions::default(), &key, &value)?;
    }
    std::process::exit(0);
}

fn open_recovery_child(root: &Path) -> TestResult {
    println!("OPENING");
    io::stdout().flush()?;
    let _db = Db::open(&Options::default(), root)?;
    println!("OPENED");
    io::stdout().flush()?;
    thread::sleep(Duration::from_secs(30));
    Ok(())
}

#[test]
fn process_child() -> TestResult {
    let Some(mode) = env::var_os(CHILD_MODE) else {
        return Ok(());
    };
    let root = PathBuf::from(env::var_os(CHILD_PATH).ok_or("missing child database path")?);
    match mode.to_str().ok_or("invalid child mode")? {
        NO_DROP_MODE => no_drop_child(&root),
        CRASH_WRITER_MODE => crash_writer_child(&root),
        PREPARE_RECOVERY_MODE => prepare_recovery_child(&root),
        OPEN_RECOVERY_MODE => open_recovery_child(&root),
        _ => Err("unknown child mode".into()),
    }
}

fn apply_no_drop_expected(oracle: &mut Oracle) {
    oracle.insert(
        key_32("restart-a").to_vec(),
        ExpectedValue::Raw(b"a".to_vec()),
    );
    oracle.remove(key_32("restart-b").as_slice());
    oracle.insert(
        key_32("restart-sync-tail").to_vec(),
        ExpectedValue::Raw(b"c".to_vec()),
    );
    oracle.insert(
        key_32("restart-batch-a").to_vec(),
        ExpectedValue::Raw(b"final".to_vec()),
    );
    oracle.insert(
        key_32("restart-batch-b").to_vec(),
        ExpectedValue::Raw(b"two".to_vec()),
    );
}

#[test]
#[ignore = "writes and rereads at least 10 GiB; run explicitly with --ignored"]
fn ten_gib_multivlog_public_api_restart_and_crash_matrix() -> TestResult {
    if env::var_os(CHILD_MODE).is_some() {
        return Ok(());
    }

    let folder = TempDir::new()?;
    let root = folder.path().join("db");
    let oracle_path = folder.path().join("expected-pairs.bin");
    println!("10 GiB database: {}", root.display());
    println!("key/value oracle: {}", oracle_path.display());

    let mut oracle = Oracle::new();
    {
        let db = Db::open(&create_options(), &root)?;
        exercise_initial_crud(&db, &mut oracle)?;
        fill_through_ten_gib(&db, &mut oracle)?;
        assert_ten_gib_vlog_layout(&root, &db)?;

        // Make the complete 10 GiB prefix durable, then leave a buffered tail
        // for normal Drop/reopen recovery to promote.
        db.write(&WriteOptions { sync: true }, &WriteBatch::new())?;
        let normal_buffered = key_32("normal-buffered");
        let normal_deleted = key_32("normal-deleted");
        db.put(&WriteOptions::default(), &normal_buffered, b"after-ten-gib")?;
        oracle.insert(
            normal_buffered.to_vec(),
            ExpectedValue::Raw(b"after-ten-gib".to_vec()),
        );
        db.put(&WriteOptions::default(), &normal_deleted, b"old")?;
        db.delete(&WriteOptions::default(), &normal_deleted)?;
        oracle.remove(normal_deleted.as_slice());
        write_oracle(&oracle_path, &oracle)?;
    }

    // core_restart: normal Drop and reopen must preserve buffered and durable
    // writes, including pointers retained in files 0, 1, and 2.
    {
        let reopened = Db::open(&Options::default(), &root)?;
        verify_oracle(&reopened, &oracle_path)?;
        assert_eq!(read(&reopened, &key_32("normal-deleted"))?, None);
        assert_ten_gib_vlog_layout(&root, &reopened)?;
        let stats = reopened.stats();
        assert_eq!(stats.head_seq, stats.durable_seq);
        assert_eq!(stats.durability_lag, 0);
    }

    // core_restart: a successful process exit that skips Rust destructors must
    // still reopen to the complete sequence of returned transactions.
    let mut no_drop = spawn_child(NO_DROP_MODE, &root)?;
    let status = wait_for_exit(&mut no_drop, CHILD_TIMEOUT)?;
    assert!(status.success());
    apply_no_drop_expected(&mut oracle);
    write_oracle(&oracle_path, &oracle)?;
    {
        let reopened = Db::open(&Options::default(), &root)?;
        verify_oracle(&reopened, &oracle_path)?;
        assert_eq!(read(&reopened, &key_32("restart-b"))?, None);
        assert_ten_gib_vlog_layout(&root, &reopened)?;
        assert_eq!(reopened.stats().durability_lag, 0);
    }

    // Additional cross-file crash stress: kill around one 512-key public
    // WriteBatch. Exact protocol-point observation is covered by core_crash.
    // Recovery may accept or reject this transaction, but never a subset.
    let mut writer = spawn_child(CRASH_WRITER_MODE, &root)?;
    let writer_lines = start_line_reader(&mut writer)?;
    let observed = wait_for_marker(&mut writer, &writer_lines, "BEGIN candidate")?;
    assert!(observed.iter().any(|line| line == "DONE complete"));
    thread::sleep(Duration::from_millis(2));
    assert!(!kill_and_wait(&mut writer)?.success());

    {
        let reopened = Db::open(&Options::default(), &root)?;
        verify_oracle(&reopened, &oracle_path)?;
        for slot in 0..CRASH_COMPLETE_KEYS {
            let key = crash_complete_key(slot);
            let value = generated_value(CRASH_COMPLETE_TAG, slot, 40_000);
            assert_eq!(read(&reopened, &key)?, Some(value.clone()));
            oracle.insert(
                key.to_vec(),
                ExpectedValue::Generated {
                    tag: CRASH_COMPLETE_TAG,
                    slot,
                    len: value.len(),
                },
            );
        }

        let first_candidate_present = read(&reopened, &crash_candidate_key(0))?.is_some();
        for slot in 0..CRASH_CANDIDATE_KEYS {
            let key = crash_candidate_key(slot);
            let expected = generated_value(CRASH_CANDIDATE_TAG, slot, CRASH_VALUE_LEN);
            let actual = read(&reopened, &key)?;
            assert_eq!(
                actual.is_some(),
                first_candidate_present,
                "a killed cross-file database batch became partially visible"
            );
            if first_candidate_present {
                assert_eq!(actual, Some(expected.clone()));
                oracle.insert(
                    key.to_vec(),
                    ExpectedValue::Generated {
                        tag: CRASH_CANDIDATE_TAG,
                        slot,
                        len: expected.len(),
                    },
                );
            }
        }
        let stats = reopened.stats();
        assert_eq!(stats.head_seq, stats.durable_seq);
        assert_eq!(stats.durability_lag, 0);
        assert_ten_gib_vlog_layout(&root, &reopened)?;
    }
    write_oracle(&oracle_path, &oracle)?;

    // Additional large-database reopen stress: leave a buffered tail without
    // Drop and kill around a subsequent Open. Exact interrupted-Recovery
    // observation is covered by core_crash.
    let mut prepare_recovery = spawn_child(PREPARE_RECOVERY_MODE, &root)?;
    let status = wait_for_exit(&mut prepare_recovery, CHILD_TIMEOUT)?;
    assert!(status.success());
    for slot in 0..12 {
        oracle.insert(
            recovery_key(slot).to_vec(),
            ExpectedValue::Generated {
                tag: RECOVERY_TAG,
                slot,
                len: 40_000,
            },
        );
    }
    write_oracle(&oracle_path, &oracle)?;

    let mut recovery = spawn_child(OPEN_RECOVERY_MODE, &root)?;
    let recovery_lines = start_line_reader(&mut recovery)?;
    let _ = wait_for_marker(&mut recovery, &recovery_lines, "OPENING")?;
    thread::sleep(Duration::from_millis(2));
    let completed_before_kill = recovery_lines.try_iter().any(|line| line == "OPENED");
    let recovery_status = kill_and_wait(&mut recovery)?;
    assert!(
        !completed_before_kill,
        "heavy recovery child completed Open before SIGKILL"
    );
    assert!(!recovery_status.success());

    let reopened = Db::open(&Options::default(), &root)?;
    verify_oracle(&reopened, &oracle_path)?;
    assert_ten_gib_vlog_layout(&root, &reopened)?;
    let stats = reopened.stats();
    assert_eq!(stats.head_seq, stats.durable_seq);
    assert_eq!(stats.durability_lag, 0);
    Ok(())
}
