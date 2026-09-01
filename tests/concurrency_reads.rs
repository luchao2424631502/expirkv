use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use rustkv::{Db, KeyRange, Options, ReadOptions, WriteBatch, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
type WorkerResult = std::result::Result<(), String>;

const TIMEOUT: Duration = Duration::from_secs(20);

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn wait_for_workers(
    stage: &str,
    expected: usize,
    receiver: &mpsc::Receiver<WorkerResult>,
) -> TestResult {
    let deadline = Instant::now() + TIMEOUT;
    for completed in 0..expected {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = receiver.recv_timeout(remaining).map_err(|error| {
            format!(
                "stage={stage} completed={completed}/{expected} timed out or disconnected: {error}"
            )
        })?;
        result.map_err(|error| format!("stage={stage} worker failed: {error}"))?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteApi {
    Put,
    Delete,
    WriteBatch,
}

#[derive(Clone, Copy, Debug)]
struct CallWindow<T> {
    operation: T,
    started: u64,
    returned: u64,
}

fn calls_overlap<T, U>(left: &CallWindow<T>, right: &CallWindow<U>) -> bool {
    left.started < right.returned && right.started < left.returned
}

fn collect_range(db: &Db, start: &[u8], end: &[u8], limit: usize) -> WorkerResult {
    let mut cursor = db
        .range(
            &ReadOptions::default(),
            KeyRange {
                start: Some(start),
                end: Some(end),
            },
            limit,
        )
        .map_err(|error| format!("range creation failed: {error:?}"))?;
    let mut rows = Vec::new();
    while cursor.valid() {
        rows.push((
            cursor.key().unwrap().to_vec(),
            cursor.value().unwrap().to_vec(),
        ));
        cursor.next();
    }
    cursor
        .status()
        .map_err(|error| format!("range cursor failed: {error:?}"))?;

    if rows.len() != limit {
        return Err(format!(
            "range returned {} rows, expected {limit}",
            rows.len()
        ));
    }
    for (offset, (key, value)) in rows.iter().enumerate() {
        let expected = format!("key-{:03}", 16 + offset);
        if key != expected.as_bytes() || value != format!("value-{:03}", 16 + offset).as_bytes() {
            return Err(format!("unexpected range row at offset {offset}"));
        }
    }
    Ok(())
}

#[test]
fn get_range_and_independent_cursors_run_on_multiple_os_threads() -> TestResult {
    const THREADS: usize = 8;
    const GET_ROUNDS: usize = 64;

    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let mut initial = WriteBatch::new();
    for index in 0..64 {
        initial.put(format!("key-{index:03}"), format!("value-{index:03}"))?;
    }
    db.write(&WriteOptions { sync: true }, &initial)?;

    let db = Arc::new(db);
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();
    let mut start_senders = Vec::new();
    for thread_id in 0..THREADS {
        let db = Arc::clone(&db);
        let entered = Arc::clone(&entered);
        let (start_sender, start_receiver) = mpsc::channel();
        start_senders.push(start_sender);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("reader {thread_id} start timeout: {error}"))?;
                entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let entry_deadline = Instant::now() + TIMEOUT;
                while entered.load(std::sync::atomic::Ordering::SeqCst) != THREADS {
                    if Instant::now() >= entry_deadline {
                        return Err(format!(
                            "reader {thread_id} timed out waiting for all read workers to enter"
                        ));
                    }
                    thread::yield_now();
                }
                for round in 0..GET_ROUNDS {
                    let index = (thread_id * 17 + round * 13) % 64;
                    let key = format!("key-{index:03}");
                    let expected = format!("value-{index:03}").into_bytes();
                    let actual = db
                        .get(&ReadOptions::default(), key.as_bytes())
                        .map_err(|error| format!("get failed: {error:?}"))?;
                    if actual.as_deref() != Some(expected.as_slice()) {
                        return Err(format!("get mismatch for {key}"));
                    }
                }

                collect_range(&db, b"key-016", b"key-024", 8)?;

                let mut cursor = db
                    .iter(&ReadOptions::default())
                    .map_err(|error| format!("iterator creation failed: {error:?}"))?;
                cursor.seek_to_first();
                for index in 0..10 {
                    if !cursor.valid()
                        || cursor.key() != Some(format!("key-{index:03}").as_bytes())
                        || cursor.value() != Some(format!("value-{index:03}").as_bytes())
                    {
                        return Err(format!("iterator mismatch at row {index}"));
                    }
                    cursor.next();
                }
                cursor
                    .status()
                    .map_err(|error| format!("iterator failed: {error:?}"))?;
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    drop(sender);
    for start in start_senders {
        start.send(())?;
    }

    wait_for_workers("parallel-get-range-cursor", THREADS, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "read worker panicked")?;
    }
    Ok(())
}

#[test]
fn reads_overlapping_put_delete_and_write_batch_never_observe_partial_batch() -> TestResult {
    const READERS: usize = 6;
    const WRITE_ROUNDS: u64 = 240;
    const MIN_READS: usize = 240;
    const MAX_READS: usize = 12_000;

    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let mut initial = WriteBatch::new();
    initial.put(b"pair/a", 0_u64.to_le_bytes())?;
    initial.put(b"pair/b", 0_u64.to_le_bytes())?;
    initial.put(b"hot", 0_u64.to_le_bytes())?;
    db.write(&WriteOptions { sync: true }, &initial)?;

    let db = Arc::new(db);
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let get_windows = Arc::new(Mutex::new(Vec::new()));
    let write_windows = Arc::new(Mutex::new(Vec::new()));
    let (sender, receiver) = mpsc::channel();
    let (ready_sender, ready_receiver) = mpsc::channel();
    let mut handles = Vec::new();
    let mut start_senders = Vec::new();

    for reader_id in 0..READERS {
        let db = Arc::clone(&db);
        let (start_sender, start_receiver) = mpsc::channel();
        start_senders.push(start_sender);
        let finished = Arc::clone(&finished);
        let clock = Arc::clone(&clock);
        let get_windows = Arc::clone(&get_windows);
        let ready_sender = ready_sender.clone();
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                ready_sender
                    .send(())
                    .map_err(|error| format!("reader {reader_id} ready report failed: {error}"))?;
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("reader {reader_id} start timeout: {error}"))?;
                let mut reads = 0;
                while reads < MIN_READS
                    || (!finished.load(std::sync::atomic::Ordering::Acquire) && reads < MAX_READS)
                {
                    let get_started = clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    let hot = db
                        .get(&ReadOptions::default(), b"hot")
                        .map_err(|error| format!("reader {reader_id} get failed: {error:?}"))?;
                    let get_returned = clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    if let Some(value) = hot.as_deref() {
                        let encoded: [u8; 8] = value.try_into().map_err(|_| {
                            format!("reader {reader_id} observed malformed hot value")
                        })?;
                        let version = u64::from_le_bytes(encoded);
                        if version > WRITE_ROUNDS {
                            return Err(format!(
                                "reader {reader_id} observed unknown hot version {version}"
                            ));
                        }
                    }
                    get_windows
                        .lock()
                        .map_err(|_| "get-window mutex poisoned".to_owned())?
                        .push(CallWindow {
                            operation: (),
                            started: get_started,
                            returned: get_returned,
                        });

                    let mut cursor = db
                        .range(
                            &ReadOptions::default(),
                            KeyRange {
                                start: Some(b"pair/a"),
                                end: Some(b"pair/c"),
                            },
                            2,
                        )
                        .map_err(|error| format!("reader {reader_id} range failed: {error:?}"))?;
                    let mut rows = Vec::new();
                    while cursor.valid() {
                        rows.push((
                            cursor.key().unwrap().to_vec(),
                            cursor.value().unwrap().to_vec(),
                        ));
                        cursor.next();
                    }
                    cursor
                        .status()
                        .map_err(|error| format!("reader {reader_id} cursor failed: {error:?}"))?;
                    match rows.as_slice() {
                        [] => {}
                        [(left_key, left_value), (right_key, right_value)]
                            if left_key == b"pair/a"
                                && right_key == b"pair/b"
                                && left_value == right_value
                                && left_value.len() == 8 => {}
                        _ => {
                            return Err(format!(
                                "reader {reader_id} observed partial or mixed batch with {} rows",
                                rows.len()
                            ));
                        }
                    }
                    reads += 1;
                    if reads % 8 == 0 {
                        thread::yield_now();
                    }
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }

    let writer_db = Arc::clone(&db);
    let (writer_start_sender, writer_start_receiver) = mpsc::channel();
    start_senders.push(writer_start_sender);
    let writer_finished = Arc::clone(&finished);
    let writer_clock = Arc::clone(&clock);
    let writer_windows = Arc::clone(&write_windows);
    let writer_ready = ready_sender.clone();
    let writer_sender = sender.clone();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            writer_ready
                .send(())
                .map_err(|error| format!("writer ready report failed: {error}"))?;
            writer_start_receiver
                .recv_timeout(TIMEOUT)
                .map_err(|error| format!("writer start timeout: {error}"))?;
            for version in 1..=WRITE_ROUNDS {
                let operation = match version % 3 {
                    0 => WriteApi::Delete,
                    1 => WriteApi::Put,
                    _ => WriteApi::WriteBatch,
                };
                let started = writer_clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                match operation {
                    WriteApi::Delete => {
                        writer_db
                            .delete(&WriteOptions::default(), b"hot")
                            .map_err(|error| format!("overlapping delete failed: {error:?}"))?;
                    }
                    WriteApi::Put => {
                        writer_db
                            .put(&WriteOptions::default(), b"hot", &version.to_le_bytes())
                            .map_err(|error| format!("overlapping put failed: {error:?}"))?;
                    }
                    WriteApi::WriteBatch => {
                        let mut batch = WriteBatch::new();
                        batch
                            .put(b"hot", version.to_le_bytes())
                            .map_err(|error| format!("hot batch put failed: {error:?}"))?;
                        batch
                            .put(b"pair/a", version.to_le_bytes())
                            .map_err(|error| format!("left batch put failed: {error:?}"))?;
                        batch
                            .put(b"pair/b", version.to_le_bytes())
                            .map_err(|error| format!("right batch put failed: {error:?}"))?;
                        writer_db
                            .write(&WriteOptions::default(), &batch)
                            .map_err(|error| format!("overlapping batch failed: {error:?}"))?;
                    }
                }
                let returned = writer_clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                writer_windows
                    .lock()
                    .map_err(|_| "write-window mutex poisoned".to_owned())?
                    .push(CallWindow {
                        operation,
                        started,
                        returned,
                    });
                thread::yield_now();
            }
            writer_db
                .write(&WriteOptions { sync: true }, &WriteBatch::new())
                .map_err(|error| format!("final durability barrier failed: {error:?}"))?;
            Ok(())
        })();
        writer_finished.store(true, std::sync::atomic::Ordering::Release);
        let _ = writer_sender.send(result);
    }));
    drop(sender);
    drop(ready_sender);
    let ready_deadline = Instant::now() + TIMEOUT;
    for ready in 0..READERS + 1 {
        ready_receiver
            .recv_timeout(ready_deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| format!("worker ready {ready}/{} failed: {error}", READERS + 1))?;
    }
    for start in start_senders {
        start.send(())?;
    }

    wait_for_workers("reads-overlapping-writes", READERS + 1, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "overlap worker panicked")?;
    }

    let gets = get_windows
        .lock()
        .map_err(|_| "get-window mutex poisoned")?;
    let writes = write_windows
        .lock()
        .map_err(|_| "write-window mutex poisoned")?;
    assert!(!gets.is_empty());
    assert_eq!(writes.len(), WRITE_ROUNDS as usize);
    for operation in [WriteApi::Put, WriteApi::Delete, WriteApi::WriteBatch] {
        assert!(
            writes
                .iter()
                .filter(|write| write.operation == operation)
                .any(|write| gets.iter().any(|get| calls_overlap(write, get))),
            "no public Get call overlapped a same-key {operation:?} call"
        );
    }
    Ok(())
}
