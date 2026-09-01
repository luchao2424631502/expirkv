use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use rustkv::{Db, Options, ReadOptions, WriteBatch, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
type WorkerResult = std::result::Result<(), String>;

const TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Clone, Copy, Debug)]
struct FirstBatchWindow {
    thread_id: usize,
    started: Instant,
    returned: Instant,
}

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn receive_all(
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

#[test]
fn concurrent_write_batches_have_contiguous_sequences_and_atomic_final_effects() -> TestResult {
    const THREADS: usize = 8;
    const BATCHES_PER_THREAD: usize = 12;

    let folder = TempDir::new()?;
    let db = Arc::new(Db::open(
        &create_options(),
        folder.path().join("contiguous"),
    )?);
    let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_windows = Arc::new(Mutex::new(Vec::new()));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();
    let mut start_senders = Vec::new();

    for thread_id in 0..THREADS {
        let db = Arc::clone(&db);
        let entered = Arc::clone(&entered);
        let first_windows = Arc::clone(&first_windows);
        let (start_sender, start_receiver) = mpsc::channel();
        start_senders.push(start_sender);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("writer {thread_id} start timeout: {error}"))?;
                let first_started = Instant::now();
                entered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let entry_deadline = Instant::now() + TIMEOUT;
                while entered.load(std::sync::atomic::Ordering::SeqCst) != THREADS {
                    if Instant::now() >= entry_deadline {
                        return Err(format!(
                            "writer {thread_id} timed out waiting for concurrent batch entry"
                        ));
                    }
                    thread::yield_now();
                }
                for round in 0..BATCHES_PER_THREAD {
                    let left_key = format!("txn-{thread_id:02}-{round:02}-a");
                    let right_key = format!("txn-{thread_id:02}-{round:02}-b");
                    let value = format!("value-{thread_id:02}-{round:02}");
                    let mut batch = WriteBatch::new();
                    batch
                        .put(left_key.as_bytes(), value.as_bytes())
                        .map_err(|error| format!("left batch construction failed: {error:?}"))?;
                    batch
                        .put(right_key.as_bytes(), value.as_bytes())
                        .map_err(|error| format!("right batch construction failed: {error:?}"))?;
                    db.write(&WriteOptions::default(), &batch)
                        .map_err(|error| format!("batch commit failed: {error:?}"))?;
                    if round == 0 {
                        first_windows
                            .lock()
                            .map_err(|_| "first-batch window mutex poisoned".to_owned())?
                            .push(FirstBatchWindow {
                                thread_id,
                                started: first_started,
                                returned: Instant::now(),
                            });
                    }

                    // This read starts after the batch returned, so it must see this
                    // transaction or a later state. These keys have no later writer.
                    let observed = db
                        .get(&ReadOptions::default(), left_key.as_bytes())
                        .map_err(|error| format!("post-commit get failed: {error:?}"))?;
                    if observed.as_deref() != Some(value.as_bytes()) {
                        return Err(format!("post-commit visibility failed for {left_key}"));
                    }
                    if round % 3 == 0 {
                        thread::yield_now();
                    }
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    drop(sender);
    for start in start_senders {
        start.send(())?;
    }

    receive_all("concurrent-batch-commit", THREADS, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "write worker panicked")?;
    }

    let first_windows = first_windows
        .lock()
        .map_err(|_| "first-batch window mutex poisoned")?;
    assert_eq!(first_windows.len(), THREADS);
    assert!(first_windows.iter().enumerate().any(|(left_index, left)| {
        first_windows.iter().skip(left_index + 1).any(|right| {
            left.thread_id != right.thread_id
                && left.started < right.returned
                && right.started < left.returned
        })
    }));
    drop(first_windows);

    let expected_transactions = (THREADS * BATCHES_PER_THREAD) as u64;
    let buffered = db.stats();
    assert_eq!(buffered.head_seq, expected_transactions);
    assert!(buffered.durable_seq <= buffered.head_seq);
    assert_eq!(
        buffered.durability_lag,
        buffered.head_seq - buffered.durable_seq
    );
    db.write(&WriteOptions { sync: true }, &WriteBatch::new())?;
    let durable = db.stats();
    assert_eq!(durable.head_seq, expected_transactions);
    assert_eq!(durable.durable_seq, expected_transactions);
    assert_eq!(durable.durability_lag, 0);

    for thread_id in 0..THREADS {
        for round in 0..BATCHES_PER_THREAD {
            let value = format!("value-{thread_id:02}-{round:02}").into_bytes();
            for suffix in ['a', 'b'] {
                let key = format!("txn-{thread_id:02}-{round:02}-{suffix}");
                assert_eq!(
                    db.get(&ReadOptions::default(), key.as_bytes())?,
                    Some(value.clone()),
                    "batch member missing for {key}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn each_writer_preserves_its_own_program_order() -> TestResult {
    const THREADS: usize = 6;
    const ROUNDS: usize = 20;

    let folder = TempDir::new()?;
    let db = Arc::new(Db::open(
        &create_options(),
        folder.path().join("program-order"),
    )?);
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();
    let mut start_senders = Vec::new();

    for thread_id in 0..THREADS {
        let db = Arc::clone(&db);
        let (start_sender, start_receiver) = mpsc::channel();
        start_senders.push(start_sender);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("writer {thread_id} start timeout: {error}"))?;
                let key = format!("ordered-{thread_id:02}");
                for round in 0..ROUNDS {
                    let value = (round as u64).to_le_bytes();
                    db.put(&WriteOptions::default(), key.as_bytes(), &value)
                        .map_err(|error| format!("ordered put failed: {error:?}"))?;
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    drop(sender);
    for start in start_senders {
        start.send(())?;
    }

    receive_all("per-thread-program-order", THREADS, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "ordered writer panicked")?;
    }

    assert_eq!(db.stats().head_seq, (THREADS * ROUNDS) as u64);
    for thread_id in 0..THREADS {
        let key = format!("ordered-{thread_id:02}");
        assert_eq!(
            db.get(&ReadOptions::default(), key.as_bytes())?,
            Some(((ROUNDS - 1) as u64).to_le_bytes().to_vec())
        );
    }
    Ok(())
}

#[test]
fn non_overlapping_same_key_writes_preserve_real_time_order() -> TestResult {
    let folder = TempDir::new()?;
    let db = Arc::new(Db::open(
        &create_options(),
        folder.path().join("real-time"),
    )?);
    let (first_done_sender, first_done_receiver) = mpsc::sync_channel(0);
    let (sender, receiver) = mpsc::channel();

    let first_db = Arc::clone(&db);
    let first_sender = sender.clone();
    let first = thread::spawn(move || {
        let result = first_db
            .put(&WriteOptions::default(), b"same", b"first")
            .map_err(|error| format!("first write failed: {error:?}"));
        if result.is_ok() {
            let _ = first_done_sender.send(());
        }
        let _ = first_sender.send(result);
    });

    let second_db = Arc::clone(&db);
    let second_sender = sender.clone();
    let second = thread::spawn(move || {
        let result = (|| -> WorkerResult {
            first_done_receiver
                .recv_timeout(TIMEOUT)
                .map_err(|error| format!("first write completion wait failed: {error}"))?;
            second_db
                .put(&WriteOptions { sync: true }, b"same", b"second")
                .map_err(|error| format!("second write failed: {error:?}"))?;
            Ok(())
        })();
        let _ = second_sender.send(result);
    });
    drop(sender);

    receive_all("non-overlap-real-time", 2, &receiver)?;
    first.join().map_err(|_| "first writer panicked")?;
    second.join().map_err(|_| "second writer panicked")?;
    assert_eq!(
        db.get(&ReadOptions::default(), b"same")?,
        Some(b"second".to_vec())
    );
    let stats = db.stats();
    assert_eq!(stats.head_seq, 2);
    assert_eq!(stats.durable_seq, 2);
    Ok(())
}
