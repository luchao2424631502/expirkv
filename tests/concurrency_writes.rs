use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use std::{fs, path::Path};

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

#[derive(Clone, Copy, Debug)]
struct MixedOperationWindow {
    kind: &'static str,
    started: Instant,
    returned: Instant,
}

#[derive(Clone, Copy, Debug)]
struct FrontierObservation {
    head: u64,
    durable: u64,
    end: Option<(u32, u64)>,
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

fn vlog_inventory(
    root: &Path,
) -> std::result::Result<Vec<(String, u64)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(root.join("vlog"))? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "non-UTF8 VLog file name")?;
        files.push((name, entry.metadata()?.len()));
    }
    files.sort();
    Ok(files)
}

fn persisted_frontier(
    root: &Path,
) -> std::result::Result<(u64, u64, Option<(u32, u64)>), Box<dyn std::error::Error + Send + Sync>> {
    let database = fjall::Database::builder(root.join("index"))
        .manual_journal_persist(true)
        .open()?;
    let system = database.keyspace(
        "rustkv_system_metadata",
        fjall::KeyspaceCreateOptions::default,
    )?;
    let encoded = system
        .get(b"durable_frontier")?
        .ok_or_else(|| "durable frontier is missing")?
        .to_vec();
    if encoded.len() != 39
        || encoded.get(0..4) != Some(b"RKDF".as_slice())
        || u16::from_le_bytes(encoded[4..6].try_into()?) != 0
        || crc32c::crc32c(&encoded[..35]) != u32::from_le_bytes(encoded[35..39].try_into()?)
    {
        return Err("malformed durable frontier".into());
    }
    let durable = u64::from_le_bytes(encoded[6..14].try_into()?);
    let vlog_seq = u64::from_le_bytes(encoded[14..22].try_into()?);
    let file_id = u32::from_le_bytes(encoded[23..27].try_into()?);
    let offset = u64::from_le_bytes(encoded[27..35].try_into()?);
    let end = match encoded[22] {
        0 if file_id == 0 && offset == 0 => None,
        1 => Some((file_id, offset)),
        _ => return Err("invalid durable VLog end".into()),
    };
    Ok((durable, vlog_seq, end))
}

#[test]
fn concurrent_put_delete_batches_and_barriers_preserve_frontiers_without_delete_vlog_growth()
-> TestResult {
    const SEEDS: usize = 16;
    const PUTS: usize = 24;
    const SINGLE_DELETES: usize = 24;
    const DELETE_BATCHES: usize = 12;
    const SYNC_DELETES: usize = 12;
    const EMPTY_BARRIERS: usize = 12;

    let folder = TempDir::new()?;
    let mixed_root = folder.path().join("mixed");
    let control_root = folder.path().join("put-only-control");
    let mixed = Arc::new(Db::open(&create_options(), &mixed_root)?);
    let control = Db::open(&create_options(), &control_root)?;
    let value = [0x5a_u8; 32];

    // The control database receives the same number of Put transactions with
    // the same key/value lengths. Fixed-width envelope metadata therefore
    // makes it an exact physical-length control even though commit_seq/UUID
    // bytes differ from the interleaved database.
    for seed in 0..SEEDS {
        let key = format!("seed-{seed:04}");
        mixed.put(&WriteOptions::default(), key.as_bytes(), &value)?;
        control.put(&WriteOptions::default(), key.as_bytes(), &value)?;
    }
    for item in 0..DELETE_BATCHES {
        for side in ["left", "right"] {
            let key = format!("batch-{item:04}-{side}");
            mixed.put(&WriteOptions::default(), key.as_bytes(), &value)?;
            control.put(&WriteOptions::default(), key.as_bytes(), &value)?;
        }
    }
    for item in 0..PUTS {
        let key = format!("put-{item:04}");
        control.put(&WriteOptions::default(), key.as_bytes(), &value)?;
    }
    control.write(&WriteOptions { sync: true }, &WriteBatch::new())?;

    let start = Arc::new(std::sync::Barrier::new(5));
    let first_calls = Arc::new(std::sync::Barrier::new(4));
    let windows = Arc::new(Mutex::new(Vec::<MixedOperationWindow>::new()));
    let frontier_observations = Arc::new(Mutex::new(Vec::<FrontierObservation>::new()));
    let batch_reader_ready = Arc::new(AtomicBool::new(false));
    let batch_done = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();

    {
        let db = Arc::clone(&mixed);
        let start = Arc::clone(&start);
        let first_calls = Arc::clone(&first_calls);
        let windows = Arc::clone(&windows);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            let result = (|| -> WorkerResult {
                let first_started = Instant::now();
                first_calls.wait();
                for item in 0..PUTS {
                    let key = format!("put-{item:04}");
                    db.put(&WriteOptions::default(), key.as_bytes(), &[0x5a; 32])
                        .map_err(|error| format!("put {item} failed: {error:?}"))?;
                    if item == 0 {
                        windows
                            .lock()
                            .map_err(|_| "operation-window mutex poisoned".to_owned())?
                            .push(MixedOperationWindow {
                                kind: "put",
                                started: first_started,
                                returned: Instant::now(),
                            });
                    }
                    thread::yield_now();
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    {
        let db = Arc::clone(&mixed);
        let start = Arc::clone(&start);
        let first_calls = Arc::clone(&first_calls);
        let windows = Arc::clone(&windows);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            let result = (|| -> WorkerResult {
                let first_started = Instant::now();
                first_calls.wait();
                for item in 0..SINGLE_DELETES {
                    let key = format!("seed-{:04}", item % SEEDS);
                    db.delete(&WriteOptions::default(), key.as_bytes())
                        .map_err(|error| format!("delete {item} failed: {error:?}"))?;
                    if item == 0 {
                        windows
                            .lock()
                            .map_err(|_| "operation-window mutex poisoned".to_owned())?
                            .push(MixedOperationWindow {
                                kind: "delete",
                                started: first_started,
                                returned: Instant::now(),
                            });
                    }
                    thread::yield_now();
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    {
        let db = Arc::clone(&mixed);
        let start = Arc::clone(&start);
        let first_calls = Arc::clone(&first_calls);
        let windows = Arc::clone(&windows);
        let reader_ready = Arc::clone(&batch_reader_ready);
        let done = Arc::clone(&batch_done);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            let result = (|| -> WorkerResult {
                let ready_deadline = Instant::now() + TIMEOUT;
                while !reader_ready.load(Ordering::Acquire) {
                    if Instant::now() >= ready_deadline {
                        return Err("batch reader did not become ready".to_owned());
                    }
                    thread::yield_now();
                }
                let first_started = Instant::now();
                first_calls.wait();
                for item in 0..DELETE_BATCHES {
                    let mut batch = WriteBatch::new();
                    batch
                        .delete(format!("batch-{item:04}-left").as_bytes())
                        .map_err(|error| format!("batch delete construction: {error:?}"))?;
                    batch
                        .delete(format!("batch-{item:04}-right").as_bytes())
                        .map_err(|error| format!("batch delete construction: {error:?}"))?;
                    db.write(&WriteOptions::default(), &batch)
                        .map_err(|error| format!("delete batch {item} failed: {error:?}"))?;
                    if item == 0 {
                        windows
                            .lock()
                            .map_err(|_| "operation-window mutex poisoned".to_owned())?
                            .push(MixedOperationWindow {
                                kind: "delete-batch",
                                started: first_started,
                                returned: Instant::now(),
                            });
                    }
                    thread::yield_now();
                }
                Ok(())
            })();
            done.store(true, Ordering::Release);
            let _ = sender.send(result);
        }));
    }
    {
        let db = Arc::clone(&mixed);
        let start = Arc::clone(&start);
        let first_calls = Arc::clone(&first_calls);
        let windows = Arc::clone(&windows);
        let observations = Arc::clone(&frontier_observations);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            let result = (|| -> WorkerResult {
                let first_started = Instant::now();
                first_calls.wait();
                for item in 0..SYNC_DELETES {
                    let key = format!("seed-{:04}", (item + 4) % SEEDS);
                    db.delete(&WriteOptions { sync: true }, key.as_bytes())
                        .map_err(|error| format!("sync delete {item} failed: {error:?}"))?;
                    let after_delete = db.stats();
                    observations
                        .lock()
                        .map_err(|_| "frontier-observation mutex poisoned".to_owned())?
                        .push(FrontierObservation {
                            head: after_delete.head_seq,
                            durable: after_delete.durable_seq,
                            end: after_delete
                                .durable_vlog_end
                                .map(|end| (end.file_id, end.offset)),
                        });
                    db.write(&WriteOptions { sync: true }, &WriteBatch::new())
                        .map_err(|error| format!("empty barrier {item} failed: {error:?}"))?;
                    let after_empty = db.stats();
                    observations
                        .lock()
                        .map_err(|_| "frontier-observation mutex poisoned".to_owned())?
                        .push(FrontierObservation {
                            head: after_empty.head_seq,
                            durable: after_empty.durable_seq,
                            end: after_empty
                                .durable_vlog_end
                                .map(|end| (end.file_id, end.offset)),
                        });
                    if item == 0 {
                        windows
                            .lock()
                            .map_err(|_| "operation-window mutex poisoned".to_owned())?
                            .push(MixedOperationWindow {
                                kind: "sync-delete-and-empty-barrier",
                                started: first_started,
                                returned: Instant::now(),
                            });
                    }
                    thread::yield_now();
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    {
        let db = Arc::clone(&mixed);
        let start = Arc::clone(&start);
        let ready = Arc::clone(&batch_reader_ready);
        let done = Arc::clone(&batch_done);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            ready.store(true, Ordering::Release);
            let result = (|| -> WorkerResult {
                let mut checks = 0_usize;
                while checks < 8 || !done.load(Ordering::Acquire) {
                    let snapshot = db
                        .snapshot()
                        .map_err(|error| format!("batch observer snapshot failed: {error:?}"))?;
                    let options = ReadOptions {
                        snapshot: Some(&snapshot),
                    };
                    for item in 0..DELETE_BATCHES {
                        let left_key = format!("batch-{item:04}-left");
                        let right_key = format!("batch-{item:04}-right");
                        let left = db
                            .get(&options, left_key.as_bytes())
                            .map_err(|error| format!("batch left read failed: {error:?}"))?;
                        let right = db
                            .get(&options, right_key.as_bytes())
                            .map_err(|error| format!("batch right read failed: {error:?}"))?;
                        if left.is_some() != right.is_some() {
                            return Err(format!(
                                "partial pure-Delete Batch observed for item {item}"
                            ));
                        }
                        if left.as_deref().is_some_and(|bytes| bytes != [0x5a; 32])
                            || right.as_deref().is_some_and(|bytes| bytes != [0x5a; 32])
                        {
                            return Err(format!("unexpected batch value for item {item}"));
                        }
                    }
                    checks += 1;
                    thread::yield_now();
                }
                Ok(())
            })();
            let _ = sender.send(result);
        }));
    }
    drop(sender);

    receive_all("mixed-index-only-concurrency", 5, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "mixed writer panicked")?;
    }
    let windows = windows
        .lock()
        .map_err(|_| "operation-window mutex poisoned")?;
    assert_eq!(windows.len(), 4);
    for expected in [
        "put",
        "delete",
        "delete-batch",
        "sync-delete-and-empty-barrier",
    ] {
        assert!(windows.iter().any(|window| window.kind == expected));
    }
    let latest_start = windows
        .iter()
        .map(|window| window.started)
        .max()
        .expect("four operation windows");
    let earliest_return = windows
        .iter()
        .map(|window| window.returned)
        .min()
        .expect("four operation windows");
    assert!(
        latest_start <= earliest_return,
        "first operation windows did not overlap"
    );
    drop(windows);

    let observations = frontier_observations
        .lock()
        .map_err(|_| "frontier-observation mutex poisoned")?;
    assert_eq!(observations.len(), SYNC_DELETES * 2);
    for observation in observations.iter() {
        assert!(observation.durable <= observation.head);
        assert!(observation.end.is_some());
    }
    for pair in observations.windows(2) {
        assert!(pair[0].head <= pair[1].head);
        assert!(pair[0].durable <= pair[1].durable);
        assert!(pair[0].end <= pair[1].end);
    }
    drop(observations);

    let concurrent_transactions =
        (SEEDS + (2 * DELETE_BATCHES) + PUTS + SINGLE_DELETES + DELETE_BATCHES + SYNC_DELETES)
            as u64;
    assert_eq!(mixed.stats().head_seq, concurrent_transactions);
    let tail_put_seq = concurrent_transactions + 1;
    mixed.put(&WriteOptions::default(), b"tail-put", &value)?;
    control.put(&WriteOptions::default(), b"tail-put", &value)?;
    mixed.delete(&WriteOptions { sync: true }, b"tail-delete-missing")?;
    control.write(&WriteOptions { sync: true }, &WriteBatch::new())?;
    mixed.write(&WriteOptions { sync: true }, &WriteBatch::new())?;

    let expected_transactions = tail_put_seq + 1;
    let stats = mixed.stats();
    assert_eq!(
        (stats.head_seq, stats.durable_seq),
        (expected_transactions, expected_transactions)
    );
    assert_eq!(stats.durability_lag, 0);
    let public_end = stats
        .durable_vlog_end
        .as_ref()
        .map(|end| (end.file_id, end.offset));
    assert_eq!(
        vlog_inventory(&mixed_root)?,
        vlog_inventory(&control_root)?,
        "IndexOnlyDelete and both barrier forms must not add VLog bytes"
    );

    for seed in 0..SEEDS {
        let key = format!("seed-{seed:04}");
        assert_eq!(mixed.get(&ReadOptions::default(), key.as_bytes())?, None);
    }
    for item in 0..PUTS {
        let key = format!("put-{item:04}");
        assert_eq!(
            mixed
                .get(&ReadOptions::default(), key.as_bytes())?
                .as_deref(),
            Some(value.as_slice())
        );
    }
    for item in 0..DELETE_BATCHES {
        for side in ["left", "right"] {
            let key = format!("batch-{item:04}-{side}");
            assert_eq!(mixed.get(&ReadOptions::default(), key.as_bytes())?, None);
        }
    }
    assert_eq!(
        mixed.get(&ReadOptions::default(), b"tail-put")?.as_deref(),
        Some(value.as_slice())
    );
    assert_eq!(EMPTY_BARRIERS, SYNC_DELETES);

    drop(mixed);
    drop(control);
    let (durable, durable_vlog_seq, durable_end) = persisted_frontier(&mixed_root)?;
    assert_eq!(durable, expected_transactions);
    assert_eq!(durable_vlog_seq, tail_put_seq);
    assert_eq!(durable_end, public_end);
    let physical = vlog_inventory(&mixed_root)?;
    let (last_name, last_len) = physical.last().expect("VLog contains Put envelopes");
    let last_file_id = last_name
        .strip_prefix('D')
        .and_then(|name| name.strip_suffix(".data"))
        .ok_or("malformed VLog file name")?
        .parse::<u32>()?;
    assert_eq!(durable_end, Some((last_file_id, *last_len)));
    Ok(())
}
