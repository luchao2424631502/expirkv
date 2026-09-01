use std::sync::{Arc, Condvar, Mutex, mpsc};
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

struct BarrierState {
    arrived: usize,
    generation: u64,
}

struct TimedBarrier {
    participants: usize,
    state: Mutex<BarrierState>,
    changed: Condvar,
}

impl TimedBarrier {
    fn new(participants: usize) -> Self {
        Self {
            participants,
            state: Mutex::new(BarrierState {
                arrived: 0,
                generation: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait(&self, stage: &str) -> WorkerResult {
        let deadline = Instant::now() + TIMEOUT;
        let mut state = self
            .state
            .lock()
            .map_err(|_| format!("stage={stage} barrier mutex poisoned"))?;
        let generation = state.generation;
        state.arrived += 1;
        if state.arrived == self.participants {
            state.arrived = 0;
            state.generation += 1;
            self.changed.notify_all();
            return Ok(());
        }

        while state.generation == generation {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "stage={stage} barrier timeout generation={generation} arrived={}/{}",
                    state.arrived, self.participants
                ));
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| format!("stage={stage} barrier mutex poisoned while waiting"))?;
            state = next;
            if wait.timed_out() && state.generation == generation {
                return Err(format!(
                    "stage={stage} barrier timeout generation={generation} arrived={}/{}",
                    state.arrived, self.participants
                ));
            }
        }
        Ok(())
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

fn verify_snapshot_rows(db: &Db, snapshot: &rustkv::Snapshot) -> WorkerResult {
    let options = ReadOptions {
        snapshot: Some(snapshot),
    };
    for (key, expected) in [
        (b"a".as_slice(), Some(b"old-a".as_slice())),
        (b"b".as_slice(), Some(b"old-b".as_slice())),
        (b"c".as_slice(), Some(b"old-c".as_slice())),
        (b"d".as_slice(), None),
    ] {
        let actual = db
            .get(&options, key)
            .map_err(|error| format!("snapshot get failed: {error:?}"))?;
        if actual.as_deref() != expected {
            return Err(format!("snapshot get changed for key={key:?}"));
        }
    }

    let mut range = db
        .range(
            &options,
            KeyRange {
                start: Some(b"a"),
                end: Some(b"z"),
            },
            8,
        )
        .map_err(|error| format!("snapshot range creation failed: {error:?}"))?;
    let mut rows = Vec::new();
    while range.valid() {
        rows.push((
            range.key().unwrap().to_vec(),
            range.value().unwrap().to_vec(),
        ));
        range.next();
    }
    range
        .status()
        .map_err(|error| format!("snapshot range failed: {error:?}"))?;
    if rows
        != vec![
            (b"a".to_vec(), b"old-a".to_vec()),
            (b"b".to_vec(), b"old-b".to_vec()),
            (b"c".to_vec(), b"old-c".to_vec()),
        ]
    {
        return Err("explicit snapshot range changed".to_owned());
    }
    Ok(())
}

#[test]
fn explicit_snapshot_and_precreated_cursors_remain_fixed_during_writes() -> TestResult {
    const PARTICIPANTS: usize = 4;
    const WRITE_ROUNDS: u64 = 40;

    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    let mut initial = WriteBatch::new();
    initial.put(b"a", b"old-a")?;
    initial.put(b"b", b"old-b")?;
    initial.put(b"c", b"old-c")?;
    db.write(&WriteOptions { sync: true }, &initial)?;

    let snapshot = db.snapshot()?;
    let explicit_cursor = db.iter(&ReadOptions {
        snapshot: Some(&snapshot),
    })?;
    let implicit_cursor = db.range(
        &ReadOptions::default(),
        KeyRange {
            start: Some(b"a"),
            end: Some(b"z"),
        },
        8,
    )?;

    let db = Arc::new(db);
    let phases = Arc::new(TimedBarrier::new(PARTICIPANTS));
    let writer_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel();
    let mut handles = Vec::new();

    let writer_db = Arc::clone(&db);
    let writer_phases = Arc::clone(&phases);
    let writer_done = Arc::clone(&writer_finished);
    let writer_sender = sender.clone();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            writer_phases.wait("start")?;
            for version in 1..=WRITE_ROUNDS {
                let mut batch = WriteBatch::new();
                batch
                    .put(b"a", version.to_le_bytes())
                    .map_err(|error| format!("overwrite construction failed: {error:?}"))?;
                batch
                    .delete(b"b")
                    .map_err(|error| format!("delete construction failed: {error:?}"))?;
                batch
                    .delete(b"c")
                    .map_err(|error| format!("delete construction failed: {error:?}"))?;
                batch
                    .put(b"d", version.to_le_bytes())
                    .map_err(|error| format!("insert construction failed: {error:?}"))?;
                writer_db
                    .write(&WriteOptions::default(), &batch)
                    .map_err(|error| format!("writer batch failed: {error:?}"))?;

                if version == 1 {
                    writer_phases.wait("first-write-visible")?;
                    writer_phases.wait("fixed-view-checked")?;
                }
                thread::yield_now();
            }
            writer_db
                .write(&WriteOptions { sync: true }, &WriteBatch::new())
                .map_err(|error| format!("writer durability barrier failed: {error:?}"))?;
            Ok(())
        })();
        writer_done.store(true, std::sync::atomic::Ordering::Release);
        let _ = writer_sender.send(result);
    }));

    let reader_db = Arc::clone(&db);
    let reader_snapshot = snapshot.clone();
    let reader_phases = Arc::clone(&phases);
    let reader_done = Arc::clone(&writer_finished);
    let reader_sender = sender.clone();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            reader_phases.wait("start")?;
            reader_phases.wait("first-write-visible")?;
            verify_snapshot_rows(&reader_db, &reader_snapshot)?;
            reader_phases.wait("fixed-view-checked")?;

            let mut checks = 0;
            while checks < 64
                || (!reader_done.load(std::sync::atomic::Ordering::Acquire) && checks < 1_000)
            {
                verify_snapshot_rows(&reader_db, &reader_snapshot)?;
                checks += 1;
                if checks % 4 == 0 {
                    thread::yield_now();
                }
            }
            Ok(())
        })();
        let _ = reader_sender.send(result);
    }));

    let explicit_phases = Arc::clone(&phases);
    let explicit_sender = sender.clone();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            explicit_phases.wait("start")?;
            explicit_phases.wait("first-write-visible")?;
            let mut cursor = explicit_cursor;
            cursor.seek_to_first();
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
                .map_err(|error| format!("explicit iterator failed: {error:?}"))?;
            if rows
                != vec![
                    (b"a".to_vec(), b"old-a".to_vec()),
                    (b"b".to_vec(), b"old-b".to_vec()),
                    (b"c".to_vec(), b"old-c".to_vec()),
                ]
            {
                return Err("explicit iterator view changed".to_owned());
            }
            explicit_phases.wait("fixed-view-checked")?;
            Ok(())
        })();
        let _ = explicit_sender.send(result);
    }));

    let implicit_phases = Arc::clone(&phases);
    let implicit_sender = sender.clone();
    handles.push(thread::spawn(move || {
        let result = (|| -> WorkerResult {
            implicit_phases.wait("start")?;
            implicit_phases.wait("first-write-visible")?;
            let mut cursor = implicit_cursor;
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
                .map_err(|error| format!("implicit range failed: {error:?}"))?;
            if rows
                != vec![
                    (b"a".to_vec(), b"old-a".to_vec()),
                    (b"b".to_vec(), b"old-b".to_vec()),
                    (b"c".to_vec(), b"old-c".to_vec()),
                ]
            {
                return Err("implicit range view changed".to_owned());
            }
            implicit_phases.wait("fixed-view-checked")?;
            Ok(())
        })();
        let _ = implicit_sender.send(result);
    }));
    drop(sender);

    receive_all("snapshot-cursor-overlap", PARTICIPANTS, &receiver)?;
    for handle in handles {
        handle.join().map_err(|_| "snapshot worker panicked")?;
    }

    verify_snapshot_rows(&db, &snapshot)?;
    assert_eq!(
        db.get(&ReadOptions::default(), b"a")?,
        Some(WRITE_ROUNDS.to_le_bytes().to_vec())
    );
    assert_eq!(db.get(&ReadOptions::default(), b"b")?, None);
    assert_eq!(db.get(&ReadOptions::default(), b"c")?, None);
    assert_eq!(
        db.get(&ReadOptions::default(), b"d")?,
        Some(WRITE_ROUNDS.to_le_bytes().to_vec())
    );
    Ok(())
}
