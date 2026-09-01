use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use rustkv::{Db, Options, ReadOptions, StorageErrorKind, WriteOptions};
use tempfile::TempDir;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
type WorkerResult = std::result::Result<(), String>;

const HISTORY_SEED: u64 = 0x18_c0ff_ee42_5eed;
const TIMEOUT: Duration = Duration::from_secs(20);
const KEY_FINGERPRINT: u64 = 0xcbeb_bd90_c578_9341;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HistoryOperation {
    Put(u64),
    Delete,
    Get,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HistoryResult {
    Success,
    Error(StorageErrorKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogicalEffect {
    Set(u64),
    Remove,
    Observe(Option<u64>),
}

// The history intentionally contains only timing/order metadata, a one-way Key
// fingerprint, result classification, and logical numeric effects. It never
// retains the original Key or Value bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HistoryRecord {
    started: u64,
    returned: u64,
    thread_id: usize,
    thread_step: usize,
    operation: HistoryOperation,
    key_fingerprint: u64,
    result: HistoryResult,
    logical_effect: LogicalEffect,
}

struct RoundState {
    arrived: usize,
    generation: usize,
}

struct TimedRoundGate {
    participants: usize,
    state: Mutex<RoundState>,
    changed: Condvar,
}

impl TimedRoundGate {
    fn new(participants: usize) -> Self {
        Self {
            participants,
            state: Mutex::new(RoundState {
                arrived: 0,
                generation: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn wait(&self, thread_id: usize, thread_step: usize) -> WorkerResult {
        let deadline = Instant::now() + TIMEOUT;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "history round-gate mutex poisoned".to_owned())?;
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
                    "history round gate timed out thread={thread_id} step={thread_step} arrived={}/{}",
                    state.arrived, self.participants
                ));
            }
            let (next, wait) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| "history round-gate mutex poisoned while waiting".to_owned())?;
            state = next;
            if wait.timed_out() && state.generation == generation {
                return Err(format!(
                    "history round gate timed out thread={thread_id} step={thread_step} arrived={}/{}",
                    state.arrived, self.participants
                ));
            }
        }
        Ok(())
    }
}

fn create_options() -> Options {
    Options {
        create_if_missing: true,
        ..Options::default()
    }
}

fn fingerprint(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn decode_logical_value(value: Option<Vec<u8>>) -> WorkerResultWith<Option<u64>> {
    value
        .map(|bytes| {
            let encoded: [u8; 8] = bytes
                .try_into()
                .map_err(|_| "read returned a non-logical test value".to_owned())?;
            Ok(u64::from_le_bytes(encoded))
        })
        .transpose()
}

type WorkerResultWith<T> = std::result::Result<T, String>;

fn xorshift(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^ (state << 17)
}

fn invoke(db: &Db, operation: HistoryOperation) -> (HistoryResult, LogicalEffect, WorkerResult) {
    match operation {
        HistoryOperation::Put(value) => match db.put(
            &WriteOptions::default(),
            b"contended-key",
            &value.to_le_bytes(),
        ) {
            Ok(()) => (HistoryResult::Success, LogicalEffect::Set(value), Ok(())),
            Err(error) => (
                HistoryResult::Error(error.kind),
                LogicalEffect::Set(value),
                Err(format!("put failed: {error:?}")),
            ),
        },
        HistoryOperation::Delete => match db.delete(&WriteOptions::default(), b"contended-key") {
            Ok(()) => (HistoryResult::Success, LogicalEffect::Remove, Ok(())),
            Err(error) => (
                HistoryResult::Error(error.kind),
                LogicalEffect::Remove,
                Err(format!("delete failed: {error:?}")),
            ),
        },
        HistoryOperation::Get => match db.get(&ReadOptions::default(), b"contended-key") {
            Ok(value) => match decode_logical_value(value) {
                Ok(observed) => (
                    HistoryResult::Success,
                    LogicalEffect::Observe(observed),
                    Ok(()),
                ),
                Err(error) => (
                    HistoryResult::Error(StorageErrorKind::Corruption),
                    LogicalEffect::Observe(None),
                    Err(error),
                ),
            },
            Err(error) => (
                HistoryResult::Error(error.kind),
                LogicalEffect::Observe(None),
                Err(format!("get failed: {error:?}")),
            ),
        },
    }
}

fn predecessors(history: &[HistoryRecord]) -> Vec<u64> {
    let mut required = vec![0_u64; history.len()];
    for (candidate_index, candidate) in history.iter().enumerate() {
        for (prior_index, prior) in history.iter().enumerate() {
            if prior_index == candidate_index {
                continue;
            }
            let real_time_before = prior.returned < candidate.started;
            let program_order_before =
                prior.thread_id == candidate.thread_id && prior.thread_step < candidate.thread_step;
            if real_time_before || program_order_before {
                required[candidate_index] |= 1_u64 << prior_index;
            }
        }
    }
    required
}

fn apply_record(record: HistoryRecord, state: Option<u64>) -> Option<Option<u64>> {
    if record.result != HistoryResult::Success {
        return None;
    }
    match (record.operation, record.logical_effect) {
        (HistoryOperation::Put(value), LogicalEffect::Set(effect)) if value == effect => {
            Some(Some(value))
        }
        (HistoryOperation::Delete, LogicalEffect::Remove) => Some(None),
        (HistoryOperation::Get, LogicalEffect::Observe(observed)) if observed == state => {
            Some(state)
        }
        _ => None,
    }
}

fn search_linearization(
    history: &[HistoryRecord],
    required: &[u64],
    placed: u64,
    state: Option<u64>,
    final_state: Option<u64>,
    deadline: Instant,
    order: &mut Vec<usize>,
) -> bool {
    if Instant::now() >= deadline {
        return false;
    }
    if order.len() == history.len() {
        return state == final_state;
    }

    for index in 0..history.len() {
        let bit = 1_u64 << index;
        if placed & bit != 0 || required[index] & !placed != 0 {
            continue;
        }
        let Some(next_state) = apply_record(history[index], state) else {
            continue;
        };
        order.push(index);
        if search_linearization(
            history,
            required,
            placed | bit,
            next_state,
            final_state,
            deadline,
            order,
        ) {
            return true;
        }
        order.pop();
    }
    false
}

#[test]
fn seeded_same_key_history_has_a_legal_linearization() -> TestResult {
    const THREADS: usize = 4;
    const OPERATIONS_PER_THREAD: usize = 3;

    assert_eq!(fingerprint(b"contended-key"), KEY_FINGERPRINT);
    let folder = TempDir::new()?;
    let db = Db::open(&create_options(), folder.path().join("db"))?;
    db.put(
        &WriteOptions { sync: true },
        b"contended-key",
        &0_u64.to_le_bytes(),
    )?;

    let schedules = [
        [
            HistoryOperation::Put(101),
            HistoryOperation::Get,
            HistoryOperation::Delete,
        ],
        [
            HistoryOperation::Put(201),
            HistoryOperation::Get,
            HistoryOperation::Put(202),
        ],
        [
            HistoryOperation::Delete,
            HistoryOperation::Put(301),
            HistoryOperation::Get,
        ],
        [
            HistoryOperation::Get,
            HistoryOperation::Put(401),
            HistoryOperation::Get,
        ],
    ];

    let db = Arc::new(db);
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let history = Arc::new(Mutex::new(Vec::new()));
    let rounds = Arc::new(TimedRoundGate::new(THREADS));
    let (sender, receiver) = mpsc::channel();
    let mut start_senders = Vec::new();
    let mut handles = Vec::new();

    for (thread_id, operations) in schedules.into_iter().enumerate() {
        let (start_sender, start_receiver) = mpsc::channel();
        start_senders.push(start_sender);
        let db = Arc::clone(&db);
        let clock = Arc::clone(&clock);
        let history = Arc::clone(&history);
        let rounds = Arc::clone(&rounds);
        let sender = sender.clone();
        handles.push(thread::spawn(move || {
            let result = (|| -> WorkerResult {
                start_receiver
                    .recv_timeout(TIMEOUT)
                    .map_err(|error| format!("thread {thread_id} start timeout: {error}"))?;
                let mut schedule_state = HISTORY_SEED ^ ((thread_id as u64 + 1) << 32);
                for (thread_step, operation) in operations.into_iter().enumerate() {
                    schedule_state = xorshift(schedule_state);
                    for _ in 0..(schedule_state % 11) {
                        thread::yield_now();
                    }

                    let started = clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    // The invocation event is recorded before this bounded gate.
                    // Consequently all four same-key operations in a round are
                    // outstanding before any of them can enter the Db method.
                    rounds.wait(thread_id, thread_step)?;
                    let (result_kind, logical_effect, operation_result) = invoke(&db, operation);
                    let returned = clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    history
                        .lock()
                        .map_err(|_| "history mutex poisoned".to_owned())?
                        .push(HistoryRecord {
                            started,
                            returned,
                            thread_id,
                            thread_step,
                            operation,
                            key_fingerprint: KEY_FINGERPRINT,
                            result: result_kind,
                            logical_effect,
                        });
                    operation_result?;
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

    let completion_deadline = Instant::now() + TIMEOUT;
    for completed in 0..THREADS {
        let result = receiver
            .recv_timeout(completion_deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| {
                format!("history seed={HISTORY_SEED:#x} completed={completed}/{THREADS}: {error}")
            })?;
        result.map_err(|error| format!("history worker failed: {error}"))?;
    }
    for handle in handles {
        handle.join().map_err(|_| "history worker panicked")?;
    }

    let final_state = decode_logical_value(db.get(&ReadOptions::default(), b"contended-key")?)?;
    let mut history = Arc::try_unwrap(history)
        .map_err(|_| "history still has owners")?
        .into_inner()
        .map_err(|_| "history mutex poisoned")?;
    history.sort_by_key(|record| (record.started, record.returned));
    assert_eq!(history.len(), THREADS * OPERATIONS_PER_THREAD);
    for record in &history {
        assert!(record.started < record.returned);
        assert_eq!(record.key_fingerprint, KEY_FINGERPRINT);
        assert_eq!(record.result, HistoryResult::Success);
    }
    assert!(
        history.iter().enumerate().any(|(left_index, left)| {
            history.iter().skip(left_index + 1).any(|right| {
                left.thread_id != right.thread_id
                    && left.started < right.returned
                    && right.started < left.returned
            })
        }),
        "the history contained no overlapping cross-thread operation"
    );

    let required = predecessors(&history);
    let mut order = Vec::with_capacity(history.len());
    let checker_deadline = Instant::now() + TIMEOUT;
    let linearizable = search_linearization(
        &history,
        &required,
        0,
        Some(0),
        final_state,
        checker_deadline,
        &mut order,
    );
    assert!(
        linearizable,
        "seed={HISTORY_SEED:#x} final={final_state:?} has no legal linearization; history={history:?}"
    );
    assert_eq!(order.len(), history.len());
    Ok(())
}
