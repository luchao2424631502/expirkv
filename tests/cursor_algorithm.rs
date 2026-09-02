#![allow(dead_code, unused_imports)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/snapshot.rs"]
mod snapshot;
pub(crate) use snapshot::Snapshot;
#[path = "../src/cursor.rs"]
mod cursor;
pub(crate) use cursor::{DbIterator, KeyRange, RangeCursor};

#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};
#[path = "../src/batch.rs"]
mod batch;
pub(crate) use batch::WriteBatch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/options.rs"]
mod options;
pub(crate) use options::{Options, ReadOptions, WriteOptions};
#[path = "../src/db.rs"]
mod db;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/recovery/mod.rs"]
mod recovery;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use db::{
    Db, ReadRuntime, ReadStateSnapshot, UserIndexIterator, UserIndexReader, UserIndexSnapshot,
    ValueReader,
};
use index::{IndexEntry, UserKeyRange};
use runtime::RuntimeControl;
use stats::StatsState;

impl ReadRuntime for RuntimeControl {
    fn state_snapshot(&self) -> ReadStateSnapshot {
        let state = self.state();
        ReadStateSnapshot {
            instance_state: state.instance_state,
            state_epoch: state.state_epoch,
        }
    }

    fn latch_read_failure(&self, target: InstanceState, error: &StorageError) -> ReadStateSnapshot {
        let state = self.latch_failure(target, error).current;
        ReadStateSnapshot {
            instance_state: state.instance_state,
            state_epoch: state.state_epoch,
        }
    }

    fn read_stats(&self) -> DbStats {
        self.stats()
    }
}

#[derive(Default)]
struct IteratorCounts {
    full_creations: AtomicUsize,
    range_creations: AtomicUsize,
    next_calls: AtomicUsize,
    next_back_calls: AtomicUsize,
    consumed_entries: AtomicUsize,
    created_range_keys: Mutex<Vec<Vec<Vec<u8>>>>,
}

struct CountingIterator {
    entries: Vec<IndexEntry>,
    front: usize,
    back: usize,
    counts: Arc<IteratorCounts>,
    error_key: Option<Vec<u8>>,
}

impl CountingIterator {
    fn new(
        entries: Vec<IndexEntry>,
        counts: Arc<IteratorCounts>,
        error_key: Option<Vec<u8>>,
    ) -> Self {
        let back = entries.len();
        Self {
            entries,
            front: 0,
            back,
            counts,
            error_key,
        }
    }

    fn item(&self, entry: IndexEntry) -> Result<IndexEntry> {
        self.counts.consumed_entries.fetch_add(1, Ordering::SeqCst);
        if self.error_key.as_deref() == Some(entry.key.as_slice()) {
            return Err(StorageError::codec_error(
                StorageErrorKind::Io,
                Operation::Iterator,
                ProtocolStage::Read,
                None,
                RetryAdvice::DoNotRetry,
            ));
        }
        Ok(entry)
    }
}

impl Iterator for CountingIterator {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.counts.next_calls.fetch_add(1, Ordering::SeqCst);
        if self.front == self.back {
            return None;
        }
        let entry = self.entries[self.front].clone();
        self.front += 1;
        Some(self.item(entry))
    }
}

impl DoubleEndedIterator for CountingIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.counts.next_back_calls.fetch_add(1, Ordering::SeqCst);
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        Some(self.item(self.entries[self.back].clone()))
    }
}

struct CountingIndex {
    entries: Mutex<Vec<IndexEntry>>,
    counts: Arc<IteratorCounts>,
    iterator_error_key: Mutex<Option<Vec<u8>>>,
}

impl CountingIndex {
    fn replace_entries(&self, entries: Vec<IndexEntry>) {
        *self.entries.lock().unwrap() = entries;
    }

    fn fail_iterator_at(&self, key: &[u8]) {
        *self.iterator_error_key.lock().unwrap() = Some(key.to_vec());
    }
}

struct CountingSnapshot {
    entries: Vec<IndexEntry>,
    counts: Arc<IteratorCounts>,
    iterator_error_key: Option<Vec<u8>>,
}

impl CountingSnapshot {
    fn iterator(&self, entries: Vec<IndexEntry>) -> UserIndexIterator {
        Box::new(CountingIterator::new(
            entries,
            Arc::clone(&self.counts),
            self.iterator_error_key.clone(),
        ))
    }
}

impl UserIndexSnapshot for CountingSnapshot {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.clone()))
    }

    fn iter_user(&self) -> Result<UserIndexIterator> {
        self.counts.full_creations.fetch_add(1, Ordering::SeqCst);
        Ok(self.iterator(self.entries.clone()))
    }

    fn iter_user_range(&self, range: UserKeyRange) -> Result<UserIndexIterator> {
        self.counts.range_creations.fetch_add(1, Ordering::SeqCst);
        let entries = self
            .entries
            .iter()
            .filter(|entry| range.contains(&entry.key))
            .cloned()
            .collect::<Vec<_>>();
        self.counts
            .created_range_keys
            .lock()
            .unwrap()
            .push(entries.iter().map(|entry| entry.key.clone()).collect());
        Ok(self.iterator(entries))
    }
}

impl UserIndexReader for CountingIndex {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.clone()))
    }

    fn snapshot_view(self: Arc<Self>) -> Result<Arc<dyn UserIndexSnapshot>> {
        Ok(Arc::new(CountingSnapshot {
            entries: self.entries.lock().unwrap().clone(),
            counts: Arc::clone(&self.counts),
            iterator_error_key: self.iterator_error_key.lock().unwrap().clone(),
        }))
    }
}

struct CountingValues {
    calls: AtomicUsize,
    error_key: Mutex<Option<Vec<u8>>>,
}

impl CountingValues {
    fn fail_at(&self, key: &[u8]) {
        *self.error_key.lock().unwrap() = Some(key.to_vec());
    }
}

impl ValueReader for CountingValues {
    fn read_value(&self, encoded_pointer: &[u8], expected_key: &[u8]) -> Result<Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.error_key.lock().unwrap().as_deref() == Some(expected_key) {
            return Err(StorageError::codec_error(
                StorageErrorKind::Corruption,
                Operation::Iterator,
                ProtocolStage::Read,
                None,
                RetryAdvice::DoNotRetry,
            ));
        }
        Ok(encoded_pointer.to_vec())
    }
}

fn entries(keys: &[&[u8]]) -> Vec<IndexEntry> {
    keys.iter()
        .map(|key| IndexEntry::new(key.to_vec(), key.to_vec()))
        .collect()
}

fn counting_db(keys: &[&[u8]]) -> (Db, Arc<CountingIndex>, Arc<CountingValues>) {
    let counts = Arc::new(IteratorCounts::default());
    let index = Arc::new(CountingIndex {
        entries: Mutex::new(entries(keys)),
        counts,
        iterator_error_key: Mutex::new(None),
    });
    let values = Arc::new(CountingValues {
        calls: AtomicUsize::new(0),
        error_key: Mutex::new(None),
    });
    let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
    let db = Db::from_read_components(runtime, Arc::clone(&index), Arc::clone(&values));
    (db, index, values)
}

fn collect_forward(mut iterator: DbIterator) -> Vec<Vec<u8>> {
    iterator.seek_to_first();
    let mut keys = Vec::new();
    while iterator.valid() {
        keys.push(iterator.key().unwrap().to_vec());
        iterator.next();
    }
    assert!(iterator.status().is_ok());
    keys
}

#[test]
fn same_direction_scans_reuse_one_iterator_and_consume_each_entry_once() {
    let expected = [b"a".as_slice(), b"b", b"c", b"d", b"e"];
    let (forward_db, forward_index, _) = counting_db(&expected);
    let forward = forward_db.iter(&ReadOptions::default()).unwrap();
    assert_eq!(
        collect_forward(forward),
        entries(&expected)
            .iter()
            .map(|e| e.key.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        forward_index.counts.full_creations.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        forward_index.counts.range_creations.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        forward_index.counts.consumed_entries.load(Ordering::SeqCst),
        expected.len()
    );
    assert_eq!(
        forward_index.counts.next_calls.load(Ordering::SeqCst),
        expected.len() + 1
    );

    let (reverse_db, reverse_index, _) = counting_db(&expected);
    let mut reverse = reverse_db.iter(&ReadOptions::default()).unwrap();
    reverse.seek_to_last();
    let mut seen = Vec::new();
    while reverse.valid() {
        seen.push(reverse.key().unwrap().to_vec());
        reverse.prev();
    }
    assert_eq!(seen, [b"e", b"d", b"c", b"b", b"a"]);
    assert!(reverse.status().is_ok());
    assert_eq!(
        reverse_index.counts.full_creations.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        reverse_index.counts.range_creations.load(Ordering::SeqCst),
        0
    );
    assert_eq!(
        reverse_index.counts.consumed_entries.load(Ordering::SeqCst),
        expected.len()
    );
    assert_eq!(
        reverse_index.counts.next_back_calls.load(Ordering::SeqCst),
        expected.len() + 1
    );
}

#[test]
fn seek_is_bounded_and_direction_switches_rebuild_only_at_the_current_key() {
    let (db, index, _) = counting_db(&[b"a", b"c", b"e", b"g"]);
    let mut iterator = db.iter(&ReadOptions::default()).unwrap();
    assert_eq!(index.counts.full_creations.load(Ordering::SeqCst), 1);

    iterator.seek(b"d");
    assert_eq!(iterator.key(), Some(b"e".as_slice()));
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 1);
    assert_eq!(index.counts.consumed_entries.load(Ordering::SeqCst), 1);
    assert_eq!(
        index.counts.created_range_keys.lock().unwrap()[0],
        [b"e", b"g"]
    );

    iterator.next();
    assert_eq!(iterator.key(), Some(b"g".as_slice()));
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 1);
    iterator.prev();
    assert_eq!(iterator.key(), Some(b"e".as_slice()));
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 2);
    iterator.next();
    assert_eq!(iterator.key(), Some(b"g".as_slice()));
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 3);

    iterator.seek(b"c");
    assert_eq!(iterator.key(), Some(b"c".as_slice()));
    iterator.seek(b"b");
    assert_eq!(iterator.key(), Some(b"c".as_slice()));
    iterator.seek(b"z");
    assert!(!iterator.valid());
    assert!(iterator.status().is_ok());
    iterator.seek(b"");
    assert_eq!(iterator.key(), Some(b"a".as_slice()));
    assert_eq!(index.counts.full_creations.load(Ordering::SeqCst), 1);
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 7);
}

#[test]
fn range_uses_one_half_open_bounded_iterator_and_limit_does_not_overread() {
    let (db, index, _) = counting_db(&[b"a", b"b", b"c", b"d", b"e"]);
    let mut range = db
        .range(
            &ReadOptions::default(),
            KeyRange {
                start: Some(b"b"),
                end: Some(b"d"),
            },
            usize::MAX,
        )
        .unwrap();
    let mut seen = Vec::new();
    while range.valid() {
        seen.push(range.key().unwrap().to_vec());
        range.next();
    }
    assert_eq!(seen, [b"b", b"c"]);
    assert!(range.status().is_ok());
    assert_eq!(index.counts.full_creations.load(Ordering::SeqCst), 0);
    assert_eq!(index.counts.range_creations.load(Ordering::SeqCst), 1);
    assert_eq!(
        index.counts.created_range_keys.lock().unwrap()[0],
        [b"b", b"c"]
    );
    assert_eq!(index.counts.consumed_entries.load(Ordering::SeqCst), 2);

    let (limited_db, limited_index, _) = counting_db(&[b"a", b"b", b"c", b"d"]);
    let mut limited = limited_db
        .range(
            &ReadOptions::default(),
            KeyRange {
                start: None,
                end: None,
            },
            2,
        )
        .unwrap();
    assert_eq!(limited.key(), Some(b"a".as_slice()));
    limited.next();
    assert_eq!(limited.key(), Some(b"b".as_slice()));
    limited.next();
    assert!(!limited.valid());
    assert_eq!(
        limited_index.counts.range_creations.load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        limited_index.counts.consumed_entries.load(Ordering::SeqCst),
        2
    );
}

#[test]
fn iterator_snapshot_is_fixed_after_index_mutation() {
    let (db, index, _) = counting_db(&[b"a", b"c", b"e"]);
    let iterator = db.iter(&ReadOptions::default()).unwrap();
    index.replace_entries(entries(&[b"a", b"b", b"e", b"g"]));
    assert_eq!(collect_forward(iterator), [b"a", b"c", b"e"]);
}

#[test]
fn index_and_value_failures_are_terminal_without_cursor_recreation() {
    let (first_db, first_index, _) = counting_db(&[b"a", b"b"]);
    first_index.fail_iterator_at(b"a");
    let mut first = first_db.iter(&ReadOptions::default()).unwrap();
    first.seek_to_first();
    assert_eq!(first.status().unwrap_err().kind, StorageErrorKind::Io);
    assert_eq!(first_db.stats().instance_state, InstanceState::Poisoned);
    let creations = first_index.counts.full_creations.load(Ordering::SeqCst);
    first.next();
    first.prev();
    first.seek(b"b");
    assert_eq!(
        first_index.counts.full_creations.load(Ordering::SeqCst),
        creations
    );
    assert_eq!(first_index.counts.range_creations.load(Ordering::SeqCst), 0);

    let (middle_db, middle_index, _) = counting_db(&[b"a", b"b", b"c"]);
    middle_index.fail_iterator_at(b"b");
    let mut middle = middle_db.iter(&ReadOptions::default()).unwrap();
    middle.seek_to_first();
    assert_eq!(middle.key(), Some(b"a".as_slice()));
    middle.next();
    assert_eq!(middle.status().unwrap_err().kind, StorageErrorKind::Io);
    assert_eq!(
        middle_index.counts.consumed_entries.load(Ordering::SeqCst),
        2
    );

    let (value_db, _, values) = counting_db(&[b"a", b"b"]);
    values.fail_at(b"b");
    let mut value = value_db.iter(&ReadOptions::default()).unwrap();
    value.seek_to_first();
    value.next();
    let error = value.status().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.operation, Operation::Iterator);
    assert_eq!(error.retry_advice, RetryAdvice::RestoreOrRepair);
    assert_eq!(value_db.stats().instance_state, InstanceState::Poisoned);
    let calls = values.calls.load(Ordering::SeqCst);
    value.seek_to_first();
    value.next();
    value.prev();
    assert_eq!(values.calls.load(Ordering::SeqCst), calls);
}
