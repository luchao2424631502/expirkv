#![allow(dead_code, unused_imports)]

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;

use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, HandleOpener, VLogDirectory};
use vlog::format::{PageHeader, VLogFileHeader, VLogGeometry};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn database_uuid() -> [u8; 16] {
    [101, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47]
}

struct CacheHarness {
    _temporary: TempDir,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
}

impl CacheHarness {
    fn new(file_count: u32) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        for file_id in 0..file_count {
            let file = directory.create_new_for_test(file_id)?;
            let page = PageHeader {
                file_id,
                page_no: 0,
            }
            .encode()?;
            let header = VLogFileHeader::new(database_uuid(), file_id).encode()?;
            file.write_all_at(&page, 0)?;
            file.write_all_at(&header, page.len() as u64)?;
            catalog.register(file_id, &file)?;
        }
        Ok(Self {
            _temporary: temporary,
            directory,
            catalog,
        })
    }

    fn files(&self, capacity: usize, opener: Arc<dyn HandleOpener>) -> Result<Arc<FileSet>> {
        Ok(Arc::new(FileSet::with_opener(
            Arc::clone(&self.directory),
            database_uuid(),
            VLogGeometry::PRODUCTION,
            Arc::clone(&self.catalog),
            capacity,
            opener,
        )?))
    }
}

#[derive(Debug, Default)]
struct CountingOpener {
    calls: AtomicUsize,
}

impl HandleOpener for CountingOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        directory.open_read_only(file_id)
    }
}

#[test]
fn capacity_zero_never_retains_handles_and_hits_clone_one_arc() -> TestResult {
    let harness = CacheHarness::new(1)?;
    let zero_opener = Arc::new(CountingOpener::default());
    let zero = harness.files(0, zero_opener.clone())?;
    let first = zero.handle(0)?;
    let second = zero.handle(0)?;
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(zero.cache_len()?, 0);
    assert_eq!(zero_opener.calls.load(Ordering::SeqCst), 2);

    let cached_opener = Arc::new(CountingOpener::default());
    let cached = harness.files(2, cached_opener.clone())?;
    let first = cached.handle(0)?;
    let second = cached.handle(0)?;
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(cached.cache_len()?, 1);
    assert_eq!(cached_opener.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn fifo_does_not_refresh_on_hit_and_evicted_inflight_arc_stays_valid() -> TestResult {
    let harness = CacheHarness::new(3)?;
    let opener = Arc::new(CountingOpener::default());
    let files = harness.files(2, opener.clone())?;
    let held = files.handle(0)?;
    files.handle(1)?;
    files.handle(0)?;
    assert_eq!(files.cache_order()?, vec![0, 1]);
    files.handle(2)?;
    assert_eq!(files.cache_order()?, vec![1, 2]);
    assert_eq!(files.cache_len()?, 2);
    assert!(held.metadata()?.is_file());

    files.handle(0)?;
    assert_eq!(opener.calls.load(Ordering::SeqCst), 4);
    assert_eq!(files.cache_order()?, vec![2, 0]);
    Ok(())
}

#[derive(Debug)]
struct BarrierOpener {
    calls: AtomicUsize,
    barrier: Barrier,
}

impl HandleOpener for BarrierOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.barrier.wait();
        directory.open_read_only(file_id)
    }
}

#[test]
fn concurrent_misses_open_outside_the_lock_and_double_check_one_cache_entry() -> TestResult {
    let harness = CacheHarness::new(1)?;
    let opener = Arc::new(BarrierOpener {
        calls: AtomicUsize::new(0),
        barrier: Barrier::new(2),
    });
    let files = harness.files(1, opener.clone())?;
    let left_files = Arc::clone(&files);
    let left = thread::spawn(move || left_files.handle(0));
    let right_files = Arc::clone(&files);
    let right = thread::spawn(move || right_files.handle(0));
    let left = left.join().expect("left thread")?;
    let right = right.join().expect("right thread")?;
    assert_eq!(opener.calls.load(Ordering::SeqCst), 2);
    assert_eq!(files.cache_len()?, 1);
    assert!(Arc::ptr_eq(&left, &right));
    Ok(())
}

#[derive(Debug)]
struct EmfileOpener {
    target: u32,
    failures: usize,
    target_calls: AtomicUsize,
}

impl HandleOpener for EmfileOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        if file_id == self.target {
            let attempt = self.target_calls.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures {
                return Err(io::Error::from_raw_os_error(24));
            }
        }
        directory.open_read_only(file_id)
    }
}

#[test]
fn emfile_clears_cache_and_retries_the_original_open_exactly_once() -> TestResult {
    let harness = CacheHarness::new(2)?;
    let opener = Arc::new(EmfileOpener {
        target: 1,
        failures: 1,
        target_calls: AtomicUsize::new(0),
    });
    let files = harness.files(2, opener.clone())?;
    let held = files.handle(0)?;
    files.handle(1)?;
    assert_eq!(opener.target_calls.load(Ordering::SeqCst), 2);
    assert_eq!(files.cache_order()?, vec![1]);
    assert!(held.metadata()?.is_file());

    let always = Arc::new(EmfileOpener {
        target: 0,
        failures: usize::MAX,
        target_calls: AtomicUsize::new(0),
    });
    let failing = harness.files(1, always.clone())?;
    let error = failing.handle(0).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
    assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
    assert_eq!(always.target_calls.load(Ordering::SeqCst), 2);
    assert_eq!(failing.cache_len()?, 0);
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum TransientOpenFailure {
    Interrupted,
    WouldBlock,
    Enfile,
}

impl TransientOpenFailure {
    fn error(self) -> io::Error {
        match self {
            Self::Interrupted => io::Error::from(io::ErrorKind::Interrupted),
            Self::WouldBlock => io::Error::from(io::ErrorKind::WouldBlock),
            Self::Enfile => io::Error::from_raw_os_error(23),
        }
    }
}

#[derive(Debug)]
struct TransientOpener {
    failure: TransientOpenFailure,
    remaining_failures: AtomicUsize,
    calls: AtomicUsize,
}

impl HandleOpener for TransientOpener {
    fn open(&self, directory: &VLogDirectory, file_id: u32) -> io::Result<File> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self
            .remaining_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(self.failure.error());
        }
        directory.open_read_only(file_id)
    }
}

#[test]
fn eintr_eagain_and_enfile_use_bounded_open_retry_budgets() -> TestResult {
    for (failure, retry_budget) in [
        (TransientOpenFailure::Interrupted, 8),
        (TransientOpenFailure::WouldBlock, 3),
        (TransientOpenFailure::Enfile, 3),
    ] {
        let success_harness = CacheHarness::new(1)?;
        let success_opener = Arc::new(TransientOpener {
            failure,
            remaining_failures: AtomicUsize::new(retry_budget),
            calls: AtomicUsize::new(0),
        });
        let success = success_harness.files(1, success_opener.clone())?;
        success.handle(0)?;
        assert_eq!(
            success_opener.calls.load(Ordering::SeqCst),
            retry_budget + 1
        );
        assert_eq!(success.cache_len()?, 1);

        let exhausted_harness = CacheHarness::new(1)?;
        let exhausted_opener = Arc::new(TransientOpener {
            failure,
            remaining_failures: AtomicUsize::new(retry_budget + 1),
            calls: AtomicUsize::new(0),
        });
        let exhausted = exhausted_harness.files(1, exhausted_opener.clone())?;
        let error = exhausted.handle(0).unwrap_err();
        assert_eq!(error.kind, StorageErrorKind::ResourceExhausted);
        assert_eq!(error.retry_advice, RetryAdvice::RetrySameInstance);
        assert_eq!(
            exhausted_opener.calls.load(Ordering::SeqCst),
            retry_budget + 1
        );
        assert_eq!(exhausted.cache_len()?, 0);
    }
    Ok(())
}

#[test]
fn catalog_identity_and_file_header_are_revalidated_before_cache_insert() -> TestResult {
    let harness = CacheHarness::new(1)?;
    let original = harness.directory.path().join("D000000.data");
    let displaced = harness.directory.path().join("displaced.data");
    std::fs::rename(&original, &displaced)?;
    let replacement = harness.directory.create_new_for_test(0)?;
    replacement.write_all_at(
        &PageHeader {
            file_id: 0,
            page_no: 0,
        }
        .encode()?,
        0,
    )?;
    replacement.write_all_at(&VLogFileHeader::new(database_uuid(), 0).encode()?, 16)?;

    let files = harness.files(1, Arc::new(CountingOpener::default()))?;
    let error = files.handle(0).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.vlog_file_id, Some(0));
    assert_eq!(error.vlog_offset, None);
    assert_eq!(files.cache_len()?, 0);

    let header_harness = CacheHarness::new(1)?;
    let same_file = header_harness.directory.open_writable_for_test(0)?;
    same_file.write_all_at(b"X", 16)?;
    let files = header_harness.files(1, Arc::new(CountingOpener::default()))?;
    let error = files.handle(0).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Corruption);
    assert_eq!(error.vlog_file_id, Some(0));
    assert_eq!(error.vlog_offset, Some(16));
    assert_eq!(files.cache_len()?, 0);
    Ok(())
}
