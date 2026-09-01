//! Direct official LevelDB C-API backend.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path;
use std::ptr;

use crate::BenchConfig;

use super::leveldb_ffi::{
    AggregateBatchItem, AggregateExpectedRecord, AggregateScanResult, BATCH_DELETE, BATCH_PUT,
    SCAN_FULL, SCAN_TIMED, bench_leveldb_iterator_scan, bench_leveldb_write_batch,
    leveldb_cache_create_lru, leveldb_cache_destroy, leveldb_cache_t, leveldb_close,
    leveldb_delete, leveldb_free, leveldb_get, leveldb_open, leveldb_options_create,
    leveldb_options_destroy, leveldb_options_set_block_restart_interval,
    leveldb_options_set_block_size, leveldb_options_set_cache, leveldb_options_set_compression,
    leveldb_options_set_create_if_missing, leveldb_options_set_error_if_exists,
    leveldb_options_set_max_file_size, leveldb_options_set_max_open_files,
    leveldb_options_set_write_buffer_size, leveldb_options_t, leveldb_put,
    leveldb_readoptions_create, leveldb_readoptions_destroy, leveldb_readoptions_t, leveldb_t,
    leveldb_writeoptions_create, leveldb_writeoptions_destroy, leveldb_writeoptions_set_sync,
    leveldb_writeoptions_t,
};
use super::{
    BackendError, BackendKind, BackendOperation, BackendResult, BatchItem, BenchBackend, GetResult,
    ScanRequest, ScanResult, ScanValidation,
};

pub struct LevelDbBackend {
    handle: LevelDbHandle,
}

impl LevelDbBackend {
    pub fn open(path: impl AsRef<Path>, config: &BenchConfig) -> BackendResult<Self> {
        let path = CString::new(path.as_ref().as_os_str().as_encoded_bytes()).map_err(|_| {
            BackendError::new(
                BackendKind::LevelDb,
                BackendOperation::Open,
                "database path contains an interior NUL byte",
            )
        })?;
        let max_open_files = c_int::try_from(config.max_open_files()).map_err(|_| {
            BackendError::new(
                BackendKind::LevelDb,
                BackendOperation::Open,
                "max_open_files does not fit LevelDB's int option",
            )
        })?;
        let restart_interval = c_int::try_from(config.block_restart_interval()).map_err(|_| {
            BackendError::new(
                BackendKind::LevelDb,
                BackendOperation::Open,
                "block_restart_interval does not fit LevelDB's int option",
            )
        })?;

        let mut handle = LevelDbHandle::empty();
        // SAFETY: Every returned object is immediately stored in the unique
        // handle and is released by Drop on every early return.
        unsafe {
            handle.options = leveldb_options_create();
            require_pointer(handle.options, "leveldb_options_create returned null")?;
            handle.cache = leveldb_cache_create_lru(config.block_cache_size());
            require_pointer(handle.cache, "leveldb_cache_create_lru returned null")?;
            handle.read_options = leveldb_readoptions_create();
            require_pointer(
                handle.read_options,
                "leveldb_readoptions_create returned null",
            )?;
            handle.write_options = leveldb_writeoptions_create();
            require_pointer(
                handle.write_options,
                "leveldb_writeoptions_create returned null",
            )?;

            leveldb_options_set_create_if_missing(handle.options, 1);
            leveldb_options_set_error_if_exists(handle.options, 0);
            leveldb_options_set_write_buffer_size(handle.options, config.write_buffer_size());
            leveldb_options_set_max_open_files(handle.options, max_open_files);
            leveldb_options_set_cache(handle.options, handle.cache);
            leveldb_options_set_block_size(handle.options, config.block_size());
            leveldb_options_set_block_restart_interval(handle.options, restart_interval);
            leveldb_options_set_max_file_size(handle.options, config.max_table_file_size());
            leveldb_options_set_compression(handle.options, 0);
            leveldb_writeoptions_set_sync(handle.write_options, u8::from(config.sync_writes()));

            let mut error = ptr::null_mut();
            handle.db = leveldb_open(handle.options, path.as_ptr(), &mut error);
            check_error(BackendOperation::Open, error)?;
            require_pointer(handle.db, "leveldb_open returned no database and no error")?;
        }
        Ok(Self { handle })
    }
}

impl BenchBackend for LevelDbBackend {
    fn get(&self, key: &[u8]) -> BackendResult<GetResult> {
        let mut value_length = 0_usize;
        let mut error = ptr::null_mut();
        // SAFETY: The handle is live and all buffers outlive this call.
        let value = unsafe {
            leveldb_get(
                self.handle.db,
                self.handle.read_options,
                key.as_ptr().cast(),
                key.len(),
                &mut value_length,
                &mut error,
            )
        };
        if !error.is_null() {
            if !value.is_null() {
                // SAFETY: A non-null Get result is allocated by LevelDB.
                unsafe { leveldb_free(value.cast::<c_void>()) };
            }
            return Err(leveldb_error(BackendOperation::Get, error));
        }
        let found = !value.is_null();
        if found {
            std::hint::black_box(value_length);
            // SAFETY: A non-null successful Get result is allocated by LevelDB.
            unsafe { leveldb_free(value.cast::<c_void>()) };
        }
        Ok(GetResult {
            found,
            value_length,
        })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BackendResult<()> {
        let mut error = ptr::null_mut();
        // SAFETY: All pointers remain valid for the synchronous C API call.
        unsafe {
            leveldb_put(
                self.handle.db,
                self.handle.write_options,
                key.as_ptr().cast(),
                key.len(),
                value.as_ptr().cast(),
                value.len(),
                &mut error,
            );
        }
        check_error(BackendOperation::Put, error)
    }

    fn delete(&self, key: &[u8]) -> BackendResult<()> {
        let mut error = ptr::null_mut();
        // SAFETY: All pointers remain valid for the synchronous C API call.
        unsafe {
            leveldb_delete(
                self.handle.db,
                self.handle.write_options,
                key.as_ptr().cast(),
                key.len(),
                &mut error,
            );
        }
        check_error(BackendOperation::Delete, error)
    }

    fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()> {
        let ffi_items: Vec<_> = items
            .iter()
            .map(|item| match item {
                BatchItem::Put { key, value } => AggregateBatchItem {
                    kind: BATCH_PUT,
                    key: key.as_ptr().cast(),
                    key_length: key.len(),
                    value: value.as_ptr().cast(),
                    value_length: value.len(),
                },
                BatchItem::Delete { key } => AggregateBatchItem {
                    kind: BATCH_DELETE,
                    key: key.as_ptr().cast(),
                    key_length: key.len(),
                    value: ptr::null(),
                    value_length: 0,
                },
            })
            .collect();
        let mut error = ptr::null_mut();
        // SAFETY: FFI descriptors borrow bytes alive for the synchronous call.
        unsafe {
            bench_leveldb_write_batch(
                self.handle.db,
                self.handle.write_options,
                ffi_items.as_ptr(),
                ffi_items.len(),
                &mut error,
            );
        }
        check_error(BackendOperation::WriteBatch, error)
    }

    fn iterator_scan(&self, request: ScanRequest<'_>) -> BackendResult<ScanResult> {
        let (validation_mode, expected_value_length, expected_records) = match request.validation {
            ScanValidation::Timed {
                expected_value_length,
            } => (SCAN_TIMED, expected_value_length, Vec::new()),
            ScanValidation::Full { expected } => (
                SCAN_FULL,
                0,
                expected
                    .iter()
                    .map(|record| AggregateExpectedRecord {
                        key: record.key.as_ptr().cast(),
                        key_length: record.key.len(),
                        value: record.value.as_ptr().cast(),
                        value_length: record.value.len(),
                    })
                    .collect(),
            ),
        };
        let mut result = AggregateScanResult::default();
        let mut error = ptr::null_mut();
        // SAFETY: Request/expected bytes remain live for the synchronous call.
        unsafe {
            bench_leveldb_iterator_scan(
                self.handle.db,
                self.handle.read_options,
                request.start.as_ptr().cast(),
                request.start.len(),
                request.limit,
                validation_mode,
                expected_value_length,
                expected_records.as_ptr(),
                expected_records.len(),
                &mut result,
                &mut error,
            );
        }
        check_error(BackendOperation::IteratorScan, error)?;
        Ok(ScanResult {
            record_count: result.record_count,
            value_bytes: result.value_bytes,
        })
    }
}

struct LevelDbHandle {
    db: *mut leveldb_t,
    options: *mut leveldb_options_t,
    read_options: *mut leveldb_readoptions_t,
    write_options: *mut leveldb_writeoptions_t,
    cache: *mut leveldb_cache_t,
}

impl LevelDbHandle {
    const fn empty() -> Self {
        Self {
            db: ptr::null_mut(),
            options: ptr::null_mut(),
            read_options: ptr::null_mut(),
            write_options: ptr::null_mut(),
            cache: ptr::null_mut(),
        }
    }
}

// LevelDB DB methods are thread-safe. Options are immutable after Open; each
// Batch and Iterator is call-local. This is the only raw-pointer owner, and it
// closes the DB only after all shared Backend references have been dropped.
unsafe impl Send for LevelDbHandle {}
unsafe impl Sync for LevelDbHandle {}

impl Drop for LevelDbHandle {
    fn drop(&mut self) {
        // SAFETY: Unique ownership guarantees one release of every object.
        unsafe {
            if !self.db.is_null() {
                leveldb_close(self.db);
            }
            if !self.read_options.is_null() {
                leveldb_readoptions_destroy(self.read_options);
            }
            if !self.write_options.is_null() {
                leveldb_writeoptions_destroy(self.write_options);
            }
            if !self.options.is_null() {
                leveldb_options_destroy(self.options);
            }
            if !self.cache.is_null() {
                leveldb_cache_destroy(self.cache);
            }
        }
    }
}

fn require_pointer<T>(pointer: *mut T, source: &str) -> BackendResult<()> {
    if pointer.is_null() {
        Err(BackendError::new(
            BackendKind::LevelDb,
            BackendOperation::Open,
            source,
        ))
    } else {
        Ok(())
    }
}

fn check_error(operation: BackendOperation, error: *mut c_char) -> BackendResult<()> {
    if error.is_null() {
        Ok(())
    } else {
        Err(leveldb_error(operation, error))
    }
}

fn leveldb_error(operation: BackendOperation, error: *mut c_char) -> BackendError {
    // SAFETY: C errors are NUL-terminated allocations compatible with free.
    let text = unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned();
    // SAFETY: The error allocation is consumed exactly once here.
    unsafe { leveldb_free(error.cast::<c_void>()) };
    BackendError::new(BackendKind::LevelDb, operation, text)
}
