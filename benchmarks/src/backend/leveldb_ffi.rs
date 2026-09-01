//! Raw official LevelDB C API and the two benchmark-only C aggregates.

#![allow(non_camel_case_types)]

use std::ffi::{c_char, c_int, c_void};

#[repr(C)]
pub(super) struct leveldb_t {
    _private: [u8; 0],
}

#[repr(C)]
pub(super) struct leveldb_options_t {
    _private: [u8; 0],
}

#[repr(C)]
pub(super) struct leveldb_readoptions_t {
    _private: [u8; 0],
}

#[repr(C)]
pub(super) struct leveldb_writeoptions_t {
    _private: [u8; 0],
}

#[repr(C)]
pub(super) struct leveldb_cache_t {
    _private: [u8; 0],
}

#[repr(C)]
pub(super) struct AggregateBatchItem {
    pub kind: u8,
    pub key: *const c_char,
    pub key_length: usize,
    pub value: *const c_char,
    pub value_length: usize,
}

#[repr(C)]
pub(super) struct AggregateExpectedRecord {
    pub key: *const c_char,
    pub key_length: usize,
    pub value: *const c_char,
    pub value_length: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct AggregateScanResult {
    pub record_count: usize,
    pub value_bytes: usize,
}

pub(super) const BATCH_PUT: u8 = 1;
pub(super) const BATCH_DELETE: u8 = 2;
pub(super) const SCAN_TIMED: u8 = 0;
pub(super) const SCAN_FULL: u8 = 1;

unsafe extern "C" {
    pub(super) fn leveldb_major_version() -> c_int;
    pub(super) fn leveldb_minor_version() -> c_int;
    pub(super) fn leveldb_open(
        options: *const leveldb_options_t,
        name: *const c_char,
        error: *mut *mut c_char,
    ) -> *mut leveldb_t;
    pub(super) fn leveldb_close(db: *mut leveldb_t);
    pub(super) fn leveldb_put(
        db: *mut leveldb_t,
        options: *const leveldb_writeoptions_t,
        key: *const c_char,
        key_length: usize,
        value: *const c_char,
        value_length: usize,
        error: *mut *mut c_char,
    );
    pub(super) fn leveldb_delete(
        db: *mut leveldb_t,
        options: *const leveldb_writeoptions_t,
        key: *const c_char,
        key_length: usize,
        error: *mut *mut c_char,
    );
    pub(super) fn leveldb_get(
        db: *mut leveldb_t,
        options: *const leveldb_readoptions_t,
        key: *const c_char,
        key_length: usize,
        value_length: *mut usize,
        error: *mut *mut c_char,
    ) -> *mut c_char;

    pub(super) fn leveldb_options_create() -> *mut leveldb_options_t;
    pub(super) fn leveldb_options_destroy(options: *mut leveldb_options_t);
    pub(super) fn leveldb_options_set_create_if_missing(options: *mut leveldb_options_t, value: u8);
    pub(super) fn leveldb_options_set_error_if_exists(options: *mut leveldb_options_t, value: u8);
    pub(super) fn leveldb_options_set_write_buffer_size(
        options: *mut leveldb_options_t,
        value: usize,
    );
    pub(super) fn leveldb_options_set_max_open_files(options: *mut leveldb_options_t, value: c_int);
    pub(super) fn leveldb_options_set_cache(
        options: *mut leveldb_options_t,
        cache: *mut leveldb_cache_t,
    );
    pub(super) fn leveldb_options_set_block_size(options: *mut leveldb_options_t, value: usize);
    pub(super) fn leveldb_options_set_block_restart_interval(
        options: *mut leveldb_options_t,
        value: c_int,
    );
    pub(super) fn leveldb_options_set_max_file_size(options: *mut leveldb_options_t, value: usize);
    pub(super) fn leveldb_options_set_compression(options: *mut leveldb_options_t, value: c_int);
    pub(super) fn leveldb_readoptions_create() -> *mut leveldb_readoptions_t;
    pub(super) fn leveldb_readoptions_destroy(options: *mut leveldb_readoptions_t);
    pub(super) fn leveldb_writeoptions_create() -> *mut leveldb_writeoptions_t;
    pub(super) fn leveldb_writeoptions_destroy(options: *mut leveldb_writeoptions_t);
    pub(super) fn leveldb_writeoptions_set_sync(options: *mut leveldb_writeoptions_t, value: u8);
    pub(super) fn leveldb_cache_create_lru(capacity: usize) -> *mut leveldb_cache_t;
    pub(super) fn leveldb_cache_destroy(cache: *mut leveldb_cache_t);
    pub(super) fn leveldb_free(pointer: *mut c_void);

    pub(super) fn bench_leveldb_write_batch(
        db: *mut leveldb_t,
        options: *const leveldb_writeoptions_t,
        items: *const AggregateBatchItem,
        item_count: usize,
        error: *mut *mut c_char,
    );
    pub(super) fn bench_leveldb_iterator_scan(
        db: *mut leveldb_t,
        options: *const leveldb_readoptions_t,
        start: *const c_char,
        start_length: usize,
        limit: usize,
        validation_mode: u8,
        expected_value_length: usize,
        expected: *const AggregateExpectedRecord,
        expected_count: usize,
        result: *mut AggregateScanResult,
        error: *mut *mut c_char,
    );
}

/// Returns the version reported by the linked official LevelDB C API.
pub fn linked_leveldb_version() -> (i32, i32) {
    // SAFETY: build.rs validates that these functions come from LevelDB 1.23.
    unsafe { (leveldb_major_version(), leveldb_minor_version()) }
}
