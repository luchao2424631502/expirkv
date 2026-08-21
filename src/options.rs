//! Database, read, and write options.

use crate::Snapshot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    NoCompression,
    Lz4,
}

pub struct Options {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub write_buffer_size: usize,
    pub max_open_files: usize,
    pub block_cache_size: usize,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub max_file_size: usize,
    pub compression: Compression,
    pub vlog_read_handle_cache_capacity: usize,
}

// RAII construct
impl Default for Options {
    fn default() -> Self {
        Self {
            create_if_missing: false,
            error_if_exists: false,
            // 4MB MemTable
            write_buffer_size: 4 * 1024 * 1024,
            max_open_files: 1000,
            // 8MB LRU Block
            block_cache_size: 8 * 1024 * 1024,
            // 4KB Block
            block_size: 4 * 1024,
            block_restart_interval: 16,
            // 2MB SSTable file
            max_file_size: 2 * 1024 * 1024,
            compression: Compression::NoCompression,
            vlog_read_handle_cache_capacity: 64,
        }
    }
}

pub struct WriteOptions {
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { sync: false }
    }
}

pub struct ReadOptions<'a> {
    pub snapshot: Option<&'a Snapshot>,
}

impl Default for ReadOptions<'_> {
    fn default() -> Self {
        Self { snapshot: None }
    }
}
