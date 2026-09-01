//! Frozen benchmark configuration and explicitly non-formal smoke configuration.

/// The provenance mode carried by every benchmark configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchMode {
    Formal,
    Smoke,
}

/// Complete configuration shared by trace generation and both backends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchConfig {
    mode: BenchMode,
    record_count: u64,
    key_length: usize,
    value_length: usize,
    range_length: u64,
    batch_size: u64,
    sync_writes: bool,
    compression_enabled: bool,
    seed: u64,
    repetitions: u32,
    thread_counts: [usize; 4],
    random_get_operations: u64,
    range_scan_operations: u64,
    write_buffer_size: usize,
    block_cache_size: usize,
    block_size: usize,
    block_restart_interval: usize,
    max_open_files: usize,
    max_table_file_size: usize,
}

impl BenchConfig {
    /// Returns the only configuration permitted to produce formal results.
    pub const fn formal() -> Self {
        Self {
            mode: BenchMode::Formal,
            record_count: 10_000_000,
            key_length: 16,
            value_length: 1_024,
            range_length: 100,
            batch_size: 100,
            sync_writes: false,
            compression_enabled: false,
            seed: 20_260_720,
            repetitions: 5,
            thread_counts: [1, 10, 100, 1_000],
            random_get_operations: 10_000_000,
            range_scan_operations: 1_000_000,
            write_buffer_size: 4 * 1_024 * 1_024,
            block_cache_size: 8 * 1_024 * 1_024,
            block_size: 4 * 1_024,
            block_restart_interval: 16,
            max_open_files: 1_000,
            max_table_file_size: 2 * 1_024 * 1_024,
        }
    }

    /// Creates an explicitly non-formal small configuration for tests and the
    /// later smoke command. Formal output code must reject `BenchMode::Smoke`.
    #[doc(hidden)]
    pub fn test_only(
        record_count: u64,
        range_length: u64,
        batch_size: u64,
        random_get_operations: u64,
        range_scan_operations: u64,
    ) -> Self {
        assert!(record_count > 0, "test record count must be non-zero");
        assert!(
            range_length > 0 && range_length <= record_count,
            "test range length must fit the record set"
        );
        assert!(batch_size > 0, "test batch size must be non-zero");
        assert_eq!(
            record_count % batch_size,
            0,
            "test record count must be divisible by batch size"
        );

        Self {
            mode: BenchMode::Smoke,
            record_count,
            range_length,
            batch_size,
            random_get_operations,
            range_scan_operations,
            ..Self::formal()
        }
    }

    pub const fn mode(&self) -> BenchMode {
        self.mode
    }

    pub const fn is_formal(&self) -> bool {
        matches!(self.mode, BenchMode::Formal)
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub const fn key_length(&self) -> usize {
        self.key_length
    }

    pub const fn value_length(&self) -> usize {
        self.value_length
    }

    pub const fn range_length(&self) -> u64 {
        self.range_length
    }

    pub const fn batch_size(&self) -> u64 {
        self.batch_size
    }

    pub const fn sync_writes(&self) -> bool {
        self.sync_writes
    }

    pub const fn compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    pub const fn seed(&self) -> u64 {
        self.seed
    }

    pub const fn repetitions(&self) -> u32 {
        self.repetitions
    }

    pub const fn thread_counts(&self) -> &[usize; 4] {
        &self.thread_counts
    }

    pub const fn random_get_operations(&self) -> u64 {
        self.random_get_operations
    }

    pub const fn range_scan_operations(&self) -> u64 {
        self.range_scan_operations
    }

    pub const fn write_buffer_size(&self) -> usize {
        self.write_buffer_size
    }

    pub const fn block_cache_size(&self) -> usize {
        self.block_cache_size
    }

    pub const fn block_size(&self) -> usize {
        self.block_size
    }

    pub const fn block_restart_interval(&self) -> usize {
        self.block_restart_interval
    }

    pub const fn max_open_files(&self) -> usize {
        self.max_open_files
    }

    pub const fn max_table_file_size(&self) -> usize {
        self.max_table_file_size
    }
}
