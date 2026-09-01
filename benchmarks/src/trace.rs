//! Global backend-independent request traces and deterministic partitioning.

use std::fmt;

use crate::{BenchConfig, SplitMix64, deterministic_permutation, mix64};

const TRACE_SEED_DOMAIN: u64 = 0x6b76_5f74_7261_6365;
const TRACE_STREAM_GAMMA: u64 = 0xd1b5_4a32_d192_ed03;
const TRACE_REPEAT_GAMMA: u64 = 0x94d0_49bb_1331_11eb;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Workload {
    RandomGet,
    RangeScan,
    SinglePut,
    BatchPut,
    SingleDelete,
    BatchDelete,
}

impl Workload {
    pub const ALL: [Self; 6] = [
        Self::RandomGet,
        Self::RangeScan,
        Self::SinglePut,
        Self::BatchPut,
        Self::SingleDelete,
        Self::BatchDelete,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RandomGet => "random_get",
            Self::RangeScan => "range_scan",
            Self::SinglePut => "single_put",
            Self::BatchPut => "batch_put",
            Self::SingleDelete => "single_delete",
            Self::BatchDelete => "batch_delete",
        }
    }

    pub const fn operation_count(self, config: &BenchConfig) -> u64 {
        match self {
            Self::RandomGet => config.random_get_operations(),
            Self::RangeScan => config.range_scan_operations(),
            Self::SinglePut | Self::SingleDelete => config.record_count(),
            Self::BatchPut | Self::BatchDelete => config.record_count() / config.batch_size(),
        }
    }

    pub const fn records_per_operation(self, config: &BenchConfig) -> u64 {
        match self {
            Self::RangeScan => config.range_length(),
            Self::BatchPut | Self::BatchDelete => config.batch_size(),
            Self::RandomGet | Self::SinglePut | Self::SingleDelete => 1,
        }
    }

    const fn request_width(self, config: &BenchConfig) -> u64 {
        match self {
            Self::BatchPut | Self::BatchDelete => config.batch_size(),
            Self::RandomGet | Self::RangeScan | Self::SinglePut | Self::SingleDelete => 1,
        }
    }

    // Single and batch forms intentionally use the same logical permutation;
    // a batch trace only groups the corresponding single-operation order.
    const fn seed_stream(self) -> u64 {
        match self {
            Self::RandomGet => 0,
            Self::RangeScan => 1,
            Self::SinglePut | Self::BatchPut => 2,
            Self::SingleDelete | Self::BatchDelete => 3,
        }
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceError {
    RepetitionOutOfRange { repetition: u32, repetitions: u32 },
    SizeDoesNotFitUsize { value: u64 },
    RangeChoiceOverflow,
    ZeroThreads,
}

/// Fixed public seed derivation shared by both backends and every thread count.
pub const fn derive_trace_seed(base_seed: u64, workload: Workload, repetition: u32) -> u64 {
    let material = base_seed
        .wrapping_add(TRACE_SEED_DOMAIN)
        .wrapping_add(workload.seed_stream().wrapping_mul(TRACE_STREAM_GAMMA))
        .wrapping_add((repetition as u64).wrapping_mul(TRACE_REPEAT_GAMMA));
    mix64(material)
}

/// One global trace. Logical ids are flattened; `request_width` preserves the
/// request boundaries without allocating a request object per operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    generating_config: BenchConfig,
    workload: Workload,
    repetition: u32,
    seed: u64,
    records_per_operation: u64,
    request_width: usize,
    logical_ids: Vec<u64>,
}

impl Trace {
    pub fn generate(
        config: &BenchConfig,
        workload: Workload,
        repetition: u32,
    ) -> Result<Self, TraceError> {
        if repetition >= config.repetitions() {
            return Err(TraceError::RepetitionOutOfRange {
                repetition,
                repetitions: config.repetitions(),
            });
        }
        let seed = derive_trace_seed(config.seed(), workload, repetition);
        let records_per_operation = workload.records_per_operation(config);
        let request_width = to_usize(workload.request_width(config))?;
        let operation_count = to_usize(workload.operation_count(config))?;

        let logical_ids = match workload {
            Workload::RandomGet => {
                let mut random = SplitMix64::new(seed);
                (0..operation_count)
                    .map(|_| random.uniform_below(config.record_count()))
                    .collect()
            }
            Workload::RangeScan => {
                let max_start = config.record_count() - config.range_length();
                let choices = max_start
                    .checked_add(1)
                    .ok_or(TraceError::RangeChoiceOverflow)?;
                let mut random = SplitMix64::new(seed);
                (0..operation_count)
                    .map(|_| random.uniform_below(choices))
                    .collect()
            }
            Workload::SinglePut | Workload::BatchPut => {
                deterministic_permutation(to_usize(config.record_count())?, seed)
            }
            Workload::SingleDelete | Workload::BatchDelete => {
                deterministic_permutation(to_usize(config.record_count())?, seed)
            }
        };

        debug_assert_eq!(logical_ids.len(), operation_count * request_width);
        Ok(Self {
            generating_config: config.clone(),
            workload,
            repetition,
            seed,
            records_per_operation,
            request_width,
            logical_ids,
        })
    }

    pub const fn workload(&self) -> Workload {
        self.workload
    }

    /// The generating configuration is retained privately so a later
    /// WorkloadRun cannot relabel a smoke or smaller-domain Trace as formal.
    pub(crate) fn was_generated_from(&self, config: &BenchConfig) -> bool {
        &self.generating_config == config
    }

    pub const fn repetition(&self) -> u32 {
        self.repetition
    }

    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Number of logical records represented by one request in this Trace.
    /// This is captured from the generating configuration so later stages
    /// cannot silently report a caller-supplied, mismatched records/op value.
    pub const fn records_per_operation(&self) -> u64 {
        self.records_per_operation
    }

    pub const fn request_width(&self) -> usize {
        self.request_width
    }

    pub fn request_count(&self) -> usize {
        self.logical_ids.len() / self.request_width
    }

    pub fn logical_ids(&self) -> &[u64] {
        &self.logical_ids
    }

    pub fn request(&self, index: usize) -> Option<&[u64]> {
        let start = index.checked_mul(self.request_width)?;
        let end = start.checked_add(self.request_width)?;
        self.logical_ids.get(start..end)
    }

    pub fn requests(&self) -> std::slice::ChunksExact<'_, u64> {
        self.logical_ids.chunks_exact(self.request_width)
    }

    pub fn partition(&self, thread_count: usize) -> Result<Vec<TracePartition<'_>>, TraceError> {
        if thread_count == 0 {
            return Err(TraceError::ZeroThreads);
        }
        let total = self.request_count();
        let base = total / thread_count;
        let remainder = total % thread_count;
        let mut request_start = 0_usize;
        let mut partitions = Vec::with_capacity(thread_count);
        for thread_index in 0..thread_count {
            let request_count = base + usize::from(thread_index < remainder);
            let request_end = request_start + request_count;
            let logical_start = request_start * self.request_width;
            let logical_end = request_end * self.request_width;
            partitions.push(TracePartition {
                thread_index,
                request_start,
                request_width: self.request_width,
                logical_ids: &self.logical_ids[logical_start..logical_end],
            });
            request_start = request_end;
        }
        debug_assert_eq!(request_start, total);
        Ok(partitions)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TracePartition<'a> {
    thread_index: usize,
    request_start: usize,
    request_width: usize,
    logical_ids: &'a [u64],
}

impl<'a> TracePartition<'a> {
    pub const fn thread_index(&self) -> usize {
        self.thread_index
    }

    pub const fn request_start(&self) -> usize {
        self.request_start
    }

    pub fn request_count(&self) -> usize {
        self.logical_ids.len() / self.request_width
    }

    pub const fn request_width(&self) -> usize {
        self.request_width
    }

    pub const fn logical_ids(&self) -> &'a [u64] {
        self.logical_ids
    }

    pub fn requests(&self) -> std::slice::ChunksExact<'a, u64> {
        self.logical_ids.chunks_exact(self.request_width)
    }
}

fn to_usize(value: u64) -> Result<usize, TraceError> {
    usize::try_from(value).map_err(|_| TraceError::SizeDoesNotFitUsize { value })
}
