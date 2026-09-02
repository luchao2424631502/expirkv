//! Shared entry point for the repository-local RustKV/LevelDB benchmark.
//!
//! Stage B0 intentionally exposes only build metadata. Backend operations,
//! workloads, traces, and statistics belong to later reviewed stages.

mod backend;
mod config;
mod fs;
mod key;
mod metrics;
mod rng;
mod runner;
mod template;
mod trace;
mod validation;
mod workload;

pub use backend::{
    BackendError, BackendKind, BackendOperation, BackendResult, BatchItem, BenchBackend,
    ExpectedRecord, GetResult, LevelDbBackend, RustKvBackend, ScanRequest, ScanResult,
    ScanValidation, linked_leveldb_version,
};
pub use config::{BenchConfig, BenchMode};
pub use fs::{BenchmarkWorkspace, DatabaseDirectory, FsError, FsErrorKind};
pub use key::{KEY_LENGTH, KeyCodecError, decode_key, encode_key, fixed_value};
pub use metrics::{LatencySummary, MetricsError, RunMetrics, calculate_run_metrics};
pub use rng::{SplitMix64, deterministic_permutation, mix64};
pub use runner::{RequestContext, RunError, RunResult, RunSpec, ThreadRunSummary, run_concurrent};
pub use template::{
    DatabaseTemplate, OpenRun, PreparedRun, TemplateBuildFault, TemplateError, TemplateErrorKind,
    TemplateOpenGuard, build_formal_template, build_test_template, build_test_template_with_fault,
    prepare_run,
};
pub use trace::{Trace, TraceError, TracePartition, Workload, derive_trace_seed};
pub use validation::{
    ValidationError, ValidationSummary, prewarm_full_dataset, validate_empty_dataset,
    validate_final_dataset, validate_full_dataset,
};
pub use workload::{WorkloadError, WorkloadRun, run_workload};

/// The pinned LevelDB version used by every benchmark build.
pub const EXPECTED_LEVELDB_VERSION: (i32, i32) = (1, 23);

/// Stable help output for the B0 command-line skeleton.
pub fn help_text() -> &'static str {
    concat!(
        "kv_bench ",
        env!("CARGO_PKG_VERSION"),
        "\n",
        "RustKV/LevelDB benchmark driver\n\n",
        "Usage:\n",
        "  kv_bench --help\n",
        "  kv_bench --version\n\n",
        "Benchmark commands are not implemented in stage B0.\n",
    )
}

/// Version text includes the dynamically queried LevelDB C API version so
/// `--version` also proves that the final binary is linked to the pinned lib.
pub fn version_text() -> String {
    let (major, minor) = linked_leveldb_version();
    format!(
        "kv_bench {} (LevelDB {major}.{minor})",
        env!("CARGO_PKG_VERSION")
    )
}
