//! Shared entry point for the repository-local RustKV/LevelDB benchmark.
//!
//! The staged implementation exposes the fixed configuration, trace, Backend,
//! execution, validation, matrix, CSV, and report components used by `kv_bench`.

mod backend;
mod cli;
mod config;
mod csv;
mod fs;
mod key;
mod matrix;
mod metrics;
mod report;
mod rng;
mod run_unit;
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
pub use cli::{CliCommand, CliError, execute_cli, parse_cli};
pub use config::{BenchConfig, BenchMode};
pub use csv::{CSV_COLUMNS, CsvError, CsvFile, CsvRow, ResumeIdentity, require_exact_matrix};
pub use fs::{BenchmarkWorkspace, DatabaseDirectory, FsError, FsErrorKind};
pub use key::{KEY_LENGTH, KeyCodecError, decode_key, encode_key, fixed_value};
pub use matrix::{
    CONFIG_VERSION, ExecutionMetadata, LEVELDB_COMMIT, MatrixError, MatrixExecutionError, RunId,
    RunUnit, execute_units, formal_matrix, mode_as_str, parse_mode, smoke_matrix, validate_run_id,
};
pub use metrics::{LatencySummary, MetricsError, RunMetrics, calculate_run_metrics};
pub use report::{
    ReportError, ReportSummary, SummaryRow, generate_formal_report, generate_smoke_report,
    summarize_formal, summarize_smoke,
};
pub use rng::{SplitMix64, deterministic_permutation, mix64};
#[doc(hidden)]
pub use run_unit::{
    RunUnitAttempt, RunUnitAudit, RunUnitExecutionError, RunUnitFault, RunUnitStage,
    execute_run_unit_with_fault,
};
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

/// Stable closed command-line surface. Formal sizing has no CLI overrides.
pub fn help_text() -> &'static str {
    concat!(
        "kv_bench ",
        env!("CARGO_PKG_VERSION"),
        "\n",
        "RustKV/LevelDB benchmark driver\n\n",
        "Usage:\n",
        "  kv_bench run-one --workspace ABS_PATH --csv ABS_PATH --backend rustkv|leveldb \\\n       --workload NAME --threads 1|10|100|1000 --repetition 0..4 \\\n       --rustkv-commit FULL_SHA --environment-id ID\n",
        "  kv_bench matrix --dry-run\n",
        "  kv_bench matrix --workspace ABS_PATH --csv ABS_PATH --rustkv-commit FULL_SHA \\\n       --environment-id ID [--resume]\n",
        "  kv_bench report --csv ABS_PATH --output-dir ABS_PATH\n",
        "  kv_bench smoke --output-dir ABS_PATH\n",
        "  kv_bench --help\n",
        "  kv_bench --version\n\n",
        "Formal configuration is compiled in and cannot be overridden.\n",
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
