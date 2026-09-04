//! One caller-selected, explicitly non-formal RunUnit.
//!
//! The custom command reuses the same direct Load -> close -> reopen and
//! validate -> close -> Run -> reopen and validate lifecycle as formal runs.
//! Its separate result schema cannot be consumed by the B7 formal report.

use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::run_unit::execute_run_unit;
use crate::{
    BackendKind, BenchConfig, BenchmarkWorkspace, LEVELDB_COMMIT, RunResult, RunUnit, Trace,
    Workload,
};

const RANGE_LENGTH: u64 = 100;
const BATCH_SIZE: u64 = 100;

pub const CUSTOM_CSV_COLUMNS: [&str; 25] = [
    "mode",
    "run_id",
    "record_count",
    "value_bytes",
    "range_length",
    "batch_size",
    "seed",
    "backend",
    "workload",
    "threads",
    "completed_ops",
    "completed_records",
    "wall_seconds",
    "ops_per_second",
    "records_per_second",
    "mean_latency_us",
    "p50_latency_us",
    "p95_latency_us",
    "p99_latency_us",
    "error_count",
    "validation_success",
    "error_text",
    "rustkv_commit",
    "rustkv_worktree",
    "leveldb_commit",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomRunSpec {
    pub output_directory: PathBuf,
    pub record_count: u64,
    pub backend: BackendKind,
    pub workload: Workload,
    pub thread_count: usize,
    pub rustkv_commit: String,
    pub worktree_state: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CustomRunRow {
    pub run_id: String,
    pub record_count: u64,
    pub backend: BackendKind,
    pub workload: Workload,
    pub thread_count: usize,
    pub completed_ops: u64,
    pub completed_records: u64,
    pub wall_seconds: Option<f64>,
    pub ops_per_second: Option<f64>,
    pub records_per_second: Option<f64>,
    pub mean_latency_us: Option<f64>,
    pub p50_latency_us: Option<f64>,
    pub p95_latency_us: Option<f64>,
    pub p99_latency_us: Option<f64>,
    pub error_count: usize,
    pub validation_success: bool,
    pub error_text: String,
    pub rustkv_commit: String,
    pub worktree_state: String,
}

impl CustomRunRow {
    pub fn is_effective(&self) -> bool {
        self.error_count == 0
            && self.validation_success
            && self.error_text.is_empty()
            && self.completed_ops > 0
            && self.wall_seconds.is_some_and(|value| value > 0.0)
            && self.ops_per_second.is_some_and(|value| value > 0.0)
            && self.mean_latency_us.is_some()
            && self.p50_latency_us.is_some()
            && self.p95_latency_us.is_some()
            && self.p99_latency_us.is_some()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomRunOutcome {
    pub csv_path: PathBuf,
    pub summary_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CustomRunError {
    Invalid(String),
    Runtime(String),
    RunFailed { run_id: String, csv_path: PathBuf },
}

impl CustomRunError {
    fn runtime(context: &str, error: impl fmt::Display) -> Self {
        Self::Runtime(format!("{context}: {error}"))
    }
}

impl fmt::Display for CustomRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid custom run: {message}"),
            Self::Runtime(message) => write!(formatter, "custom run failed: {message}"),
            Self::RunFailed { run_id, csv_path } => write!(
                formatter,
                "custom RunUnit {run_id} failed; result retained in {}",
                csv_path.display()
            ),
        }
    }
}

impl Error for CustomRunError {}

pub fn execute_custom_run(spec: &CustomRunSpec) -> Result<CustomRunOutcome, CustomRunError> {
    validate_spec(spec)?;
    if spec.output_directory.exists() {
        return Err(CustomRunError::Runtime(format!(
            "output directory already exists: {}",
            spec.output_directory.display()
        )));
    }
    fs::create_dir(&spec.output_directory)
        .map_err(|error| CustomRunError::runtime("create output directory", error))?;
    let output_directory = fs::canonicalize(&spec.output_directory)
        .map_err(|error| CustomRunError::runtime("canonicalize output directory", error))?;
    let csv_path = output_directory.join("result.csv");
    let summary_path = output_directory.join("result.md");
    publish_atomic(
        &output_directory.join("parameters.txt"),
        &render_parameters(spec),
    )?;
    publish_csv(&csv_path, None)?;

    let config = BenchConfig::custom(spec.record_count);
    let unit = RunUnit::custom(spec.backend, spec.workload, spec.thread_count)
        .map_err(|error| CustomRunError::Invalid(error.to_string()))?;
    let trace = Trace::generate(&config, spec.workload, 0)
        .map_err(|error| CustomRunError::runtime("generate custom Trace", format!("{error:?}")))?;
    let workspace_path = output_directory.join("workspace");
    let workspace = BenchmarkWorkspace::create(&workspace_path)
        .map_err(|error| CustomRunError::runtime("create custom workspace", error))?;

    eprintln!(
        "start custom RunUnit: backend={} workload={} threads={} records={}",
        spec.backend.as_str(),
        spec.workload.as_str(),
        spec.thread_count,
        spec.record_count
    );
    let attempt = execute_run_unit(&workspace, &config, unit, &trace)
        .map_err(|error| CustomRunError::runtime("execute custom RunUnit", error))?;
    let row = CustomRunRow::from_attempt(spec, attempt.result(), &attempt);
    let effective = row.is_effective();
    let run_id = row.run_id.clone();
    publish_csv(&csv_path, Some(&row))?;
    attempt
        .cleanup(&workspace)
        .map_err(|error| CustomRunError::runtime("cleanup custom RunUnit", error))?;
    drop(workspace);
    fs::remove_dir(&workspace_path)
        .map_err(|error| CustomRunError::runtime("remove empty custom workspace", error))?;
    if !effective {
        return Err(CustomRunError::RunFailed { run_id, csv_path });
    }
    publish_atomic(&summary_path, &render_summary(spec, &row))?;
    sync_directory(&output_directory)?;
    eprintln!(
        "done custom RunUnit: run_id={} ops/s={:.3}",
        row.run_id,
        row.ops_per_second.expect("effective row has ops/s")
    );
    Ok(CustomRunOutcome {
        csv_path,
        summary_path,
    })
}

impl CustomRunRow {
    fn from_attempt(
        spec: &CustomRunSpec,
        result: Option<&RunResult>,
        attempt: &crate::RunUnitAttempt,
    ) -> Self {
        let metrics = result.and_then(|result| result.metrics.as_ref());
        let valid_run = result.is_some_and(RunResult::is_valid);
        let validation_success = attempt.validation_success();
        let effective = valid_run && validation_success;
        let error_text = attempt
            .error_text()
            .map(str::to_owned)
            .or_else(|| {
                result
                    .and_then(|result| result.first_error.as_ref())
                    .map(|error| format!("{error:?}"))
            })
            .unwrap_or_default();
        Self {
            run_id: format!(
                "custom-n{}-{}-{}-t{}",
                spec.record_count,
                spec.backend.as_str(),
                spec.workload.as_str(),
                spec.thread_count
            ),
            record_count: spec.record_count,
            backend: spec.backend,
            workload: spec.workload,
            thread_count: spec.thread_count,
            completed_ops: result.map_or(0, |result| result.completed_ops),
            completed_records: result.map_or(0, |result| result.completed_records),
            wall_seconds: metrics.map(|metrics| metrics.elapsed_seconds()),
            ops_per_second: metrics.map(|metrics| metrics.ops_per_second()),
            records_per_second: metrics.and_then(|metrics| metrics.records_per_second()),
            mean_latency_us: metrics.map(|metrics| metrics.latency().mean_us()),
            p50_latency_us: metrics.map(|metrics| metrics.latency().p50_us()),
            p95_latency_us: metrics.map(|metrics| metrics.latency().p95_us()),
            p99_latency_us: metrics.map(|metrics| metrics.latency().p99_us()),
            error_count: if effective {
                0
            } else {
                result.map_or(1, |result| result.error_count.max(1))
            },
            validation_success,
            error_text,
            rustkv_commit: spec.rustkv_commit.clone(),
            worktree_state: spec.worktree_state.clone(),
        }
    }
}

fn validate_spec(spec: &CustomRunSpec) -> Result<(), CustomRunError> {
    if !spec.output_directory.is_absolute() || spec.output_directory.file_name().is_none() {
        return Err(CustomRunError::Invalid(
            "output directory must be an absolute non-root path".to_owned(),
        ));
    }
    if spec.record_count < RANGE_LENGTH || !spec.record_count.is_multiple_of(BATCH_SIZE) {
        return Err(CustomRunError::Invalid(
            "record count must be at least 100 and divisible by 100".to_owned(),
        ));
    }
    if !BenchConfig::formal()
        .thread_counts()
        .contains(&spec.thread_count)
    {
        return Err(CustomRunError::Invalid(
            "thread count must be one of 1, 10, 100, 1000".to_owned(),
        ));
    }
    let operation_count = spec
        .workload
        .operation_count(&BenchConfig::custom(spec.record_count));
    if operation_count < spec.thread_count as u64 {
        return Err(CustomRunError::Invalid(format!(
            "workload has only {operation_count} requests for {} threads; increase --records so every thread receives work",
            spec.thread_count
        )));
    }
    if !is_full_commit(&spec.rustkv_commit) {
        return Err(CustomRunError::Invalid(
            "RustKV commit must be a full lowercase hexadecimal SHA".to_owned(),
        ));
    }
    if !matches!(spec.worktree_state.as_str(), "clean" | "dirty") {
        return Err(CustomRunError::Invalid(
            "worktree state must be clean or dirty".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn is_full_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn render_parameters(spec: &CustomRunSpec) -> String {
    format!(
        concat!(
            "mode=custom\n",
            "formal_result=false\n",
            "record_count={}\n",
            "value_bytes=1024\n",
            "range_length=100\n",
            "batch_size=100\n",
            "seed=20260720\n",
            "backend={}\n",
            "workload={}\n",
            "threads={}\n",
            "rustkv_commit={}\n",
            "rustkv_worktree={}\n",
            "leveldb_commit={}\n",
        ),
        spec.record_count,
        spec.backend.as_str(),
        spec.workload.as_str(),
        spec.thread_count,
        spec.rustkv_commit,
        spec.worktree_state,
        LEVELDB_COMMIT
    )
}

fn publish_csv(path: &Path, row: Option<&CustomRunRow>) -> Result<(), CustomRunError> {
    let mut contents = CUSTOM_CSV_COLUMNS.join(",");
    contents.push_str("\r\n");
    if let Some(row) = row {
        let fields = [
            "custom".to_owned(),
            row.run_id.clone(),
            row.record_count.to_string(),
            "1024".to_owned(),
            RANGE_LENGTH.to_string(),
            BATCH_SIZE.to_string(),
            BenchConfig::formal().seed().to_string(),
            row.backend.as_str().to_owned(),
            row.workload.as_str().to_owned(),
            row.thread_count.to_string(),
            row.completed_ops.to_string(),
            row.completed_records.to_string(),
            optional_float(row.wall_seconds),
            optional_float(row.ops_per_second),
            optional_float(row.records_per_second),
            optional_float(row.mean_latency_us),
            optional_float(row.p50_latency_us),
            optional_float(row.p95_latency_us),
            optional_float(row.p99_latency_us),
            row.error_count.to_string(),
            row.validation_success.to_string(),
            row.error_text.clone(),
            row.rustkv_commit.clone(),
            row.worktree_state.clone(),
            LEVELDB_COMMIT.to_owned(),
        ];
        contents.push_str(
            &fields
                .iter()
                .map(|field| escape_csv(field))
                .collect::<Vec<_>>()
                .join(","),
        );
        contents.push_str("\r\n");
    }
    publish_atomic(path, &contents)
}

fn render_summary(spec: &CustomRunSpec, row: &CustomRunRow) -> String {
    format!(
        concat!(
            "# KV Benchmark单项自定义结果\n\n",
            "> 本结果使用自定义条目数，不是B7正式性能结果，不得合并到正式raw.csv。\n\n",
            "| 参数 | 值 |\n",
            "|---|---|\n",
            "| Backend | {} |\n",
            "| Workload | {} |\n",
            "| Threads | {} |\n",
            "| Records | {} |\n",
            "| Completed ops | {} |\n",
            "| Completed records | {} |\n",
            "| Wall seconds | {} |\n",
            "| ops/s | {} |\n",
            "| records/s | {} |\n",
            "| Mean us/request | {} |\n",
            "| P50 us/request | {} |\n",
            "| P95 us/request | {} |\n",
            "| P99 us/request | {} |\n",
            "| Validation | {} |\n",
        ),
        spec.backend.as_str(),
        spec.workload.as_str(),
        spec.thread_count,
        spec.record_count,
        row.completed_ops,
        row.completed_records,
        display_float(row.wall_seconds),
        display_float(row.ops_per_second),
        display_float(row.records_per_second),
        display_float(row.mean_latency_us),
        display_float(row.p50_latency_us),
        display_float(row.p95_latency_us),
        display_float(row.p99_latency_us),
        row.validation_success,
    )
}

fn publish_atomic(path: &Path, contents: &str) -> Result<(), CustomRunError> {
    let parent = path
        .parent()
        .ok_or_else(|| CustomRunError::Runtime("output file has no parent".to_owned()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| CustomRunError::Runtime("output file has no name".to_owned()))?;
    let checkpoint = parent.join(format!(".{}.checkpoint", file_name.to_string_lossy()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&checkpoint)
        .map_err(|error| CustomRunError::runtime("create output checkpoint", error))?;
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|error| CustomRunError::runtime("write output checkpoint", error))?;
    fs::rename(&checkpoint, path)
        .map_err(|error| CustomRunError::runtime("publish output file", error))?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), CustomRunError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CustomRunError::runtime("sync output directory", error))
}

fn optional_float(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn display_float(value: Option<f64>) -> String {
    value.map_or_else(|| "—".to_owned(), |value| format!("{value:.3}"))
}

fn escape_csv(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}
