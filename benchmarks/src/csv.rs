//! Strict RFC 4180 raw-result persistence with whole-file atomic publication.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::matrix::{
    CONFIG_VERSION, LEVELDB_COMMIT, RunId, mode_as_str, parse_mode, validate_run_id,
};
use crate::{BackendKind, BenchConfig, BenchMode, RunResult, RunUnit, Workload};

pub const CSV_COLUMNS: [&str; 22] = [
    "mode",
    "config_version",
    "run_id",
    "backend",
    "workload",
    "threads",
    "repetition",
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
    "leveldb_commit",
    "environment_id",
];

#[derive(Clone, Debug, PartialEq)]
pub struct CsvRow {
    pub mode: BenchMode,
    pub config_version: String,
    pub run_id: RunId,
    pub backend: BackendKind,
    pub workload: Workload,
    pub thread_count: usize,
    pub repetition: u32,
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
    pub leveldb_commit: String,
    pub environment_id: String,
}

impl CsvRow {
    pub fn from_run(
        unit: RunUnit,
        result: Option<&RunResult>,
        validation_success: bool,
        additional_error: Option<&str>,
        rustkv_commit: &str,
        leveldb_commit: &str,
        environment_id: &str,
    ) -> Self {
        let mut errors = Vec::new();
        let mut error_count = 0;
        let (completed_ops, completed_records, wall_seconds, metrics) = match result {
            Some(result) => {
                error_count = result.error_count;
                if let Some(error) = &result.first_error {
                    errors.push(format!("{error:?}"));
                }
                (
                    result.completed_ops,
                    result.completed_records,
                    (!result.wall_time.is_zero()).then_some(result.wall_time.as_secs_f64()),
                    result.metrics.as_ref(),
                )
            }
            None => (0, 0, None, None),
        };
        if let Some(error) = additional_error {
            error_count = error_count.saturating_add(1);
            errors.push(error.to_owned());
        }
        if !validation_success && additional_error.is_none() {
            error_count = error_count.saturating_add(1);
            errors.push("terminal validation did not succeed".to_owned());
        }
        Self {
            mode: unit.mode,
            config_version: CONFIG_VERSION.to_owned(),
            run_id: unit.id(),
            backend: unit.backend,
            workload: unit.workload,
            thread_count: unit.thread_count,
            repetition: unit.repetition,
            completed_ops,
            completed_records,
            wall_seconds,
            ops_per_second: metrics.map(|metrics| metrics.ops_per_second()),
            records_per_second: metrics.and_then(|metrics| metrics.records_per_second()),
            mean_latency_us: metrics.map(|metrics| metrics.latency().mean_us()),
            p50_latency_us: metrics.map(|metrics| metrics.latency().p50_us()),
            p95_latency_us: metrics.map(|metrics| metrics.latency().p95_us()),
            p99_latency_us: metrics.map(|metrics| metrics.latency().p99_us()),
            error_count,
            validation_success,
            error_text: errors.join("; "),
            rustkv_commit: rustkv_commit.to_owned(),
            leveldb_commit: leveldb_commit.to_owned(),
            environment_id: environment_id.to_owned(),
        }
    }

    pub fn is_effective(&self) -> bool {
        self.validate().is_ok()
            && self.error_count == 0
            && self.validation_success
            && self.error_text.is_empty()
            && self.completed_ops > 0
            && self.wall_seconds.is_some()
            && self.ops_per_second.is_some()
            && self.mean_latency_us.is_some()
            && self.p50_latency_us.is_some()
            && self.p95_latency_us.is_some()
            && self.p99_latency_us.is_some()
    }

    pub fn validate(&self) -> Result<(), CsvError> {
        if self.config_version != CONFIG_VERSION {
            return Err(CsvError::InvalidRow(format!(
                "unsupported config version {}",
                self.config_version
            )));
        }
        validate_run_id(
            self.run_id.as_str(),
            self.mode,
            self.backend,
            self.workload,
            self.thread_count,
            self.repetition,
        )
        .map_err(|error| CsvError::InvalidRow(error.to_string()))?;
        if self.rustkv_commit.is_empty()
            || self.leveldb_commit.is_empty()
            || self.environment_id.is_empty()
        {
            return Err(CsvError::InvalidRow(
                "commit and environment fields must be non-empty".to_owned(),
            ));
        }
        let config = match self.mode {
            BenchMode::Formal => {
                let config = BenchConfig::formal();
                if !config.thread_counts().contains(&self.thread_count)
                    || self.repetition >= config.repetitions()
                {
                    return Err(CsvError::InvalidRow(
                        "formal threads or repetition are outside the fixed matrix".to_owned(),
                    ));
                }
                if self.leveldb_commit != LEVELDB_COMMIT
                    || self.rustkv_commit.len() != 40
                    || !self
                        .rustkv_commit
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    return Err(CsvError::InvalidRow(
                        "formal commit provenance is not the frozen full identity".to_owned(),
                    ));
                }
                config
            }
            BenchMode::Smoke => {
                if ![1, 10].contains(&self.thread_count) || self.repetition != 0 {
                    return Err(CsvError::InvalidRow(
                        "smoke rows only permit threads 1/10 and repetition 0".to_owned(),
                    ));
                }
                BenchConfig::test_only(1_000, 100, 100, 100, 20)
            }
        };
        for (name, value) in [
            ("wall_seconds", self.wall_seconds),
            ("ops_per_second", self.ops_per_second),
            ("records_per_second", self.records_per_second),
            ("mean_latency_us", self.mean_latency_us),
            ("p50_latency_us", self.p50_latency_us),
            ("p95_latency_us", self.p95_latency_us),
            ("p99_latency_us", self.p99_latency_us),
        ] {
            if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                return Err(CsvError::InvalidRow(format!(
                    "{name} must be finite and non-negative"
                )));
            }
        }
        let auxiliary_expected = matches!(
            self.workload,
            Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
        );
        if self.is_success_shape() {
            if self.wall_seconds.is_none_or(|value| value <= 0.0)
                || self.ops_per_second.is_none_or(|value| value <= 0.0)
                || self.completed_ops == 0
                || [
                    self.mean_latency_us,
                    self.p50_latency_us,
                    self.p95_latency_us,
                    self.p99_latency_us,
                ]
                .iter()
                .any(Option::is_none)
            {
                return Err(CsvError::InvalidRow(
                    "successful row has incomplete metrics".to_owned(),
                ));
            }
            if auxiliary_expected != self.records_per_second.is_some() {
                return Err(CsvError::InvalidRow(
                    "records/s presence does not match workload units".to_owned(),
                ));
            }
            let p50 = self.p50_latency_us.expect("success metrics checked above");
            let p95 = self.p95_latency_us.expect("success metrics checked above");
            let p99 = self.p99_latency_us.expect("success metrics checked above");
            if p50 > p95 || p95 > p99 {
                return Err(CsvError::InvalidRow(
                    "latency percentiles are not monotonically ordered".to_owned(),
                ));
            }
            let expected_ops = self.workload.operation_count(&config);
            let expected_records = expected_ops
                .checked_mul(self.workload.records_per_operation(&config))
                .ok_or_else(|| {
                    CsvError::InvalidRow("expected record count overflowed".to_owned())
                })?;
            if self.completed_ops != expected_ops || self.completed_records != expected_records {
                return Err(CsvError::InvalidRow(
                    "successful row does not contain the fixed completed work".to_owned(),
                ));
            }
            let wall = self.wall_seconds.expect("success metrics checked above");
            if !approximately_equal(
                self.ops_per_second.expect("success metrics checked above"),
                self.completed_ops as f64 / wall,
            ) || auxiliary_expected
                && !approximately_equal(
                    self.records_per_second
                        .expect("auxiliary metric checked above"),
                    self.completed_records as f64 / wall,
                )
            {
                return Err(CsvError::InvalidRow(
                    "throughput is inconsistent with completed work and wall time".to_owned(),
                ));
            }
        } else if self.error_count == 0 || self.error_text.is_empty() {
            return Err(CsvError::InvalidRow(
                "failed row must preserve a non-zero error count and error text".to_owned(),
            ));
        }
        Ok(())
    }

    fn is_success_shape(&self) -> bool {
        self.error_count == 0 && self.validation_success && self.error_text.is_empty()
    }

    fn fields(&self) -> Vec<String> {
        vec![
            mode_as_str(self.mode).to_owned(),
            self.config_version.clone(),
            self.run_id.to_string(),
            self.backend.as_str().to_owned(),
            self.workload.as_str().to_owned(),
            self.thread_count.to_string(),
            self.repetition.to_string(),
            self.completed_ops.to_string(),
            self.completed_records.to_string(),
            format_optional(self.wall_seconds),
            format_optional(self.ops_per_second),
            format_optional(self.records_per_second),
            format_optional(self.mean_latency_us),
            format_optional(self.p50_latency_us),
            format_optional(self.p95_latency_us),
            format_optional(self.p99_latency_us),
            self.error_count.to_string(),
            self.validation_success.to_string(),
            self.error_text.clone(),
            self.rustkv_commit.clone(),
            self.leveldb_commit.clone(),
            self.environment_id.clone(),
        ]
    }

    fn from_fields(fields: &[String], record_number: usize) -> Result<Self, CsvError> {
        if fields.len() != CSV_COLUMNS.len() {
            return Err(CsvError::Malformed(format!(
                "record {record_number} has {} fields, expected {}",
                fields.len(),
                CSV_COLUMNS.len()
            )));
        }
        let mode = parse_mode(&fields[0]).map_err(|error| parse_error(record_number, error))?;
        let backend = fields[3]
            .parse()
            .map_err(|error| parse_error(record_number, error))?;
        let workload = fields[4]
            .parse()
            .map_err(|error| parse_error(record_number, error))?;
        let thread_count = parse_number(&fields[5], record_number, "threads")?;
        let repetition = parse_number(&fields[6], record_number, "repetition")?;
        let row = Self {
            mode,
            config_version: fields[1].clone(),
            run_id: validate_run_id(
                &fields[2],
                mode,
                backend,
                workload,
                thread_count,
                repetition,
            )
            .map_err(|error| parse_error(record_number, error))?,
            backend,
            workload,
            thread_count,
            repetition,
            completed_ops: parse_number(&fields[7], record_number, "completed_ops")?,
            completed_records: parse_number(&fields[8], record_number, "completed_records")?,
            wall_seconds: parse_optional(&fields[9], record_number, "wall_seconds")?,
            ops_per_second: parse_optional(&fields[10], record_number, "ops_per_second")?,
            records_per_second: parse_optional(&fields[11], record_number, "records_per_second")?,
            mean_latency_us: parse_optional(&fields[12], record_number, "mean_latency_us")?,
            p50_latency_us: parse_optional(&fields[13], record_number, "p50_latency_us")?,
            p95_latency_us: parse_optional(&fields[14], record_number, "p95_latency_us")?,
            p99_latency_us: parse_optional(&fields[15], record_number, "p99_latency_us")?,
            error_count: parse_number(&fields[16], record_number, "error_count")?,
            validation_success: match fields[17].as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return Err(CsvError::Malformed(format!(
                        "record {record_number} has invalid validation_success"
                    )));
                }
            },
            error_text: fields[18].clone(),
            rustkv_commit: fields[19].clone(),
            leveldb_commit: fields[20].clone(),
            environment_id: fields[21].clone(),
        };
        row.validate()?;
        Ok(row)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumeIdentity<'a> {
    pub mode: BenchMode,
    pub rustkv_commit: &'a str,
    pub leveldb_commit: &'a str,
    pub environment_id: &'a str,
}

#[derive(Clone, Debug)]
pub struct CsvFile {
    path: PathBuf,
    rows: Vec<CsvRow>,
}

impl CsvFile {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, CsvError> {
        let path = checked_absolute_file(path.as_ref())?;
        if path.exists() {
            return Err(CsvError::AlreadyExists(path));
        }
        let file = Self {
            path,
            rows: Vec::new(),
        };
        file.publish()?;
        Ok(file)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, CsvError> {
        let path = checked_absolute_file(path.as_ref())?;
        let bytes = fs::read(&path).map_err(|error| io_error("read", &path, error))?;
        let rows = decode_csv(&bytes)?;
        Ok(Self { path, rows })
    }

    pub fn load_for_resume(
        path: impl AsRef<Path>,
        identity: &ResumeIdentity<'_>,
    ) -> Result<Self, CsvError> {
        let file = Self::load(path)?;
        let mut ids = BTreeSet::new();
        for row in &file.rows {
            if !ids.insert(row.run_id.as_str()) {
                return Err(CsvError::DuplicateRunId(row.run_id.to_string()));
            }
            if !row.is_effective() {
                return Err(CsvError::FailedResumeRow(row.run_id.to_string()));
            }
            if row.mode != identity.mode
                || row.rustkv_commit != identity.rustkv_commit
                || row.leveldb_commit != identity.leveldb_commit
                || row.environment_id != identity.environment_id
            {
                return Err(CsvError::IdentityMismatch(row.run_id.to_string()));
            }
        }
        Ok(file)
    }

    pub fn rows(&self) -> &[CsvRow] {
        &self.rows
    }

    pub fn contains(&self, run_id: &RunId) -> bool {
        self.rows.iter().any(|row| &row.run_id == run_id)
    }

    pub fn append(&mut self, row: CsvRow) -> Result<(), CsvError> {
        row.validate()?;
        if self.contains(&row.run_id) {
            return Err(CsvError::DuplicateRunId(row.run_id.to_string()));
        }
        if let Some(first) = self.rows.first()
            && (row.mode != first.mode
                || row.config_version != first.config_version
                || row.rustkv_commit != first.rustkv_commit
                || row.leveldb_commit != first.leveldb_commit
                || row.environment_id != first.environment_id)
        {
            return Err(CsvError::IdentityMismatch(row.run_id.to_string()));
        }
        self.rows.push(row);
        if let Err(error) = self.publish() {
            self.rows.pop();
            return Err(error);
        }
        Ok(())
    }

    fn publish(&self) -> Result<(), CsvError> {
        let parent = self.path.parent().ok_or_else(|| {
            CsvError::InvalidPath("CSV path must have a parent directory".to_owned())
        })?;
        let file_name = self
            .path
            .file_name()
            .ok_or_else(|| CsvError::InvalidPath("CSV path must have a file name".to_owned()))?;
        let temporary = parent.join(format!(".{}.checkpoint", file_name.to_string_lossy()));
        let bytes = encode_csv(&self.rows);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| io_error("create checkpoint", &temporary, error))?;
        file.write_all(bytes.as_bytes())
            .map_err(|error| io_error("write checkpoint", &temporary, error))?;
        file.sync_all()
            .map_err(|error| io_error("sync checkpoint", &temporary, error))?;
        drop(file);
        fs::rename(&temporary, &self.path)
            .map_err(|error| io_error("publish checkpoint", &self.path, error))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync CSV parent", parent, error))?;
        Ok(())
    }
}

pub fn require_exact_matrix(rows: &[CsvRow], mode: BenchMode) -> Result<(), CsvError> {
    let expected = match mode {
        BenchMode::Formal => crate::formal_matrix(),
        BenchMode::Smoke => {
            let config = BenchConfig::test_only(1_000, 100, 100, 100, 20);
            crate::smoke_matrix(&config).map_err(|error| CsvError::InvalidRow(error.to_string()))?
        }
    };
    if rows.len() != expected.len() {
        return Err(CsvError::MatrixSizeMismatch {
            expected: expected.len(),
            actual: rows.len(),
        });
    }
    let expected_ids = expected
        .into_iter()
        .map(|unit| unit.id().to_string())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeMap::new();
    for row in rows {
        row.validate()?;
        if row.mode != mode {
            return Err(CsvError::WrongMode(row.run_id.to_string()));
        }
        if !row.is_effective() {
            return Err(CsvError::FailedResumeRow(row.run_id.to_string()));
        }
        if actual.insert(row.run_id.to_string(), ()).is_some() {
            return Err(CsvError::DuplicateRunId(row.run_id.to_string()));
        }
    }
    let actual_ids = actual.into_keys().collect::<BTreeSet<_>>();
    if actual_ids != expected_ids {
        return Err(CsvError::MatrixIdentityMismatch);
    }
    let first = rows.first().expect("non-empty fixed matrix");
    if rows.iter().any(|row| {
        row.config_version != first.config_version
            || row.rustkv_commit != first.rustkv_commit
            || row.leveldb_commit != first.leveldb_commit
            || row.environment_id != first.environment_id
    }) {
        return Err(CsvError::MixedIdentity);
    }
    Ok(())
}

#[derive(Debug)]
pub enum CsvError {
    InvalidPath(String),
    AlreadyExists(PathBuf),
    Io(String),
    Malformed(String),
    InvalidRow(String),
    DuplicateRunId(String),
    FailedResumeRow(String),
    IdentityMismatch(String),
    WrongMode(String),
    MatrixSizeMismatch { expected: usize, actual: usize },
    MatrixIdentityMismatch,
    MixedIdentity,
}

impl fmt::Display for CsvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CSV error: {self:?}")
    }
}

impl Error for CsvError {}

fn checked_absolute_file(path: &Path) -> Result<PathBuf, CsvError> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(CsvError::InvalidPath(
            "CSV path must be an absolute file path".to_owned(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| CsvError::InvalidPath("CSV path has no parent".to_owned()))?;
    let parent =
        fs::canonicalize(parent).map_err(|error| io_error("canonicalize", parent, error))?;
    Ok(parent.join(path.file_name().expect("checked above")))
}

fn encode_csv(rows: &[CsvRow]) -> String {
    let mut output = String::new();
    output.push_str(&CSV_COLUMNS.join(","));
    output.push_str("\r\n");
    for row in rows {
        let fields = row.fields();
        for (index, field) in fields.iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            push_escaped(&mut output, field);
        }
        output.push_str("\r\n");
    }
    output
}

fn decode_csv(bytes: &[u8]) -> Result<Vec<CsvRow>, CsvError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| CsvError::Malformed(format!("CSV is not UTF-8: {error}")))?;
    let records = parse_records(text)?;
    let Some(header) = records.first() else {
        return Err(CsvError::Malformed("CSV is empty".to_owned()));
    };
    if header.iter().map(String::as_str).collect::<Vec<_>>() != CSV_COLUMNS {
        return Err(CsvError::Malformed(
            "CSV header does not match schema".to_owned(),
        ));
    }
    records
        .iter()
        .enumerate()
        .skip(1)
        .map(|(index, fields)| CsvRow::from_fields(fields, index + 1))
        .collect()
}

fn parse_records(input: &str) -> Result<Vec<Vec<String>>, CsvError> {
    let bytes = input.as_bytes();
    let mut records = Vec::new();
    let mut record = Vec::new();
    let mut field = String::new();
    let mut index = 0;
    let mut quoted = false;
    let mut quote_closed = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            if byte == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    field.push('"');
                    index += 2;
                    continue;
                }
                quoted = false;
                quote_closed = true;
                index += 1;
                continue;
            }
            let ch = input[index..]
                .chars()
                .next()
                .expect("index is below UTF-8 input length");
            field.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if quote_closed && !matches!(byte, b',' | b'\r' | b'\n') {
            return Err(CsvError::Malformed(
                "unexpected character after closing quote".to_owned(),
            ));
        }
        match byte {
            b'"' if field.is_empty() && !quote_closed => {
                quoted = true;
                index += 1;
            }
            b',' => {
                record.push(std::mem::take(&mut field));
                quote_closed = false;
                index += 1;
            }
            b'"' => {
                return Err(CsvError::Malformed(
                    "quote may only appear at the start of a quoted field".to_owned(),
                ));
            }
            b'\r' | b'\n' => {
                if byte == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
                    index += 1;
                }
                record.push(std::mem::take(&mut field));
                quote_closed = false;
                records.push(std::mem::take(&mut record));
                index += 1;
            }
            _ => {
                let ch = input[index..]
                    .chars()
                    .next()
                    .expect("index is below UTF-8 input length");
                field.push(ch);
                index += ch.len_utf8();
            }
        }
    }
    if quoted {
        return Err(CsvError::Malformed("unterminated quoted field".to_owned()));
    }
    if !field.is_empty() || !record.is_empty() || quote_closed {
        return Err(CsvError::Malformed(
            "CSV ends with an incomplete record".to_owned(),
        ));
    }
    Ok(records)
}

fn push_escaped(output: &mut String, field: &str) {
    if field
        .bytes()
        .any(|byte| matches!(byte, b',' | b'"' | b'\r' | b'\n'))
    {
        output.push('"');
        for ch in field.chars() {
            if ch == '"' {
                output.push('"');
            }
            output.push(ch);
        }
        output.push('"');
    } else {
        output.push_str(field);
    }
}

fn format_optional(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| format!("{value:.17}"))
}

fn approximately_equal(left: f64, right: f64) -> bool {
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= scale * 1e-12
}

fn parse_optional(value: &str, record_number: usize, name: &str) -> Result<Option<f64>, CsvError> {
    if value.is_empty() {
        Ok(None)
    } else {
        value
            .parse()
            .map(Some)
            .map_err(|_| CsvError::Malformed(format!("record {record_number} has invalid {name}")))
    }
}

fn parse_number<T: std::str::FromStr>(
    value: &str,
    record_number: usize,
    name: &str,
) -> Result<T, CsvError> {
    value
        .parse()
        .map_err(|_| CsvError::Malformed(format!("record {record_number} has invalid {name}")))
}

fn parse_error(record_number: usize, error: impl fmt::Display) -> CsvError {
    CsvError::Malformed(format!("record {record_number}: {error}"))
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> CsvError {
    CsvError::Io(format!("{operation} {} failed: {error}", path.display()))
}
