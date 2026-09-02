use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use kv_bench::{
    BackendKind, BenchConfig, BenchMode, CsvError, CsvFile, CsvRow, ExecutionMetadata,
    LEVELDB_COMMIT, ResumeIdentity, RunUnit, Workload,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const RUST_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "kv-bench-csv-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn success_row(unit: RunUnit, sample: f64, environment: &str) -> CsvRow {
    let config = BenchConfig::formal();
    let completed_ops = unit.workload.operation_count(&config);
    let completed_records = completed_ops * unit.workload.records_per_operation(&config);
    let wall = completed_ops as f64 / sample;
    CsvRow {
        mode: BenchMode::Formal,
        config_version: kv_bench::CONFIG_VERSION.to_owned(),
        run_id: unit.id(),
        backend: unit.backend,
        workload: unit.workload,
        thread_count: unit.thread_count,
        repetition: unit.repetition,
        completed_ops,
        completed_records,
        wall_seconds: Some(wall),
        ops_per_second: Some(sample),
        records_per_second: matches!(
            unit.workload,
            Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
        )
        .then_some(completed_records as f64 / wall),
        mean_latency_us: Some(1.1234567890123457),
        p50_latency_us: Some(1.0),
        p95_latency_us: Some(2.0),
        p99_latency_us: Some(3.0),
        error_count: 0,
        validation_success: true,
        error_text: String::new(),
        rustkv_commit: RUST_COMMIT.to_owned(),
        leveldb_commit: LEVELDB_COMMIT.to_owned(),
        environment_id: environment.to_owned(),
    }
}

fn identity<'a>(environment_id: &'a str) -> ResumeIdentity<'a> {
    ResumeIdentity {
        mode: BenchMode::Formal,
        rustkv_commit: RUST_COMMIT,
        leveldb_commit: LEVELDB_COMMIT,
        environment_id,
    }
}

#[test]
fn rfc4180_round_trip_preserves_unicode_commas_quotes_newlines_and_float_precision() {
    let temp = TempDirectory::new();
    let path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&path).unwrap();
    let unit = RunUnit::formal(BackendKind::RustKv, Workload::RandomGet, 1, 0).unwrap();
    let success = success_row(unit, 12_345.678901234567, "mac-a");
    csv.append(success.clone()).unwrap();

    let failed_unit = RunUnit::formal(BackendKind::LevelDb, Workload::RandomGet, 1, 0).unwrap();
    let mut failed = CsvRow::from_run(
        failed_unit,
        None,
        false,
        Some("错误,含逗号\n第二行与\"引号\""),
        RUST_COMMIT,
        LEVELDB_COMMIT,
        "mac-a",
    );
    assert_eq!(failed.error_count, 1);
    csv.append(failed.clone()).unwrap();

    let loaded = CsvFile::load(&path).unwrap();
    assert_eq!(loaded.rows()[0], success);
    assert_eq!(loaded.rows()[1], failed);
    failed.error_text.push('!');
    assert_ne!(loaded.rows()[1], failed);
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.contains("\"错误,含逗号\n第二行与\"\"引号\"\"\""));
    assert!(raw.contains("12345.678901234567"));
}

#[test]
fn append_is_whole_file_atomic_and_an_interrupted_checkpoint_is_not_visible() {
    let temp = TempDirectory::new();
    let path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&path).unwrap();
    let first = success_row(
        RunUnit::formal(BackendKind::RustKv, Workload::SinglePut, 1, 0).unwrap(),
        1_000.0,
        "mac-a",
    );
    csv.append(first.clone()).unwrap();
    let published = std::fs::read(&path).unwrap();
    std::fs::write(temp.path().join(".raw.csv.checkpoint"), b"partial,row").unwrap();
    assert_eq!(CsvFile::load(&path).unwrap().rows(), &[first]);
    assert_eq!(std::fs::read(&path).unwrap(), published);

    let second = success_row(
        RunUnit::formal(BackendKind::LevelDb, Workload::SinglePut, 1, 0).unwrap(),
        900.0,
        "mac-a",
    );
    csv.append(second).unwrap();
    assert_eq!(CsvFile::load(&path).unwrap().rows().len(), 2);
    assert!(!temp.path().join(".raw.csv.checkpoint").exists());
}

#[test]
fn malformed_half_line_duplicate_id_and_failed_row_stop_resume() {
    let temp = TempDirectory::new();
    let malformed = temp.path().join("malformed.csv");
    std::fs::write(&malformed, "mode,config_version,run_id").unwrap();
    assert!(matches!(
        CsvFile::load(&malformed),
        Err(CsvError::Malformed(_))
    ));

    let duplicate = temp.path().join("duplicate.csv");
    let mut csv = CsvFile::create(&duplicate).unwrap();
    let row = success_row(
        RunUnit::formal(BackendKind::RustKv, Workload::RandomGet, 1, 0).unwrap(),
        1_000.0,
        "mac-a",
    );
    csv.append(row.clone()).unwrap();
    assert!(matches!(csv.append(row), Err(CsvError::DuplicateRunId(_))));
    let raw = std::fs::read_to_string(&duplicate).unwrap();
    let data = raw.split_once("\r\n").unwrap().1.to_owned();
    OpenOptions::new()
        .append(true)
        .open(&duplicate)
        .unwrap()
        .write_all(data.as_bytes())
        .unwrap();
    assert!(matches!(
        CsvFile::load_for_resume(&duplicate, &identity("mac-a")),
        Err(CsvError::DuplicateRunId(_))
    ));

    let failed_path = temp.path().join("failed.csv");
    let mut failed_csv = CsvFile::create(&failed_path).unwrap();
    let unit = RunUnit::formal(BackendKind::RustKv, Workload::RangeScan, 1, 0).unwrap();
    failed_csv
        .append(CsvRow::from_run(
            unit,
            None,
            false,
            Some("injected failure"),
            RUST_COMMIT,
            LEVELDB_COMMIT,
            "mac-a",
        ))
        .unwrap();
    assert!(matches!(
        CsvFile::load_for_resume(&failed_path, &identity("mac-a")),
        Err(CsvError::FailedResumeRow(_))
    ));
}

#[test]
fn resume_requires_exact_mode_commits_environment_and_complete_metrics() {
    let temp = TempDirectory::new();
    let path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&path).unwrap();
    let unit = RunUnit::formal(BackendKind::RustKv, Workload::BatchPut, 10, 2).unwrap();
    csv.append(success_row(unit, 500.0, "mac-a")).unwrap();
    assert!(CsvFile::load_for_resume(&path, &identity("mac-a")).is_ok());
    assert!(matches!(
        CsvFile::load_for_resume(&path, &identity("mac-b")),
        Err(CsvError::IdentityMismatch(_))
    ));

    let mut incomplete = success_row(
        RunUnit::formal(BackendKind::LevelDb, Workload::BatchPut, 10, 2).unwrap(),
        400.0,
        "mac-a",
    );
    incomplete.p99_latency_us = None;
    assert!(matches!(
        csv.append(incomplete),
        Err(CsvError::InvalidRow(_))
    ));
}

#[test]
fn mixed_identity_bad_units_non_finite_values_and_wrong_auxiliary_units_are_rejected() {
    let temp = TempDirectory::new();
    let path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&path).unwrap();
    csv.append(success_row(
        RunUnit::formal(BackendKind::RustKv, Workload::SingleDelete, 1, 0).unwrap(),
        2_000.0,
        "mac-a",
    ))
    .unwrap();
    assert!(matches!(
        csv.append(success_row(
            RunUnit::formal(BackendKind::LevelDb, Workload::SingleDelete, 1, 0).unwrap(),
            2_000.0,
            "mac-b"
        )),
        Err(CsvError::IdentityMismatch(_))
    ));

    let unit = RunUnit::formal(BackendKind::LevelDb, Workload::SingleDelete, 1, 0).unwrap();
    let mut bad = success_row(unit, 2_000.0, "mac-a");
    bad.ops_per_second = Some(f64::NAN);
    assert!(bad.validate().is_err());
    let mut bad = success_row(unit, 2_000.0, "mac-a");
    bad.records_per_second = Some(1.0);
    assert!(bad.validate().is_err());
    let mut bad = success_row(unit, 2_000.0, "mac-a");
    bad.completed_ops -= 1;
    assert!(bad.validate().is_err());
    let mut bad = success_row(unit, 2_000.0, "mac-a");
    bad.ops_per_second = Some(1.0);
    assert!(bad.validate().is_err());
    let mut bad = success_row(unit, 2_000.0, "mac-a");
    bad.p50_latency_us = Some(4.0);
    assert!(bad.validate().is_err());
    let mut old_template_identity = success_row(unit, 2_000.0, "mac-a");
    old_template_identity.config_version = "rustkv-leveldb-v1".to_owned();
    assert!(old_template_identity.validate().is_err());

    let malformed_quote = temp.path().join("bad-quote.csv");
    std::fs::write(
        &malformed_quote,
        "mode,config_version\r\nformal,bad\"quote\r\n",
    )
    .unwrap();
    assert!(matches!(
        CsvFile::load(malformed_quote),
        Err(CsvError::Malformed(_))
    ));

    let metadata = ExecutionMetadata::formal(RUST_COMMIT.to_owned(), "mac-a".to_owned());
    assert_eq!(metadata.leveldb_commit, LEVELDB_COMMIT);
}
