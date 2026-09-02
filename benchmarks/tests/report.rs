use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use kv_bench::{
    BackendKind, BenchConfig, BenchMode, CsvFile, CsvRow, LEVELDB_COMMIT, RunUnit, Workload,
    formal_matrix, generate_formal_report, generate_smoke_report, smoke_matrix, summarize_formal,
    summarize_smoke,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const RUST_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "kv-bench-report-{}-{}",
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

fn factors() -> [f64; 5] {
    let fixture = include_str!("fixtures/report_input.csv");
    let mut values = fixture
        .lines()
        .skip(1)
        .map(|line| line.split(',').nth(1).unwrap().parse().unwrap());
    std::array::from_fn(|_| values.next().unwrap())
}

fn formal_rows() -> Vec<CsvRow> {
    let factors = factors();
    let config = BenchConfig::formal();
    formal_matrix()
        .into_iter()
        .map(|unit| {
            let factor = factors[unit.repetition as usize];
            let backend_factor = match unit.backend {
                BackendKind::RustKv => 1_000.0,
                BackendKind::LevelDb => 500.0,
            };
            let latency_factor = match unit.backend {
                BackendKind::RustKv => 10.0,
                BackendKind::LevelDb => 20.0,
            };
            success_row(
                unit,
                &config,
                backend_factor * unit.thread_count as f64 * factor,
                latency_factor * factor,
            )
        })
        .collect()
}

fn smoke_rows() -> Vec<CsvRow> {
    let config = BenchConfig::test_only(1_000, 100, 100, 100, 20);
    smoke_matrix(&config)
        .unwrap()
        .into_iter()
        .map(|unit| {
            let throughput = match unit.backend {
                BackendKind::RustKv => 2_000.0,
                BackendKind::LevelDb => 1_000.0,
            } * unit.thread_count as f64;
            success_row(unit, &config, throughput, 1.0)
        })
        .collect()
}

fn success_row(unit: RunUnit, config: &BenchConfig, throughput: f64, p50: f64) -> CsvRow {
    let completed_ops = unit.workload.operation_count(config);
    let completed_records = completed_ops * unit.workload.records_per_operation(config);
    let wall = completed_ops as f64 / throughput;
    CsvRow {
        mode: unit.mode,
        config_version: kv_bench::CONFIG_VERSION.to_owned(),
        run_id: unit.id(),
        backend: unit.backend,
        workload: unit.workload,
        thread_count: unit.thread_count,
        repetition: unit.repetition,
        completed_ops,
        completed_records,
        wall_seconds: Some(wall),
        ops_per_second: Some(throughput),
        records_per_second: matches!(
            unit.workload,
            Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
        )
        .then_some(completed_records as f64 / wall),
        mean_latency_us: Some(p50 * 0.75),
        p50_latency_us: Some(p50),
        p95_latency_us: Some(p50 * 2.0),
        p99_latency_us: Some(p50 * 3.0),
        error_count: 0,
        validation_success: true,
        error_text: String::new(),
        rustkv_commit: if unit.mode == BenchMode::Formal {
            RUST_COMMIT.to_owned()
        } else {
            "smoke-not-a-formal-result".to_owned()
        },
        leveldb_commit: LEVELDB_COMMIT.to_owned(),
        environment_id: if unit.mode == BenchMode::Formal {
            "fixture-env".to_owned()
        } else {
            "local-mac-smoke".to_owned()
        },
    }
}

#[test]
fn five_unsorted_samples_use_independent_medians_and_backend_median_ratio() {
    assert_eq!(factors(), [3.0, 1.0, 5.0, 2.0, 4.0]);
    let summary = summarize_formal(&formal_rows()).unwrap();
    assert_eq!(summary.rows.len(), 24);
    let row = summary
        .rows
        .iter()
        .find(|row| row.workload == Workload::RangeScan && row.thread_count == 10)
        .unwrap();
    assert_eq!(row.rustkv_ops_per_second, 30_000.0);
    assert_eq!(row.leveldb_ops_per_second, 15_000.0);
    assert_eq!(row.throughput_ratio(), 2.0);
    assert_eq!(row.rustkv_p50_us, 30.0);
    assert_eq!(row.leveldb_p50_us, 60.0);
    assert_eq!(row.rustkv_p95_us, 60.0);
    assert_eq!(row.leveldb_p95_us, 120.0);
    assert_eq!(row.rustkv_p99_us, 90.0);
    assert_eq!(row.leveldb_p99_us, 180.0);
    assert_eq!(row.rustkv_records_per_second, Some(3_000_000.0));
    assert_eq!(row.leveldb_records_per_second, Some(1_500_000.0));
}

#[test]
fn markdown_and_six_dependency_free_svgs_match_the_golden_contract() {
    let temp = TempDirectory::new();
    let csv_path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&csv_path).unwrap();
    for row in formal_rows() {
        csv.append(row).unwrap();
    }
    let output = temp.path().join("report");
    let report = generate_formal_report(&csv_path, &output).unwrap();
    assert_eq!(
        std::fs::read_to_string(report).unwrap(),
        include_str!("fixtures/report_expected.md")
    );
    for workload in Workload::ALL {
        let svg =
            std::fs::read_to_string(output.join(format!("{}.svg", workload.as_str()))).unwrap();
        assert!(svg.contains("ops/s"));
        assert!(svg.contains("OS threads"));
        assert!(svg.contains("RustKV"));
        assert!(svg.contains("LevelDB"));
        assert_eq!(svg.matches("<polyline").count(), 2);
        assert_eq!(svg.matches("<circle").count(), 8);
        for thread_count in [1, 10, 100, 1_000] {
            assert!(svg.contains(&format!(">{thread_count}</text>")));
        }
    }
}

#[test]
fn missing_extra_failed_invalid_nonfinite_wrong_units_and_smoke_rows_are_rejected() {
    let rows = formal_rows();
    assert!(summarize_formal(&rows[..239]).is_err());
    let mut extra = rows.clone();
    extra.push(rows[0].clone());
    assert!(summarize_formal(&extra).is_err());

    let mut failed = rows.clone();
    failed[0].error_count = 1;
    failed[0].validation_success = false;
    failed[0].error_text = "failure".to_owned();
    assert!(summarize_formal(&failed).is_err());

    let mut nonfinite = rows.clone();
    nonfinite[0].ops_per_second = Some(f64::INFINITY);
    assert!(summarize_formal(&nonfinite).is_err());

    let mut wrong_units = rows.clone();
    let index = wrong_units
        .iter()
        .position(|row| row.workload == Workload::SinglePut)
        .unwrap();
    wrong_units[index].records_per_second = Some(1.0);
    assert!(summarize_formal(&wrong_units).is_err());

    let mut mixed = rows.clone();
    mixed[1].environment_id = "other-mac".to_owned();
    assert!(summarize_formal(&mixed).is_err());
    assert!(summarize_formal(&smoke_rows()).is_err());
}

#[test]
fn smoke_summary_is_separate_and_has_only_one_and_ten_thread_points() {
    let rows = smoke_rows();
    let summary = summarize_smoke(&rows).unwrap();
    assert_eq!(summary.mode, BenchMode::Smoke);
    assert_eq!(summary.rows.len(), 12);
    assert!(
        summary
            .rows
            .iter()
            .all(|row| [1, 10].contains(&row.thread_count))
    );

    let temp = TempDirectory::new();
    let csv_path = temp.path().join("smoke.csv");
    let mut csv = CsvFile::create(&csv_path).unwrap();
    for row in rows {
        csv.append(row).unwrap();
    }
    let report_path = generate_smoke_report(&csv_path, temp.path().join("report")).unwrap();
    let report = std::fs::read_to_string(report_path).unwrap();
    assert!(report.contains("Smoke（非正式性能结果）"));
    assert!(report.contains("非正式小配置：数据记录 1,000"));
    assert!(report.contains("每个 RunUnit 使用独立新目录"));
    assert!(report.contains("不使用模板、COW 克隆或物理目录复制"));
    assert!(!report.contains("数据记录 10,000,000"));
}

#[test]
fn report_rejects_relative_paths_existing_output_and_mixed_mode_input() {
    let temp = TempDirectory::new();
    let csv_path = temp.path().join("raw.csv");
    let mut csv = CsvFile::create(&csv_path).unwrap();
    for row in smoke_rows() {
        csv.append(row).unwrap();
    }
    assert!(generate_formal_report(&csv_path, temp.path().join("formal")).is_err());
    assert!(generate_formal_report("relative.csv", temp.path().join("other")).is_err());
    let existing = temp.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    assert!(generate_formal_report(&csv_path, existing).is_err());
}
