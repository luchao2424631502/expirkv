use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

use kv_bench::{
    BackendKind, BenchConfig, BenchMode, CsvFile, CsvRow, ExecutionMetadata, LEVELDB_COMMIT,
    MatrixExecutionError, RunUnit, Workload, execute_units, formal_matrix, smoke_matrix,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn formal_matrix_is_exactly_240_unique_stable_units() {
    let first = formal_matrix();
    let second = formal_matrix();
    assert_eq!(first, second);
    assert_eq!(first.len(), 240);
    assert_eq!(
        first
            .iter()
            .map(|unit| unit.id().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        240
    );
    assert_eq!(
        first
            .iter()
            .map(|unit| unit.thread_count)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 10, 100, 1_000])
    );
    let mut repetitions = BTreeMap::new();
    for unit in &first {
        *repetitions
            .entry((
                unit.workload.as_str(),
                unit.thread_count,
                unit.backend.as_str(),
            ))
            .or_insert(0) += 1;
        assert_eq!(unit.mode, BenchMode::Formal);
    }
    assert_eq!(repetitions.len(), 48);
    assert!(repetitions.values().all(|count| *count == 5));
}

#[test]
fn backend_order_alternates_by_combination_and_repetition() {
    let matrix = formal_matrix();
    for pair in matrix.chunks_exact(2) {
        let left = pair[0];
        let right = pair[1];
        assert_eq!(left.workload, right.workload);
        assert_eq!(left.thread_count, right.thread_count);
        assert_eq!(left.repetition, right.repetition);
        let expected_first =
            if (left.combination_index() + left.repetition as usize).is_multiple_of(2) {
                BackendKind::RustKv
            } else {
                BackendKind::LevelDb
            };
        assert_eq!(left.backend, expected_first);
        assert_ne!(left.backend, right.backend);
    }
}

#[test]
fn run_id_contains_every_identity_component() {
    let unit = RunUnit::formal(BackendKind::LevelDb, Workload::BatchDelete, 1_000, 4).unwrap();
    assert_eq!(
        unit.id().as_str(),
        "rustkv-leveldb-v2-formal-leveldb-batch_delete-t1000-r4"
    );
    assert!(RunUnit::formal(BackendKind::RustKv, Workload::RandomGet, 2, 0).is_err());
    assert!(RunUnit::formal(BackendKind::RustKv, Workload::RandomGet, 1, 5).is_err());
}

#[test]
fn smoke_matrix_is_explicitly_small_and_covers_both_active_thread_counts() {
    let config = smoke_config();
    let matrix = smoke_matrix(&config).unwrap();
    assert_eq!(matrix.len(), 24);
    for workload in Workload::ALL {
        for thread_count in [1, 10] {
            let selected = matrix
                .iter()
                .filter(|unit| unit.workload == workload && unit.thread_count == thread_count)
                .collect::<Vec<_>>();
            assert_eq!(selected.len(), 2);
            assert!(selected.iter().all(|unit| {
                unit.mode == BenchMode::Smoke
                    && unit.repetition == 0
                    && [BackendKind::RustKv, BackendKind::LevelDb].contains(&unit.backend)
            }));
        }
    }
    assert!(smoke_matrix(&BenchConfig::formal()).is_err());
}

#[test]
fn resume_rows_must_be_an_exact_prefix_of_the_fixed_execution_order() {
    let root = temp_root("prefix");
    let csv_path = root.join("raw.csv");
    let workspace = root.join("workspace");
    let units = formal_matrix();
    let mut csv = CsvFile::create(&csv_path).unwrap();
    csv.append(effective_row(
        units[1],
        &BenchConfig::formal(),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "mac",
    ))
    .unwrap();
    let result = execute_units(
        &workspace,
        &csv_path,
        &BenchConfig::formal(),
        &units,
        &ExecutionMetadata::formal(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "mac".to_owned(),
        ),
        true,
    );
    assert!(matches!(
        result,
        Err(MatrixExecutionError::ResumeSequenceMismatch)
    ));
    assert!(!workspace.exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn resume_uses_a_new_workspace_and_reloads_every_remaining_rununit() {
    let root = temp_root("resume");
    let workspace = root.join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(workspace.join("sentinel"), b"never adopt or modify").unwrap();
    let csv_path = root.join("raw.csv");
    let config = smoke_config();
    let units = smoke_matrix(&config).unwrap();
    let mut csv = CsvFile::create(&csv_path).unwrap();
    csv.append(effective_row(
        units[0],
        &config,
        "smoke-not-a-formal-result",
        "local-mac-smoke",
    ))
    .unwrap();

    assert_eq!(
        execute_units(
            &workspace,
            &csv_path,
            &config,
            &units,
            &ExecutionMetadata::smoke(),
            true,
        )
        .unwrap(),
        23
    );
    let resumed = root.join("workspace.resume-1");
    assert!(resumed.is_dir());
    assert!(std::fs::read_dir(&resumed).unwrap().next().is_none());
    assert_eq!(
        std::fs::read(workspace.join("sentinel")).unwrap(),
        b"never adopt or modify"
    );
    let csv = load_smoke_csv(&csv_path);
    assert_eq!(csv.rows().len(), 24);
    assert!(csv.rows().iter().all(CsvRow::is_effective));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn existing_workspace_is_never_adopted_and_each_successful_unit_is_cleaned_after_csv() {
    let root = temp_root("existing");
    let requested = root.join("workspace");
    std::fs::create_dir(&requested).unwrap();
    std::fs::write(requested.join("sentinel"), b"preserve").unwrap();
    let csv_path = root.join("raw.csv");
    let config = smoke_config();
    let unit = smoke_matrix(&config)
        .unwrap()
        .into_iter()
        .find(|unit| unit.backend == BackendKind::RustKv && unit.workload == Workload::SinglePut)
        .unwrap();

    assert_eq!(
        execute_units(
            &requested,
            &csv_path,
            &config,
            &[unit],
            &ExecutionMetadata::smoke(),
            false,
        )
        .unwrap(),
        1
    );
    let invocation = root.join("workspace.run-1");
    assert!(invocation.is_dir());
    assert!(std::fs::read_dir(&invocation).unwrap().next().is_none());
    assert_eq!(
        std::fs::read(requested.join("sentinel")).unwrap(),
        b"preserve"
    );
    assert!(load_smoke_csv(&csv_path).rows()[0].is_effective());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn full_smoke_matrix_uses_direct_load_run_and_leaves_no_run_database() {
    let root = temp_root("full");
    let workspace = root.join("workspace");
    let csv_path = root.join("raw.csv");
    let config = smoke_config();
    let units = smoke_matrix(&config).unwrap();
    assert_eq!(
        execute_units(
            &workspace,
            &csv_path,
            &config,
            &units,
            &ExecutionMetadata::smoke(),
            false,
        )
        .unwrap(),
        24
    );
    assert!(workspace.is_dir());
    assert!(std::fs::read_dir(&workspace).unwrap().next().is_none());
    let csv = load_smoke_csv(&csv_path);
    assert_eq!(csv.rows().len(), 24);
    assert!(csv.rows().iter().all(CsvRow::is_effective));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn final_execution_sources_have_no_template_copy_or_prepare_dependency() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "src/cli.rs",
        "src/custom.rs",
        "src/matrix.rs",
        "src/run_unit.rs",
        "scripts/run_custom.sh",
        "scripts/run_smoke.sh",
    ] {
        let source = std::fs::read_to_string(manifest.join(relative)).unwrap();
        for forbidden in [
            "prepare_both_templates",
            "load_prepared_templates",
            "prepare_run(",
            "build_formal_template",
            "build_test_template",
            "restore_template",
            "/bin/cp",
            "cp -cR",
            "clonefile",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} still contains forbidden final-path dependency {forbidden}"
            );
        }
    }
}

fn smoke_config() -> BenchConfig {
    BenchConfig::test_only(1_000, 100, 100, 100, 20)
}

fn effective_row(
    unit: RunUnit,
    config: &BenchConfig,
    rustkv_commit: &str,
    environment_id: &str,
) -> CsvRow {
    let completed_ops = unit.workload.operation_count(config);
    let records_per_op = unit.workload.records_per_operation(config);
    CsvRow {
        mode: unit.mode,
        config_version: kv_bench::CONFIG_VERSION.to_owned(),
        run_id: unit.id(),
        backend: unit.backend,
        workload: unit.workload,
        thread_count: unit.thread_count,
        repetition: unit.repetition,
        completed_ops,
        completed_records: completed_ops * records_per_op,
        wall_seconds: Some(completed_ops as f64 / 1_000.0),
        ops_per_second: Some(1_000.0),
        records_per_second: (records_per_op > 1).then_some(1_000.0 * records_per_op as f64),
        mean_latency_us: Some(1.0),
        p50_latency_us: Some(1.0),
        p95_latency_us: Some(1.0),
        p99_latency_us: Some(1.0),
        error_count: 0,
        validation_success: true,
        error_text: String::new(),
        rustkv_commit: rustkv_commit.to_owned(),
        leveldb_commit: LEVELDB_COMMIT.to_owned(),
        environment_id: environment_id.to_owned(),
    }
}

fn load_smoke_csv(path: &std::path::Path) -> CsvFile {
    CsvFile::load_for_resume(
        path,
        &kv_bench::ResumeIdentity {
            mode: BenchMode::Smoke,
            rustkv_commit: "smoke-not-a-formal-result",
            leveldb_commit: LEVELDB_COMMIT,
            environment_id: "local-mac-smoke",
        },
    )
    .unwrap()
}

fn temp_root(label: &str) -> std::path::PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "kv-bench-matrix-{label}-{}-{sequence}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).unwrap();
    root
}
