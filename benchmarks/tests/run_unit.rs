use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

use kv_bench::{
    BackendKind, BenchConfig, BenchmarkWorkspace, RunUnit, RunUnitFault, RunUnitStage, Trace,
    Workload, execute_run_unit_with_fault, smoke_matrix,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn both_real_backends_follow_the_exact_load_run_lifecycle_for_all_six_workloads() {
    let config = config(1_000);
    let root = temp_root("all-workloads");
    let workspace = BenchmarkWorkspace::create(root.join("workspace")).unwrap();
    let mut paths = BTreeSet::new();

    for backend in [BackendKind::RustKv, BackendKind::LevelDb] {
        for workload in Workload::ALL {
            let unit = unit(&config, backend, workload, 1);
            let trace = Trace::generate(&config, workload, 0).unwrap();
            let attempt =
                execute_run_unit_with_fault(&workspace, &config, unit, &trace, RunUnitFault::None)
                    .unwrap();
            assert!(attempt.result().unwrap().is_valid());
            assert!(attempt.validation_success());
            assert_eq!(attempt.error_text(), None);
            assert_eq!(attempt.audit().open_count, 4);
            assert_eq!(attempt.audit().stages, successful_stages(workload));

            let needs_load = matches!(
                workload,
                Workload::RandomGet
                    | Workload::RangeScan
                    | Workload::SingleDelete
                    | Workload::BatchDelete
            );
            assert_eq!(
                (attempt.audit().loaded_records, attempt.audit().load_batches),
                if needs_load { (1_000, 1) } else { (0, 0) }
            );
            assert_eq!(
                attempt.audit().load_batch_sizes,
                if needs_load { vec![1_000] } else { Vec::new() }
            );
            assert_eq!(
                attempt.audit().initial_validation.unwrap().record_count,
                if needs_load { 1_000 } else { 0 }
            );
            assert_eq!(
                attempt.audit().prewarm.map(|summary| summary.record_count),
                matches!(workload, Workload::RandomGet | Workload::RangeScan).then_some(1_000)
            );
            assert_eq!(
                attempt.audit().final_validation.unwrap().record_count,
                if matches!(workload, Workload::SingleDelete | Workload::BatchDelete) {
                    0
                } else {
                    1_000
                }
            );
            let path = attempt.path_for_test().to_path_buf();
            assert!(path.is_dir());
            assert!(paths.insert(path.clone()), "RunUnit directory was reused");
            attempt.cleanup(&workspace).unwrap();
            assert!(!path.exists());
        }
    }
    assert!(
        std::fs::read_dir(workspace.root())
            .unwrap()
            .next()
            .is_none()
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn load_uses_fixed_thousand_record_batches_and_an_explicit_final_tail() {
    let config = config(2_500);
    let root = temp_root("load-geometry");
    let workspace = BenchmarkWorkspace::create(root.join("workspace")).unwrap();
    let unit = unit(&config, BackendKind::RustKv, Workload::RandomGet, 1);
    let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
    let attempt =
        execute_run_unit_with_fault(&workspace, &config, unit, &trace, RunUnitFault::None).unwrap();
    assert_eq!(attempt.audit().loaded_records, 2_500);
    assert_eq!(attempt.audit().load_batches, 3);
    assert_eq!(attempt.audit().load_batch_sizes, [1_000, 1_000, 500]);
    assert_eq!(
        attempt.audit().initial_validation.unwrap().record_count,
        2_500
    );
    assert!(attempt.result().unwrap().is_valid());
    attempt.cleanup(&workspace).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn load_and_initial_validation_failures_never_start_the_timed_run() {
    let config = config(1_000);
    let root = temp_root("pre-run-failures");
    let workspace = BenchmarkWorkspace::create(root.join("workspace")).unwrap();
    let unit = unit(&config, BackendKind::LevelDb, Workload::RandomGet, 1);
    let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();

    let after_load = execute_run_unit_with_fault(
        &workspace,
        &config,
        unit,
        &trace,
        RunUnitFault::AfterLoadClosed,
    )
    .unwrap();
    assert!(after_load.result().is_none());
    assert_eq!(after_load.audit().open_count, 1);
    assert!(!after_load.validation_success());
    assert!(
        after_load
            .error_text()
            .unwrap()
            .contains("after Load close")
    );
    after_load.cleanup(&workspace).unwrap();

    let corrupt = execute_run_unit_with_fault(
        &workspace,
        &config,
        unit,
        &trace,
        RunUnitFault::CorruptBeforeInitialValidation,
    )
    .unwrap();
    assert!(corrupt.result().is_none());
    assert_eq!(corrupt.audit().open_count, 2);
    assert!(corrupt.audit().prewarm.is_none());
    assert!(
        corrupt
            .error_text()
            .unwrap()
            .contains("validate initial state")
    );
    corrupt.cleanup(&workspace).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn terminal_validation_failure_preserves_real_run_metrics_but_invalidates_the_attempt() {
    let config = config(1_000);
    let root = temp_root("terminal-failure");
    let workspace = BenchmarkWorkspace::create(root.join("workspace")).unwrap();
    let unit = unit(&config, BackendKind::RustKv, Workload::RandomGet, 1);
    let trace = Trace::generate(&config, Workload::RandomGet, 0).unwrap();
    let attempt = execute_run_unit_with_fault(
        &workspace,
        &config,
        unit,
        &trace,
        RunUnitFault::CorruptBeforeFinalValidation,
    )
    .unwrap();
    assert!(attempt.result().unwrap().is_valid());
    assert!(attempt.result().unwrap().wall_time.as_nanos() > 0);
    assert!(!attempt.validation_success());
    assert!(attempt.audit().final_validation.is_none());
    assert!(
        attempt
            .error_text()
            .unwrap()
            .contains("validate final state")
    );
    attempt.cleanup(&workspace).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn mismatched_trace_is_rejected_before_a_directory_or_worker_exists() {
    let config = config(1_000);
    let root = temp_root("trace-mismatch");
    let workspace = BenchmarkWorkspace::create(root.join("workspace")).unwrap();
    let unit = unit(&config, BackendKind::RustKv, Workload::RandomGet, 1);
    let wrong = Trace::generate(&config, Workload::RangeScan, 0).unwrap();
    let error = execute_run_unit_with_fault(&workspace, &config, unit, &wrong, RunUnitFault::None)
        .unwrap_err();
    assert_eq!(error.stage(), "validate Trace identity");
    assert!(
        std::fs::read_dir(workspace.root())
            .unwrap()
            .next()
            .is_none()
    );
    std::fs::remove_dir_all(root).unwrap();
}

fn successful_stages(workload: Workload) -> Vec<RunUnitStage> {
    vec![
        RunUnitStage::DirectoryCreated,
        RunUnitStage::LoadDatabaseOpened,
        RunUnitStage::LoadCompleted,
        RunUnitStage::LoadDatabaseClosed,
        RunUnitStage::InitialValidationDatabaseOpened,
        RunUnitStage::InitialStateValidated,
        RunUnitStage::InitialValidationDatabaseClosed,
        RunUnitStage::RunDatabaseOpened,
        if matches!(workload, Workload::RandomGet | Workload::RangeScan) {
            RunUnitStage::ReadPrewarmCompleted
        } else {
            RunUnitStage::NoExtraPrewarm
        },
        RunUnitStage::RunCompleted,
        RunUnitStage::RunDatabaseClosed,
        RunUnitStage::FinalValidationDatabaseOpened,
        RunUnitStage::FinalStateValidated,
        RunUnitStage::FinalValidationDatabaseClosed,
    ]
}

fn unit(config: &BenchConfig, backend: BackendKind, workload: Workload, threads: usize) -> RunUnit {
    smoke_matrix(config)
        .unwrap()
        .into_iter()
        .find(|unit| {
            unit.backend == backend && unit.workload == workload && unit.thread_count == threads
        })
        .unwrap()
}

fn config(records: u64) -> BenchConfig {
    BenchConfig::test_only(records, 100, 100, 100, 20)
}

fn temp_root(label: &str) -> std::path::PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "kv-bench-run-unit-{label}-{}-{sequence}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).unwrap();
    root
}
