use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use kv_bench::{BackendKind, CustomRunSpec, Workload, execute_custom_run};

static NEXT: AtomicU64 = AtomicU64::new(0);
const COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kv-bench-custom-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
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

#[test]
fn every_single_custom_workload_runs_on_both_real_backends_and_writes_one_valid_row() {
    let temp = TempDirectory::new("real");
    for backend in [BackendKind::RustKv, BackendKind::LevelDb] {
        for workload in Workload::ALL {
            let output = temp
                .path()
                .join(format!("{}-{}", backend.as_str(), workload.as_str()));
            let outcome = execute_custom_run(&CustomRunSpec {
                output_directory: output.clone(),
                record_count: 100,
                backend,
                workload,
                thread_count: 1,
                rustkv_commit: COMMIT.to_owned(),
                worktree_state: "dirty".to_owned(),
            })
            .unwrap();
            let canonical_output = std::fs::canonicalize(&output).unwrap();
            assert_eq!(outcome.csv_path, canonical_output.join("result.csv"));
            assert_eq!(outcome.summary_path, canonical_output.join("result.md"));
            assert!(!output.join("workspace").exists());

            let csv = std::fs::read_to_string(&outcome.csv_path).unwrap();
            let lines = csv.lines().collect::<Vec<_>>();
            assert_eq!(lines.len(), 2);
            let fields = lines[1].split(',').collect::<Vec<_>>();
            assert_eq!(fields.len(), 25);
            assert_eq!(fields[0], "custom");
            assert_eq!(fields[2], "100");
            assert_eq!(fields[7], backend.as_str());
            assert_eq!(fields[8], workload.as_str());
            assert_eq!(fields[9], "1");
            assert_eq!(fields[10], expected_ops(workload).to_string());
            assert_eq!(fields[11], "100");
            assert_eq!(fields[19], "0");
            assert_eq!(fields[20], "true");
            assert!(fields[21].is_empty());
            assert_eq!(fields[23], "dirty");
            if matches!(
                workload,
                Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete
            ) {
                assert!(!fields[14].is_empty());
            } else {
                assert!(fields[14].is_empty());
            }
            let parameters = std::fs::read_to_string(output.join("parameters.txt")).unwrap();
            assert!(parameters.starts_with("mode=custom\nformal_result=false\n"));
            let summary = std::fs::read_to_string(&outcome.summary_path).unwrap();
            assert!(summary.contains("不是B7正式性能结果"));
            assert!(
                kv_bench::generate_formal_report(
                    &outcome.csv_path,
                    output.join("forbidden-formal-report")
                )
                .is_err()
            );
        }
    }
}

#[test]
fn custom_entry_rejects_invalid_size_threads_provenance_and_existing_output() {
    let temp = TempDirectory::new("invalid");
    let valid = CustomRunSpec {
        output_directory: temp.path().join("output"),
        record_count: 100,
        backend: BackendKind::RustKv,
        workload: Workload::SinglePut,
        thread_count: 1,
        rustkv_commit: COMMIT.to_owned(),
        worktree_state: "clean".to_owned(),
    };
    for changed in [
        CustomRunSpec {
            record_count: 99,
            ..valid.clone()
        },
        CustomRunSpec {
            record_count: 150,
            ..valid.clone()
        },
        CustomRunSpec {
            thread_count: 2,
            ..valid.clone()
        },
        CustomRunSpec {
            workload: Workload::BatchPut,
            thread_count: 10,
            ..valid.clone()
        },
        CustomRunSpec {
            rustkv_commit: "short".to_owned(),
            ..valid.clone()
        },
        CustomRunSpec {
            worktree_state: "unknown".to_owned(),
            ..valid.clone()
        },
    ] {
        assert!(execute_custom_run(&changed).is_err());
        assert!(!changed.output_directory.exists());
    }

    std::fs::create_dir(&valid.output_directory).unwrap();
    assert!(execute_custom_run(&valid).is_err());
}

#[test]
fn shell_is_valid_and_documents_exactly_one_user_selected_rununit() {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/run_custom.sh");
    assert!(
        Command::new("sh")
            .arg("-n")
            .arg(&script)
            .status()
            .unwrap()
            .success()
    );
    let help = Command::new("sh")
        .arg(&script)
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    for parameter in [
        "--backend",
        "--workload",
        "--threads",
        "--records",
        "--output-dir",
    ] {
        assert!(help.contains(parameter));
    }
    assert!(help.contains("exactly\none selected"));
    assert!(!help.contains("--repetitions"));

    let output = std::env::temp_dir().join("kv-bench-custom-invalid-output");
    let base = [
        "--backend".into(),
        "rustkv".into(),
        "--workload".into(),
        "single_put".into(),
        "--threads".into(),
        "1".into(),
        "--records".into(),
        "100".into(),
        "--output-dir".into(),
        output.as_os_str().to_owned(),
    ];
    for (position, invalid) in [(1, "other"), (3, "unknown"), (5, "2"), (7, "abc")] {
        let mut arguments = base.clone();
        arguments[position] = OsString::from(invalid);
        assert!(
            !Command::new("sh")
                .arg(&script)
                .args(arguments)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}

#[test]
fn remaining_script_expands_170_runs_and_the_six_required_skips() {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/run_remaining_t1.sh");
    assert!(
        Command::new("sh")
            .arg("-n")
            .arg(&script)
            .status()
            .unwrap()
            .success()
    );

    let temp = TempDirectory::new("remaining-dry-run");
    let output_root = temp.path().join("output");
    let output = Command::new("sh")
        .arg(&script)
        .arg("--dry-run")
        .arg("--output-root")
        .arg(&output_root)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!output_root.exists());

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines = stdout.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 177);
    let expected_final = format!(
        "dry-run listed 170 executable RunUnits and 6 explicit skips: {}",
        output_root.display()
    );
    assert_eq!(lines.last().copied(), Some(expected_final.as_str()));

    let run_lines = lines
        .iter()
        .copied()
        .filter(|line| line.starts_with('[') && !line.starts_with("[skip unsupported"))
        .collect::<Vec<_>>();
    let skip_lines = lines
        .iter()
        .copied()
        .filter(|line| line.starts_with("[skip unsupported"))
        .collect::<Vec<_>>();
    assert_eq!(run_lines.len(), 170);
    assert_eq!(skip_lines.len(), 6);

    for backend in ["rustkv", "leveldb"] {
        for workload in ["random_get", "range_scan", "single_delete", "batch_delete"] {
            for (records, scale) in [
                ("10000", "1w"),
                ("100000", "10w"),
                ("1000000", "100w"),
                ("10000000", "1000w"),
            ] {
                let expected = format!(
                    " {backend} {workload} records={records} threads=1 output={}/{}_{}_{}_t1",
                    output_root.display(),
                    backend,
                    workload,
                    scale
                );
                assert_eq!(
                    run_lines
                        .iter()
                        .filter(|line| line.contains(&expected))
                        .count(),
                    1,
                    "missing or duplicate dry-run entry: {expected}"
                );
            }
        }
    }
    assert!(
        run_lines
            .iter()
            .filter(|line| line.contains("threads=1 output="))
            .all(|line| !line.contains("single_put") && !line.contains("batch_put"))
    );

    for backend in ["rustkv", "leveldb"] {
        for workload in [
            "random_get",
            "range_scan",
            "single_put",
            "batch_put",
            "single_delete",
            "batch_delete",
        ] {
            for (records, scale) in [
                ("10000", "1w"),
                ("100000", "10w"),
                ("1000000", "100w"),
                ("10000000", "1000w"),
            ] {
                for threads in ["10", "100", "1000"] {
                    let unsupported = records == "10000"
                        && threads == "1000"
                        && matches!(workload, "range_scan" | "batch_put" | "batch_delete");
                    let expected = format!(
                        " {backend} {workload} records={records} threads={threads} output={}/{}_{}_{}_t{}",
                        output_root.display(),
                        backend,
                        workload,
                        scale,
                        threads
                    );
                    assert_eq!(
                        run_lines
                            .iter()
                            .filter(|line| line.contains(&expected))
                            .count(),
                        usize::from(!unsupported),
                        "unexpected executable count: {expected}"
                    );
                    if unsupported {
                        let expected_skip = format!(
                            "backend={backend} workload={workload} records={records} threads={threads}: only 100 requests"
                        );
                        assert_eq!(
                            skip_lines
                                .iter()
                                .filter(|line| line.contains(&expected_skip))
                                .count(),
                            1,
                            "missing or duplicate skip: {expected_skip}"
                        );
                    }
                }
            }
        }
    }

    let help = Command::new("sh")
        .arg(&script)
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("random_get, range_scan, single_delete, batch_delete"));
    assert!(help.contains("10000 (1w)"));
    assert!(help.contains("10000000 (1000w)"));
    assert!(help.contains("executes 170 valid RunUnits"));
    assert!(
        !Command::new("sh")
            .arg(&script)
            .args(["--dry-run", "--output-root", "relative"])
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn remaining_script_skips_only_complete_matching_results() {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/run_remaining_t1.sh");
    let temp = TempDirectory::new("remaining-resume");
    let output_root = temp.path().join("output");
    std::fs::create_dir(&output_root).unwrap();

    for backend in ["leveldb", "rustkv"] {
        for workload in ["random_get", "range_scan", "single_delete", "batch_delete"] {
            for (records, scale) in [
                ("10000", "1w"),
                ("100000", "10w"),
                ("1000000", "100w"),
                ("10000000", "1000w"),
            ] {
                write_complete_script_result(&output_root, backend, workload, records, scale, "1");
            }
        }
    }
    for backend in ["leveldb", "rustkv"] {
        for workload in [
            "random_get",
            "range_scan",
            "single_put",
            "batch_put",
            "single_delete",
            "batch_delete",
        ] {
            for (records, scale) in [
                ("10000", "1w"),
                ("100000", "10w"),
                ("1000000", "100w"),
                ("10000000", "1000w"),
            ] {
                for threads in ["10", "100", "1000"] {
                    if records == "10000"
                        && threads == "1000"
                        && matches!(workload, "range_scan" | "batch_put" | "batch_delete")
                    {
                        continue;
                    }
                    write_complete_script_result(
                        &output_root,
                        backend,
                        workload,
                        records,
                        scale,
                        threads,
                    );
                }
            }
        }
    }

    let output = Command::new("sh")
        .arg(&script)
        .arg("--output-root")
        .arg(&output_root)
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout.matches("skip completed").count(), 170);
    assert_eq!(stdout.matches("skip unsupported").count(), 6);
    assert!(stdout.contains("all 170 executable custom benchmarks completed; skipped=6"));

    let incomplete_root = temp.path().join("incomplete");
    std::fs::create_dir_all(incomplete_root.join("leveldb_random_get_1w_t1")).unwrap();
    let output = Command::new("sh")
        .arg(&script)
        .arg("--output-root")
        .arg(&incomplete_root)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("incomplete or mismatched")
    );
}

fn write_complete_script_result(
    output_root: &Path,
    backend: &str,
    workload: &str,
    records: &str,
    scale: &str,
    threads: &str,
) {
    let result = output_root.join(format!("{backend}_{workload}_{scale}_t{threads}"));
    std::fs::create_dir(&result).unwrap();
    std::fs::write(
        result.join("parameters.txt"),
        format!(
            "mode=custom\nformal_result=false\nrecord_count={records}\nbackend={backend}\nworkload={workload}\nthreads={threads}\n"
        ),
    )
    .unwrap();
    std::fs::write(result.join("result.md"), "complete\n").unwrap();
    let fields = [
        "custom", "test-id", records, "1024", "100", "100", "20260720", backend, workload, threads,
        "1", records, "1", "1", "", "1", "1", "1", "1", "0", "true", "", COMMIT, "clean", COMMIT,
    ];
    std::fs::write(
        result.join("result.csv"),
        format!("header\n{}\n", fields.join(",")),
    )
    .unwrap();
}

const fn expected_ops(workload: Workload) -> u64 {
    match workload {
        Workload::RangeScan | Workload::BatchPut | Workload::BatchDelete => 1,
        Workload::RandomGet | Workload::SinglePut | Workload::SingleDelete => 100,
    }
}
