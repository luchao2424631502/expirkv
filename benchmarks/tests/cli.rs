use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

use kv_bench::{BackendKind, CliCommand, CliError, Workload, parse_cli};

const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn temp_path(name: &str) -> PathBuf {
    std::fs::canonicalize("/tmp").unwrap().join(name)
}

#[test]
fn every_subcommand_parses_to_strong_types() {
    let help = kv_bench::help_text();
    assert!(!help.contains("kv_bench prepare"));
    assert!(help.contains("run-one"));
    assert!(help.contains("matrix"));
    assert!(help.contains("report"));
    assert!(help.contains("smoke"));
    assert!(!help.lines().any(|line| line.starts_with('+')));
    assert_eq!(
        parse_cli(args(&[
            "run-one",
            "--workspace",
            "/tmp/kv-run",
            "--csv",
            "/tmp/result.csv",
            "--backend",
            "leveldb",
            "--workload",
            "range_scan",
            "--threads",
            "1000",
            "--repetition",
            "4",
            "--rustkv-commit",
            COMMIT,
            "--environment-id",
            "mac.m1-test",
        ]))
        .unwrap(),
        CliCommand::RunOne {
            workspace: temp_path("kv-run"),
            csv: temp_path("result.csv"),
            backend: BackendKind::LevelDb,
            workload: Workload::RangeScan,
            thread_count: 1_000,
            repetition: 4,
            rustkv_commit: COMMIT.to_owned(),
            environment_id: "mac.m1-test".to_owned(),
        }
    );
    assert_eq!(
        parse_cli(args(&["matrix", "--dry-run"])).unwrap(),
        CliCommand::MatrixDryRun
    );
    assert!(matches!(
        parse_cli(args(&[
            "matrix",
            "--workspace",
            "/tmp/kv-matrix",
            "--csv",
            "/tmp/matrix.csv",
            "--rustkv-commit",
            COMMIT,
            "--environment-id",
            "mac-1",
            "--resume",
        ])),
        Ok(CliCommand::Matrix { resume: true, .. })
    ));
    assert_eq!(
        parse_cli(args(&[
            "report",
            "--csv",
            "/tmp/matrix.csv",
            "--output-dir",
            "/tmp/report"
        ]))
        .unwrap(),
        CliCommand::Report {
            csv: temp_path("matrix.csv"),
            output_directory: temp_path("report"),
        }
    );
    assert_eq!(
        parse_cli(args(&["smoke", "--output-dir", "/tmp/smoke"])).unwrap(),
        CliCommand::Smoke {
            output_directory: temp_path("smoke")
        }
    );
}

#[test]
fn missing_unknown_duplicate_and_positional_arguments_are_usage_errors() {
    for (input, expected) in [
        (vec!["prepare"], "unknown command"),
        (
            vec!["prepare", "--workspace", "/tmp/removed"],
            "unknown command",
        ),
        (vec!["run-one"], "missing required option --workspace"),
        (
            vec!["run-one", "unexpected-positional"],
            "unexpected positional argument",
        ),
        (
            vec!["run-one", "--unknown", "value"],
            "unknown option --unknown",
        ),
        (
            vec![
                "run-one",
                "--workspace",
                "/tmp/first",
                "--workspace",
                "/tmp/second",
            ],
            "duplicate option --workspace",
        ),
        (
            vec!["run-one", "--workspace"],
            "option --workspace requires a value",
        ),
        (
            vec!["run-one", "--workspace", "--csv"],
            "option --workspace requires a value",
        ),
        (vec!["report"], "missing required option --csv"),
        (vec!["smoke"], "missing required option --output-dir"),
        (vec!["matrix", "--unknown"], "unknown option --unknown"),
        (
            vec!["matrix", "--resume", "--resume"],
            "duplicate flag --resume",
        ),
        (
            vec!["matrix", "--dry-run", "--resume"],
            "unknown option --dry-run",
        ),
    ] {
        assert!(
            matches!(parse_cli(args(&input)), Err(CliError::Usage(message)) if message.contains(expected)),
            "input {input:?} did not fail through {expected:?}"
        );
    }
}

#[test]
fn invalid_typed_values_and_conflicting_paths_are_rejected() {
    let base = [
        "run-one",
        "--workspace",
        "/tmp/run",
        "--csv",
        "/tmp/results.csv",
        "--backend",
        "rustkv",
        "--workload",
        "random_get",
        "--threads",
        "1",
        "--repetition",
        "0",
        "--rustkv-commit",
        COMMIT,
        "--environment-id",
        "mac",
    ];
    for (position, invalid) in [
        (6, "rocksdb"),
        (8, "get"),
        (10, "2"),
        (12, "5"),
        (14, "short"),
        (16, "bad id"),
    ] {
        let mut changed = base;
        changed[position] = invalid;
        assert!(parse_cli(args(&changed)).is_err(), "accepted {invalid}");
    }
    let mut relative = base;
    relative[2] = "relative";
    assert!(parse_cli(args(&relative)).is_err());
    let mut conflict = base;
    conflict[4] = "/tmp/run/results.csv";
    assert!(parse_cli(args(&conflict)).is_err());

    let conflict_root = std::env::temp_dir().join(format!("kv-bench-path-{}", std::process::id()));
    let workspace = conflict_root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace_text = workspace.to_str().unwrap();
    let csv = workspace.join("raw.csv");
    let csv_text = csv.to_str().unwrap();
    let mut actual_conflict = base;
    actual_conflict[2] = workspace_text;
    actual_conflict[4] = csv_text;
    assert!(matches!(
        parse_cli(args(&actual_conflict)),
        Err(CliError::Usage(message)) if message.contains("paths conflict")
    ));
    std::fs::remove_dir_all(conflict_root).unwrap();
}

#[test]
fn dry_run_binary_outputs_exactly_240_unique_ids_and_is_deterministic() {
    let binary = env!("CARGO_BIN_EXE_kv_bench");
    let first = Command::new(binary)
        .args(["matrix", "--dry-run"])
        .output()
        .unwrap();
    let second = Command::new(binary)
        .args(["matrix", "--dry-run"])
        .output()
        .unwrap();
    assert!(first.status.success());
    assert_eq!(first.stdout, second.stdout);
    let stdout = String::from_utf8(first.stdout).unwrap();
    let lines = stdout.lines().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(stdout.lines().count(), 240);
    assert_eq!(lines.len(), 240);
}

#[test]
fn binary_uses_exit_2_for_usage_and_exit_1_for_runtime_failure() {
    let binary = env!("CARGO_BIN_EXE_kv_bench");
    let usage = Command::new(binary).arg("unknown").status().unwrap();
    assert_eq!(usage.code(), Some(2));

    let existing = std::env::temp_dir().join(format!("kv-bench-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&existing);
    std::fs::create_dir(&existing).unwrap();
    let runtime = Command::new(binary)
        .args(["smoke", "--output-dir"])
        .arg(&existing)
        .status()
        .unwrap();
    assert_eq!(runtime.code(), Some(1));
    std::fs::remove_dir_all(existing).unwrap();
}
