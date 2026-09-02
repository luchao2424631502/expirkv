//! Dependency-free, closed command-line surface for formal and smoke runs.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::{
    BackendKind, BenchConfig, ExecutionMetadata, RunUnit, Workload, execute_units, formal_matrix,
    generate_formal_report, generate_smoke_report, smoke_matrix,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliCommand {
    Help,
    Version,
    RunOne {
        workspace: PathBuf,
        csv: PathBuf,
        backend: BackendKind,
        workload: Workload,
        thread_count: usize,
        repetition: u32,
        rustkv_commit: String,
        environment_id: String,
    },
    MatrixDryRun,
    Matrix {
        workspace: PathBuf,
        csv: PathBuf,
        rustkv_commit: String,
        environment_id: String,
        resume: bool,
    },
    Report {
        csv: PathBuf,
        output_directory: PathBuf,
    },
    Smoke {
        output_directory: PathBuf,
    },
}

pub fn parse_cli<I>(arguments: I) -> Result<CliCommand, CliError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut arguments = arguments.into_iter();
    let Some(command) = arguments.next() else {
        return Err(CliError::Usage("a command is required".to_owned()));
    };
    let command = command
        .into_string()
        .map_err(|_| CliError::Usage("command is not valid UTF-8".to_owned()))?;
    let rest = arguments
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| CliError::Usage("argument is not valid UTF-8".to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match command.as_str() {
        "--help" | "-h" | "help" => require_empty(rest, CliCommand::Help),
        "--version" | "-V" => require_empty(rest, CliCommand::Version),
        "run-one" => {
            let parsed = ParsedOptions::new(
                rest,
                &[
                    "workspace",
                    "csv",
                    "backend",
                    "workload",
                    "threads",
                    "repetition",
                    "rustkv-commit",
                    "environment-id",
                ],
                &[],
            )?;
            let workspace = absolute_path(parsed.required("workspace")?, "workspace")?;
            let csv = absolute_path(parsed.required("csv")?, "csv")?;
            require_non_conflicting(&workspace, &csv)?;
            Ok(CliCommand::RunOne {
                workspace,
                csv,
                backend: parse_value(parsed.required("backend")?, "backend")?,
                workload: parse_value(parsed.required("workload")?, "workload")?,
                thread_count: parse_thread_count(parsed.required("threads")?)?,
                repetition: parse_repetition(parsed.required("repetition")?)?,
                rustkv_commit: parse_commit(parsed.required("rustkv-commit")?)?,
                environment_id: parse_environment_id(parsed.required("environment-id")?)?,
            })
        }
        "matrix" if rest == ["--dry-run"] => Ok(CliCommand::MatrixDryRun),
        "matrix" => {
            let parsed = ParsedOptions::new(
                rest,
                &["workspace", "csv", "rustkv-commit", "environment-id"],
                &["resume"],
            )?;
            let workspace = absolute_path(parsed.required("workspace")?, "workspace")?;
            let csv = absolute_path(parsed.required("csv")?, "csv")?;
            require_non_conflicting(&workspace, &csv)?;
            Ok(CliCommand::Matrix {
                workspace,
                csv,
                rustkv_commit: parse_commit(parsed.required("rustkv-commit")?)?,
                environment_id: parse_environment_id(parsed.required("environment-id")?)?,
                resume: parsed.flag("resume"),
            })
        }
        "report" => {
            let parsed = ParsedOptions::new(rest, &["csv", "output-dir"], &[])?;
            let csv = absolute_path(parsed.required("csv")?, "csv")?;
            let output_directory = absolute_path(parsed.required("output-dir")?, "output-dir")?;
            require_non_conflicting(&csv, &output_directory)?;
            Ok(CliCommand::Report {
                csv,
                output_directory,
            })
        }
        "smoke" => {
            let parsed = ParsedOptions::new(rest, &["output-dir"], &[])?;
            Ok(CliCommand::Smoke {
                output_directory: absolute_path(parsed.required("output-dir")?, "output-dir")?,
            })
        }
        _ => Err(CliError::Usage(format!("unknown command {command:?}"))),
    }
}

pub fn execute_cli(command: CliCommand) -> Result<String, CliError> {
    match command {
        CliCommand::Help => Ok(crate::help_text().to_owned()),
        CliCommand::Version => Ok(format!("{}\n", crate::version_text())),
        CliCommand::RunOne {
            workspace,
            csv,
            backend,
            workload,
            thread_count,
            repetition,
            rustkv_commit,
            environment_id,
        } => {
            let unit = RunUnit::formal(backend, workload, thread_count, repetition)
                .map_err(|error| CliError::Usage(error.to_string()))?;
            let metadata = ExecutionMetadata::formal(rustkv_commit, environment_id);
            execute_units(
                &workspace,
                &csv,
                &BenchConfig::formal(),
                &[unit],
                &metadata,
                false,
            )
            .map_err(|error| CliError::Runtime(error.to_string()))?;
            Ok(format!("mode=formal completed {}\n", unit.id()))
        }
        CliCommand::MatrixDryRun => {
            let mut output = String::new();
            for unit in formal_matrix() {
                output.push_str(unit.id().as_str());
                output.push('\n');
            }
            Ok(output)
        }
        CliCommand::Matrix {
            workspace,
            csv,
            rustkv_commit,
            environment_id,
            resume,
        } => {
            let units = formal_matrix();
            let metadata = ExecutionMetadata::formal(rustkv_commit, environment_id);
            let completed = execute_units(
                &workspace,
                &csv,
                &BenchConfig::formal(),
                &units,
                &metadata,
                resume,
            )
            .map_err(|error| CliError::Runtime(error.to_string()))?;
            Ok(format!(
                "mode=formal completed={completed} total={} csv={}\n",
                units.len(),
                csv.display()
            ))
        }
        CliCommand::Report {
            csv,
            output_directory,
        } => {
            let report = generate_formal_report(csv, output_directory)
                .map_err(|error| CliError::Runtime(error.to_string()))?;
            Ok(format!("mode=formal report={}\n", report.display()))
        }
        CliCommand::Smoke { output_directory } => execute_smoke(&output_directory),
    }
}

fn execute_smoke(output_directory: &Path) -> Result<String, CliError> {
    if output_directory.exists() {
        return Err(CliError::Runtime(format!(
            "smoke output already exists: {}",
            output_directory.display()
        )));
    }
    let parent = output_directory
        .parent()
        .ok_or_else(|| CliError::Runtime("smoke output directory has no parent".to_owned()))?;
    fs::create_dir(output_directory).map_err(|error| {
        CliError::Runtime(format!(
            "create smoke output {} failed: {error}",
            output_directory.display()
        ))
    })?;
    let output_directory = fs::canonicalize(output_directory).map_err(|error| {
        CliError::Runtime(format!(
            "canonicalize smoke output {} failed: {error}",
            output_directory.display()
        ))
    })?;
    let config = BenchConfig::test_only(1_000, 100, 100, 100, 20);
    let units = smoke_matrix(&config).map_err(|error| CliError::Runtime(error.to_string()))?;
    let workspace = output_directory.join("workspace");
    let csv = output_directory.join("raw-smoke.csv");
    execute_units(
        &workspace,
        &csv,
        &config,
        &units,
        &ExecutionMetadata::smoke(),
        false,
    )
    .map_err(|error| CliError::Runtime(error.to_string()))?;
    let report = generate_smoke_report(&csv, output_directory.join("report"))
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| CliError::Runtime(format!("sync smoke output parent failed: {error}")))?;
    Ok(format!(
        "mode=smoke runs={} csv={} report={}\n",
        units.len(),
        csv.display(),
        report.display()
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliError {
    Usage(String),
    Runtime(String),
}

impl CliError {
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) => 2,
            Self::Runtime(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "usage error: {message}"),
            Self::Runtime(message) => write!(formatter, "benchmark failed: {message}"),
        }
    }
}

impl Error for CliError {}

struct ParsedOptions {
    values: BTreeMap<String, String>,
    flags: BTreeSet<String>,
}

impl ParsedOptions {
    fn new(
        arguments: Vec<String>,
        allowed_values: &[&str],
        allowed_flags: &[&str],
    ) -> Result<Self, CliError> {
        let allowed_values = allowed_values.iter().copied().collect::<BTreeSet<_>>();
        let allowed_flags = allowed_flags.iter().copied().collect::<BTreeSet<_>>();
        let mut values = BTreeMap::new();
        let mut flags = BTreeSet::new();
        let mut index = 0;
        while index < arguments.len() {
            let argument = &arguments[index];
            let Some(name) = argument.strip_prefix("--") else {
                return Err(CliError::Usage(format!(
                    "unexpected positional argument {argument:?}"
                )));
            };
            if allowed_flags.contains(name) {
                if !flags.insert(name.to_owned()) {
                    return Err(CliError::Usage(format!("duplicate flag --{name}")));
                }
                index += 1;
            } else if allowed_values.contains(name) {
                let value = arguments
                    .get(index + 1)
                    .ok_or_else(|| CliError::Usage(format!("option --{name} requires a value")))?;
                if value.starts_with("--") {
                    return Err(CliError::Usage(format!("option --{name} requires a value")));
                }
                if values.insert(name.to_owned(), value.clone()).is_some() {
                    return Err(CliError::Usage(format!("duplicate option --{name}")));
                }
                index += 2;
            } else {
                return Err(CliError::Usage(format!("unknown option --{name}")));
            }
        }
        Ok(Self { values, flags })
    }

    fn required(&self, name: &str) -> Result<&str, CliError> {
        self.values
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| CliError::Usage(format!("missing required option --{name}")))
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.contains(name)
    }
}

fn require_empty(arguments: Vec<String>, command: CliCommand) -> Result<CliCommand, CliError> {
    if arguments.is_empty() {
        Ok(command)
    } else {
        Err(CliError::Usage(
            "help/version do not accept additional arguments".to_owned(),
        ))
    }
}

fn absolute_path(value: &str, name: &str) -> Result<PathBuf, CliError> {
    let path = PathBuf::from(value);
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(CliError::Usage(format!(
            "--{name} must be an absolute non-root path"
        )));
    }
    let parent = path
        .parent()
        .expect("absolute non-root path has a parent directory");
    let parent = fs::canonicalize(parent).map_err(|error| {
        CliError::Usage(format!(
            "--{name} parent {} cannot be resolved: {error}",
            parent.display()
        ))
    })?;
    Ok(parent.join(path.file_name().expect("checked above")))
}

fn require_non_conflicting(left: &Path, right: &Path) -> Result<(), CliError> {
    if left == right || left.starts_with(right) || right.starts_with(left) {
        return Err(CliError::Usage(format!(
            "paths conflict: {} and {}",
            left.display(),
            right.display()
        )));
    }
    Ok(())
}

fn parse_value<T: FromStr>(value: &str, name: &str) -> Result<T, CliError>
where
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|error| CliError::Usage(format!("invalid --{name}: {error}")))
}

fn parse_thread_count(value: &str) -> Result<usize, CliError> {
    let count = parse_value(value, "threads")?;
    if BenchConfig::formal().thread_counts().contains(&count) {
        Ok(count)
    } else {
        Err(CliError::Usage(
            "--threads must be one of 1, 10, 100, 1000".to_owned(),
        ))
    }
}

fn parse_repetition(value: &str) -> Result<u32, CliError> {
    let repetition = parse_value(value, "repetition")?;
    if repetition < BenchConfig::formal().repetitions() {
        Ok(repetition)
    } else {
        Err(CliError::Usage("--repetition must be in 0..5".to_owned()))
    }
}

fn parse_commit(value: &str) -> Result<String, CliError> {
    if value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(value.to_owned())
    } else {
        Err(CliError::Usage(
            "--rustkv-commit must be a full 40-character lowercase hex commit".to_owned(),
        ))
    }
}

fn parse_environment_id(value: &str) -> Result<String, CliError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Ok(value.to_owned())
    } else {
        Err(CliError::Usage(
            "--environment-id must use ASCII letters, digits, '.', '-' or '_'".to_owned(),
        ))
    }
}
