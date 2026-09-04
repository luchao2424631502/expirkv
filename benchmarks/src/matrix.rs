//! Deterministic formal/smoke matrix construction and stable run identity.

use std::error::Error;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use crate::run_unit::{RunUnitAttempt, execute_run_unit};
use crate::{
    BackendKind, BenchConfig, BenchMode, BenchmarkWorkspace, CsvError, CsvFile, CsvRow, Trace,
    Workload,
};

pub const CONFIG_VERSION: &str = "rustkv-leveldb-v2";
pub const LEVELDB_COMMIT: &str = "99b3c03b3284f5886f9ef9a4ef703d57373e61be";

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(String);

impl RunId {
    pub fn new(
        mode: BenchMode,
        backend: BackendKind,
        workload: Workload,
        thread_count: usize,
        repetition: u32,
    ) -> Self {
        Self(format!(
            "{}-{}-{}-{}-t{}-r{}",
            CONFIG_VERSION,
            mode_as_str(mode),
            backend.as_str(),
            workload.as_str(),
            thread_count,
            repetition
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RunUnit {
    pub mode: BenchMode,
    pub backend: BackendKind,
    pub workload: Workload,
    pub thread_count: usize,
    pub repetition: u32,
    combination_index: usize,
}

impl RunUnit {
    pub fn formal(
        backend: BackendKind,
        workload: Workload,
        thread_count: usize,
        repetition: u32,
    ) -> Result<Self, MatrixError> {
        formal_matrix()
            .into_iter()
            .find(|unit| {
                unit.backend == backend
                    && unit.workload == workload
                    && unit.thread_count == thread_count
                    && unit.repetition == repetition
            })
            .ok_or_else(|| {
                MatrixError::InvalidUnit(format!(
                    "{} {} threads={} repetition={}",
                    backend.as_str(),
                    workload.as_str(),
                    thread_count,
                    repetition
                ))
            })
    }

    pub(crate) fn custom(
        backend: BackendKind,
        workload: Workload,
        thread_count: usize,
    ) -> Result<Self, MatrixError> {
        if !BenchConfig::formal()
            .thread_counts()
            .contains(&thread_count)
        {
            return Err(MatrixError::InvalidUnit(format!(
                "custom thread count {thread_count} is not one of 1, 10, 100, 1000"
            )));
        }
        Ok(Self {
            mode: BenchMode::Smoke,
            backend,
            workload,
            thread_count,
            repetition: 0,
            combination_index: 0,
        })
    }

    pub fn id(self) -> RunId {
        RunId::new(
            self.mode,
            self.backend,
            self.workload,
            self.thread_count,
            self.repetition,
        )
    }

    pub const fn combination_index(self) -> usize {
        self.combination_index
    }
}

pub fn formal_matrix() -> Vec<RunUnit> {
    build_matrix(&BenchConfig::formal(), &Workload::ALL)
}

pub fn smoke_matrix(config: &BenchConfig) -> Result<Vec<RunUnit>, MatrixError> {
    if config.mode() != BenchMode::Smoke {
        return Err(MatrixError::WrongMode);
    }
    let mut units = Vec::with_capacity(6 * 2 * 2);
    for (workload_index, workload) in Workload::ALL.into_iter().enumerate() {
        for (thread_index, thread_count) in [1_usize, 10].into_iter().enumerate() {
            let combination_index = workload_index * 2 + thread_index;
            let order = if combination_index.is_multiple_of(2) {
                [BackendKind::RustKv, BackendKind::LevelDb]
            } else {
                [BackendKind::LevelDb, BackendKind::RustKv]
            };
            for backend in order {
                units.push(RunUnit {
                    mode: BenchMode::Smoke,
                    backend,
                    workload,
                    thread_count,
                    repetition: 0,
                    combination_index,
                });
            }
        }
    }
    Ok(units)
}

fn build_matrix(config: &BenchConfig, workloads: &[Workload]) -> Vec<RunUnit> {
    let mut units = Vec::with_capacity(
        workloads.len() * config.thread_counts().len() * config.repetitions() as usize * 2,
    );
    for (workload_index, workload) in workloads.iter().copied().enumerate() {
        for (thread_index, thread_count) in config.thread_counts().iter().copied().enumerate() {
            let combination_index = workload_index * config.thread_counts().len() + thread_index;
            for repetition in 0..config.repetitions() {
                let order = if (combination_index + repetition as usize).is_multiple_of(2) {
                    [BackendKind::RustKv, BackendKind::LevelDb]
                } else {
                    [BackendKind::LevelDb, BackendKind::RustKv]
                };
                for backend in order {
                    units.push(RunUnit {
                        mode: config.mode(),
                        backend,
                        workload,
                        thread_count,
                        repetition,
                        combination_index,
                    });
                }
            }
        }
    }
    units
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatrixError {
    WrongMode,
    InvalidBackend(String),
    InvalidWorkload(String),
    InvalidMode(String),
    InvalidRunId(String),
    InvalidUnit(String),
}

impl fmt::Display for MatrixError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid benchmark matrix value: {self:?}")
    }
}

impl Error for MatrixError {}

impl FromStr for BackendKind {
    type Err = MatrixError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rustkv" => Ok(Self::RustKv),
            "leveldb" => Ok(Self::LevelDb),
            _ => Err(MatrixError::InvalidBackend(value.to_owned())),
        }
    }
}

impl FromStr for Workload {
    type Err = MatrixError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "random_get" => Ok(Self::RandomGet),
            "range_scan" => Ok(Self::RangeScan),
            "single_put" => Ok(Self::SinglePut),
            "batch_put" => Ok(Self::BatchPut),
            "single_delete" => Ok(Self::SingleDelete),
            "batch_delete" => Ok(Self::BatchDelete),
            _ => Err(MatrixError::InvalidWorkload(value.to_owned())),
        }
    }
}

pub const fn mode_as_str(mode: BenchMode) -> &'static str {
    match mode {
        BenchMode::Formal => "formal",
        BenchMode::Smoke => "smoke",
    }
}

pub fn parse_mode(value: &str) -> Result<BenchMode, MatrixError> {
    match value {
        "formal" => Ok(BenchMode::Formal),
        "smoke" => Ok(BenchMode::Smoke),
        _ => Err(MatrixError::InvalidMode(value.to_owned())),
    }
}

pub fn validate_run_id(
    value: &str,
    mode: BenchMode,
    backend: BackendKind,
    workload: Workload,
    thread_count: usize,
    repetition: u32,
) -> Result<RunId, MatrixError> {
    let expected = RunId::new(mode, backend, workload, thread_count, repetition);
    if value == expected.as_str() {
        Ok(expected)
    } else {
        Err(MatrixError::InvalidRunId(value.to_owned()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionMetadata {
    pub rustkv_commit: String,
    pub leveldb_commit: String,
    pub environment_id: String,
}

impl ExecutionMetadata {
    pub fn formal(rustkv_commit: String, environment_id: String) -> Self {
        Self {
            rustkv_commit,
            leveldb_commit: LEVELDB_COMMIT.to_owned(),
            environment_id,
        }
    }

    pub fn smoke() -> Self {
        Self {
            rustkv_commit: "smoke-not-a-formal-result".to_owned(),
            leveldb_commit: LEVELDB_COMMIT.to_owned(),
            environment_id: "local-mac-smoke".to_owned(),
        }
    }
}

pub fn execute_units(
    workspace_path: &Path,
    csv_path: &Path,
    config: &BenchConfig,
    units: &[RunUnit],
    metadata: &ExecutionMetadata,
    resume: bool,
) -> Result<usize, MatrixExecutionError> {
    execute_units_with(
        workspace_path,
        csv_path,
        config,
        units,
        metadata,
        resume,
        ProductionExecution,
    )
}

trait ExecutionHook {
    fn execute(
        &mut self,
        workspace: &BenchmarkWorkspace,
        config: &BenchConfig,
        unit: RunUnit,
        trace: &Trace,
    ) -> Result<RunUnitAttempt, crate::RunUnitExecutionError>;

    fn before_cleanup(&mut self, _attempt: &RunUnitAttempt) {}
}

struct ProductionExecution;

impl ExecutionHook for ProductionExecution {
    fn execute(
        &mut self,
        workspace: &BenchmarkWorkspace,
        config: &BenchConfig,
        unit: RunUnit,
        trace: &Trace,
    ) -> Result<RunUnitAttempt, crate::RunUnitExecutionError> {
        execute_run_unit(workspace, config, unit, trace)
    }
}

fn execute_units_with<Hooks: ExecutionHook>(
    workspace_path: &Path,
    csv_path: &Path,
    config: &BenchConfig,
    units: &[RunUnit],
    metadata: &ExecutionMetadata,
    resume: bool,
    mut hooks: Hooks,
) -> Result<usize, MatrixExecutionError> {
    if units.iter().any(|unit| unit.mode != config.mode()) {
        return Err(MatrixExecutionError::WrongUnitMode);
    }
    let identity = crate::ResumeIdentity {
        mode: config.mode(),
        rustkv_commit: &metadata.rustkv_commit,
        leveldb_commit: &metadata.leveldb_commit,
        environment_id: &metadata.environment_id,
    };
    let mut csv = if resume {
        CsvFile::load_for_resume(csv_path, &identity)?
    } else {
        CsvFile::create(csv_path)?
    };
    if csv.rows().len() > units.len()
        || csv
            .rows()
            .iter()
            .zip(units)
            .any(|(row, unit)| row.run_id != unit.id())
    {
        return Err(MatrixExecutionError::ResumeSequenceMismatch);
    }
    let remaining = units
        .iter()
        .copied()
        .filter(|unit| !csv.contains(&unit.id()))
        .collect::<Vec<_>>();
    if remaining.is_empty() {
        return Ok(0);
    }

    let invocation_workspace = if workspace_path.exists() {
        select_sibling_workspace(workspace_path, if resume { "resume" } else { "run" })?
    } else {
        workspace_path.to_path_buf()
    };
    let workspace = BenchmarkWorkspace::create(invocation_workspace)?;
    let mut completed = 0;
    for unit in remaining {
        let executed = execute_one(&workspace, config, unit, metadata, &mut hooks);
        let valid = executed.row.is_effective();
        let run_id = executed.row.run_id.to_string();
        if let Err(error) = csv.append(executed.row) {
            if let Some(attempt) = executed.attempt {
                let _ = attempt.cleanup(&workspace);
            }
            return Err(error.into());
        }
        if let Some(attempt) = executed.attempt {
            hooks.before_cleanup(&attempt);
            attempt.cleanup(&workspace)?;
        }
        completed += 1;
        if !valid {
            return Err(MatrixExecutionError::RunFailed(run_id));
        }
    }
    Ok(completed)
}

fn select_sibling_workspace(
    requested: &Path,
    purpose: &str,
) -> Result<std::path::PathBuf, MatrixExecutionError> {
    for sequence in 1_u64..=u64::MAX {
        let mut candidate = requested.as_os_str().to_os_string();
        candidate.push(format!(".{purpose}-{sequence}"));
        let candidate = std::path::PathBuf::from(candidate);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(MatrixExecutionError::InvocationWorkspaceExhausted)
}

fn execute_one<Hooks: ExecutionHook>(
    workspace: &BenchmarkWorkspace,
    config: &BenchConfig,
    unit: RunUnit,
    metadata: &ExecutionMetadata,
    hooks: &mut Hooks,
) -> ExecutedUnit {
    let trace = match Trace::generate(config, unit.workload, unit.repetition) {
        Ok(trace) => trace,
        Err(error) => {
            return ExecutedUnit {
                row: failure_row(
                    unit,
                    None,
                    format!("Trace generation failed: {error:?}"),
                    metadata,
                ),
                attempt: None,
            };
        }
    };
    let attempt = match hooks.execute(workspace, config, unit, &trace) {
        Ok(attempt) => attempt,
        Err(error) => {
            return ExecutedUnit {
                row: failure_row(
                    unit,
                    None,
                    format!("RunUnit setup failed: {error}"),
                    metadata,
                ),
                attempt: None,
            };
        }
    };
    let row = CsvRow::from_run(
        unit,
        attempt.result(),
        attempt.validation_success(),
        attempt.error_text(),
        &metadata.rustkv_commit,
        &metadata.leveldb_commit,
        &metadata.environment_id,
    );
    ExecutedUnit {
        row,
        attempt: Some(attempt),
    }
}

struct ExecutedUnit {
    row: CsvRow,
    attempt: Option<RunUnitAttempt>,
}

fn failure_row(
    unit: RunUnit,
    result: Option<&crate::RunResult>,
    message: String,
    metadata: &ExecutionMetadata,
) -> CsvRow {
    CsvRow::from_run(
        unit,
        result,
        false,
        Some(&message),
        &metadata.rustkv_commit,
        &metadata.leveldb_commit,
        &metadata.environment_id,
    )
}

#[derive(Debug)]
pub enum MatrixExecutionError {
    WrongUnitMode,
    ResumeSequenceMismatch,
    InvocationWorkspaceExhausted,
    FileSystem(crate::FsError),
    Csv(CsvError),
    RunFailed(String),
}

impl From<crate::FsError> for MatrixExecutionError {
    fn from(error: crate::FsError) -> Self {
        Self::FileSystem(error)
    }
}

impl From<CsvError> for MatrixExecutionError {
    fn from(error: CsvError) -> Self {
        Self::Csv(error)
    }
}

impl fmt::Display for MatrixExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "benchmark matrix execution failed: {self:?}")
    }
}

impl Error for MatrixExecutionError {}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{ResumeIdentity, RunUnitFault, execute_run_unit_with_fault};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct FailAfterLoad;

    impl ExecutionHook for FailAfterLoad {
        fn execute(
            &mut self,
            workspace: &BenchmarkWorkspace,
            config: &BenchConfig,
            unit: RunUnit,
            trace: &Trace,
        ) -> Result<RunUnitAttempt, crate::RunUnitExecutionError> {
            execute_run_unit_with_fault(
                workspace,
                config,
                unit,
                trace,
                RunUnitFault::AfterLoadClosed,
            )
        }
    }

    struct RemoveBeforeCleanup<'a> {
        csv_path: &'a Path,
        observed_published_row: &'a Cell<bool>,
    }

    impl ExecutionHook for RemoveBeforeCleanup<'_> {
        fn execute(
            &mut self,
            workspace: &BenchmarkWorkspace,
            config: &BenchConfig,
            unit: RunUnit,
            trace: &Trace,
        ) -> Result<RunUnitAttempt, crate::RunUnitExecutionError> {
            execute_run_unit(workspace, config, unit, trace)
        }

        fn before_cleanup(&mut self, attempt: &RunUnitAttempt) {
            let csv = CsvFile::load(self.csv_path).unwrap();
            assert_eq!(csv.rows().len(), 1);
            assert!(csv.rows()[0].is_effective());
            self.observed_published_row.set(true);
            std::fs::remove_dir_all(attempt.path_for_test()).unwrap();
        }
    }

    #[test]
    fn a_real_failed_rununit_is_published_cleaned_and_rejected_by_resume() {
        let root = temp_root("failed-row");
        let workspace_path = root.join("workspace");
        let csv_path = root.join("raw.csv");
        let config = smoke_config();
        let unit = smoke_unit(&config, BackendKind::LevelDb, Workload::RandomGet);
        let result = execute_units_with(
            &workspace_path,
            &csv_path,
            &config,
            &[unit],
            &ExecutionMetadata::smoke(),
            false,
            FailAfterLoad,
        );
        assert!(matches!(result, Err(MatrixExecutionError::RunFailed(_))));

        let csv = CsvFile::load(&csv_path).unwrap();
        assert_eq!(csv.rows().len(), 1);
        let row = &csv.rows()[0];
        assert!(!row.is_effective());
        assert_eq!(row.completed_ops, 0);
        assert_eq!(row.error_count, 1);
        assert!(!row.validation_success);
        assert!(row.error_text.contains("after Load close"));
        assert!(std::fs::read_dir(&workspace_path).unwrap().next().is_none());
        assert!(matches!(
            CsvFile::load_for_resume(
                &csv_path,
                &ResumeIdentity {
                    mode: BenchMode::Smoke,
                    rustkv_commit: "smoke-not-a-formal-result",
                    leveldb_commit: LEVELDB_COMMIT,
                    environment_id: "local-mac-smoke",
                },
            ),
            Err(CsvError::FailedResumeRow(_))
        ));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn csv_is_published_before_cleanup_and_survives_a_cleanup_failure() {
        let root = temp_root("cleanup-order");
        let workspace_path = root.join("workspace");
        let csv_path = root.join("raw.csv");
        let config = smoke_config();
        let unit = smoke_unit(&config, BackendKind::RustKv, Workload::SinglePut);
        let observed_published_row = Cell::new(false);
        let result = execute_units_with(
            &workspace_path,
            &csv_path,
            &config,
            &[unit],
            &ExecutionMetadata::smoke(),
            false,
            RemoveBeforeCleanup {
                csv_path: &csv_path,
                observed_published_row: &observed_published_row,
            },
        );
        assert!(observed_published_row.get());
        assert!(matches!(result, Err(MatrixExecutionError::FileSystem(_))));
        let csv = CsvFile::load(&csv_path).unwrap();
        assert_eq!(csv.rows().len(), 1);
        assert!(csv.rows()[0].is_effective());
        assert!(std::fs::read_dir(&workspace_path).unwrap().next().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    fn smoke_config() -> BenchConfig {
        BenchConfig::test_only(1_000, 100, 100, 100, 20)
    }

    fn smoke_unit(config: &BenchConfig, backend: BackendKind, workload: Workload) -> RunUnit {
        smoke_matrix(config)
            .unwrap()
            .into_iter()
            .find(|unit| {
                unit.backend == backend && unit.workload == workload && unit.thread_count == 1
            })
            .unwrap()
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "kv-bench-matrix-unit-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        root
    }
}
