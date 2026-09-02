//! Direct per-RunUnit Load -> Run lifecycle. Nothing in this module restores
//! or copies a prepared database.

use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::fs::OpenLease;
use crate::{
    BackendKind, BatchItem, BenchBackend, BenchConfig, BenchmarkWorkspace, DatabaseDirectory,
    FsError, LevelDbBackend, RunResult, RunUnit, RustKvBackend, Trace, ValidationSummary, Workload,
    WorkloadRun, encode_key, fixed_value, prewarm_full_dataset, validate_empty_dataset,
    validate_final_dataset, validate_full_dataset,
};

const LOAD_BATCH_RECORDS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum RunUnitStage {
    DirectoryCreated,
    LoadDatabaseOpened,
    LoadCompleted,
    LoadDatabaseClosed,
    InitialValidationDatabaseOpened,
    InitialStateValidated,
    InitialValidationDatabaseClosed,
    RunDatabaseOpened,
    ReadPrewarmCompleted,
    NoExtraPrewarm,
    RunCompleted,
    RunDatabaseClosed,
    FinalValidationDatabaseOpened,
    FinalStateValidated,
    FinalValidationDatabaseClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct RunUnitAudit {
    pub stages: Vec<RunUnitStage>,
    pub open_count: usize,
    pub loaded_records: u64,
    pub load_batches: u64,
    pub load_batch_sizes: Vec<usize>,
    pub initial_validation: Option<ValidationSummary>,
    pub prewarm: Option<ValidationSummary>,
    pub final_validation: Option<ValidationSummary>,
}

impl RunUnitAudit {
    fn new() -> Self {
        Self {
            stages: vec![RunUnitStage::DirectoryCreated],
            open_count: 0,
            loaded_records: 0,
            load_batches: 0,
            load_batch_sizes: Vec::new(),
            initial_validation: None,
            prewarm: None,
            final_validation: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum RunUnitFault {
    None,
    AfterLoadClosed,
    AfterInitialValidationClosed,
    CorruptBeforeInitialValidation,
    CorruptBeforeFinalValidation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunUnitExecutionError {
    stage: &'static str,
    message: String,
}

impl RunUnitExecutionError {
    fn new(stage: &'static str, error: impl fmt::Display) -> Self {
        Self {
            stage,
            message: error.to_string(),
        }
    }

    pub const fn stage(&self) -> &'static str {
        self.stage
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RunUnitExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} failed: {}", self.stage, self.message)
    }
}

impl Error for RunUnitExecutionError {}

#[derive(Debug)]
pub struct RunUnitAttempt {
    directory: DatabaseDirectory,
    audit: RunUnitAudit,
    result: Option<RunResult>,
    validation_success: bool,
    error_text: Option<String>,
}

impl RunUnitAttempt {
    pub fn audit(&self) -> &RunUnitAudit {
        &self.audit
    }

    pub fn result(&self) -> Option<&RunResult> {
        self.result.as_ref()
    }

    pub const fn validation_success(&self) -> bool {
        self.validation_success
    }

    pub fn error_text(&self) -> Option<&str> {
        self.error_text.as_deref()
    }

    #[doc(hidden)]
    pub fn path_for_test(&self) -> &std::path::Path {
        self.directory.path_for_test()
    }

    pub fn cleanup(self, workspace: &BenchmarkWorkspace) -> Result<(), FsError> {
        workspace.cleanup_run(&self.directory)
    }

    fn fail(&mut self, error: RunUnitExecutionError) {
        self.validation_success = false;
        self.error_text = Some(error.to_string());
    }
}

pub(crate) fn execute_run_unit(
    workspace: &BenchmarkWorkspace,
    config: &BenchConfig,
    unit: RunUnit,
    trace: &Trace,
) -> Result<RunUnitAttempt, RunUnitExecutionError> {
    execute_run_unit_with_fault(workspace, config, unit, trace, RunUnitFault::None)
}

#[doc(hidden)]
pub fn execute_run_unit_with_fault(
    workspace: &BenchmarkWorkspace,
    config: &BenchConfig,
    unit: RunUnit,
    trace: &Trace,
    fault: RunUnitFault,
) -> Result<RunUnitAttempt, RunUnitExecutionError> {
    validate_trace_identity(config, unit, trace)?;
    let directory = workspace
        .create_empty_run(unit.backend, unit.id().as_str())
        .map_err(|error| RunUnitExecutionError::new("create RunUnit directory", error))?;
    let mut attempt = RunUnitAttempt {
        directory,
        audit: RunUnitAudit::new(),
        result: None,
        validation_success: false,
        error_text: None,
    };

    if let Err(error) = load_and_close(
        &attempt.directory,
        config,
        unit.workload,
        &mut attempt.audit,
    ) {
        attempt.fail(error);
        return Ok(attempt);
    }
    if fault == RunUnitFault::AfterLoadClosed {
        attempt.fail(RunUnitExecutionError::new(
            "injected after Load close",
            "test fault",
        ));
        return Ok(attempt);
    }
    if fault == RunUnitFault::CorruptBeforeInitialValidation
        && let Err(error) = corrupt_first_record_for_test(&attempt.directory, config)
    {
        attempt.fail(error);
        return Ok(attempt);
    }

    if let Err(error) = validate_initial_and_close(
        &attempt.directory,
        config,
        unit.workload,
        &mut attempt.audit,
    ) {
        attempt.fail(error);
        return Ok(attempt);
    }
    if fault == RunUnitFault::AfterInitialValidationClosed {
        attempt.fail(RunUnitExecutionError::new(
            "injected after initial validation close",
            "test fault",
        ));
        return Ok(attempt);
    }

    match run_and_close(&attempt.directory, config, unit, trace, &mut attempt.audit) {
        Ok(result) => attempt.result = Some(result),
        Err(error) => {
            attempt.fail(error);
            return Ok(attempt);
        }
    }

    if fault == RunUnitFault::CorruptBeforeFinalValidation
        && let Err(error) = corrupt_first_record_for_test(&attempt.directory, config)
    {
        attempt.fail(error);
        return Ok(attempt);
    }
    match validate_final_and_close(
        &attempt.directory,
        config,
        unit.workload,
        &mut attempt.audit,
    ) {
        Ok(()) => attempt.validation_success = true,
        Err(error) => attempt.fail(error),
    }
    Ok(attempt)
}

fn validate_trace_identity(
    config: &BenchConfig,
    unit: RunUnit,
    trace: &Trace,
) -> Result<(), RunUnitExecutionError> {
    if unit.mode != config.mode()
        || trace.workload() != unit.workload
        || trace.repetition() != unit.repetition
        || !trace.was_generated_from(config)
    {
        return Err(RunUnitExecutionError::new(
            "validate Trace identity",
            "RunUnit, BenchConfig, and Trace do not describe the same fixed work",
        ));
    }
    Ok(())
}

fn load_and_close(
    directory: &DatabaseDirectory,
    config: &BenchConfig,
    workload: Workload,
    audit: &mut RunUnitAudit,
) -> Result<(), RunUnitExecutionError> {
    let database = open_database(directory, config)
        .map_err(|error| RunUnitExecutionError::new("open Load database", error))?;
    audit.open_count += 1;
    audit.stages.push(RunUnitStage::LoadDatabaseOpened);
    if requires_full_initial_state(workload) {
        let (records, batch_sizes) = load_full_dataset(database.backend(), config)?;
        audit.loaded_records = records;
        audit.load_batches = batch_sizes.len() as u64;
        audit.load_batch_sizes = batch_sizes;
    }
    audit.stages.push(RunUnitStage::LoadCompleted);
    drop(database);
    audit.stages.push(RunUnitStage::LoadDatabaseClosed);
    Ok(())
}

fn validate_initial_and_close(
    directory: &DatabaseDirectory,
    config: &BenchConfig,
    workload: Workload,
    audit: &mut RunUnitAudit,
) -> Result<(), RunUnitExecutionError> {
    let database = open_existing_database(directory, config)
        .map_err(|error| RunUnitExecutionError::new("reopen for initial validation", error))?;
    audit.open_count += 1;
    audit
        .stages
        .push(RunUnitStage::InitialValidationDatabaseOpened);
    let summary = if requires_full_initial_state(workload) {
        validate_full_dataset(database.backend(), config)
    } else {
        validate_empty_dataset(database.backend())
    }
    .map_err(|error| RunUnitExecutionError::new("validate initial state", error))?;
    audit.initial_validation = Some(summary);
    audit.stages.push(RunUnitStage::InitialStateValidated);
    drop(database);
    audit
        .stages
        .push(RunUnitStage::InitialValidationDatabaseClosed);
    Ok(())
}

fn run_and_close(
    directory: &DatabaseDirectory,
    config: &BenchConfig,
    unit: RunUnit,
    trace: &Trace,
    audit: &mut RunUnitAudit,
) -> Result<RunResult, RunUnitExecutionError> {
    let database = open_existing_database(directory, config)
        .map_err(|error| RunUnitExecutionError::new("open formal Run database", error))?;
    audit.open_count += 1;
    audit.stages.push(RunUnitStage::RunDatabaseOpened);
    if matches!(unit.workload, Workload::RandomGet | Workload::RangeScan) {
        let summary = prewarm_full_dataset(database.backend(), config)
            .map_err(|error| RunUnitExecutionError::new("prewarm read workload", error))?;
        audit.prewarm = Some(summary);
        audit.stages.push(RunUnitStage::ReadPrewarmCompleted);
    } else {
        audit.stages.push(RunUnitStage::NoExtraPrewarm);
    }
    let result = WorkloadRun::new(
        config,
        unit.backend,
        database.shared_backend(),
        trace,
        unit.thread_count,
    )
    .execute();
    audit.stages.push(RunUnitStage::RunCompleted);
    drop(database);
    audit.stages.push(RunUnitStage::RunDatabaseClosed);
    Ok(result)
}

fn validate_final_and_close(
    directory: &DatabaseDirectory,
    config: &BenchConfig,
    workload: Workload,
    audit: &mut RunUnitAudit,
) -> Result<(), RunUnitExecutionError> {
    let database = open_existing_database(directory, config)
        .map_err(|error| RunUnitExecutionError::new("reopen for final validation", error))?;
    audit.open_count += 1;
    audit
        .stages
        .push(RunUnitStage::FinalValidationDatabaseOpened);
    let summary = validate_final_dataset(database.backend(), config, workload)
        .map_err(|error| RunUnitExecutionError::new("validate final state", error))?;
    audit.final_validation = Some(summary);
    audit.stages.push(RunUnitStage::FinalStateValidated);
    drop(database);
    audit
        .stages
        .push(RunUnitStage::FinalValidationDatabaseClosed);
    Ok(())
}

fn load_full_dataset(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
) -> Result<(u64, Vec<usize>), RunUnitExecutionError> {
    let value = fixed_value(config);
    let mut start = 0_u64;
    let mut batch_sizes = Vec::new();
    while start < config.record_count() {
        let end = start
            .checked_add(LOAD_BATCH_RECORDS)
            .ok_or_else(|| RunUnitExecutionError::new("Load", "record range overflowed u64"))?
            .min(config.record_count());
        let keys = (start..end)
            .map(|id| {
                encode_key(config, id).map_err(|error| {
                    RunUnitExecutionError::new(
                        "Load",
                        format!("key {id} cannot be encoded: {error:?}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let items = keys
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect::<Vec<_>>();
        backend
            .write_batch(&items)
            .map_err(|error| RunUnitExecutionError::new("Load write_batch", error))?;
        batch_sizes.push(items.len());
        start = end;
    }
    Ok((start, batch_sizes))
}

fn corrupt_first_record_for_test(
    directory: &DatabaseDirectory,
    config: &BenchConfig,
) -> Result<(), RunUnitExecutionError> {
    let database = open_existing_database(directory, config)
        .map_err(|error| RunUnitExecutionError::new("open injected corruption database", error))?;
    let key = encode_key(config, 0).map_err(|error| {
        RunUnitExecutionError::new("encode injected corruption key", format!("{error:?}"))
    })?;
    let mut value = fixed_value(config);
    value[0] ^= 0xff;
    database
        .backend()
        .put(&key, &value)
        .map_err(|error| RunUnitExecutionError::new("inject wrong Value", error))?;
    drop(database);
    Ok(())
}

const fn requires_full_initial_state(workload: Workload) -> bool {
    matches!(
        workload,
        Workload::RandomGet | Workload::RangeScan | Workload::SingleDelete | Workload::BatchDelete
    )
}

struct ManagedDatabase<'a> {
    backend: Option<Arc<dyn BenchBackend>>,
    _lease: OpenLease,
    _directory: PhantomData<&'a DatabaseDirectory>,
}

impl ManagedDatabase<'_> {
    fn backend(&self) -> &dyn BenchBackend {
        self.backend
            .as_deref()
            .expect("Backend lives until managed close")
    }

    fn shared_backend(&self) -> Arc<dyn BenchBackend> {
        Arc::clone(
            self.backend
                .as_ref()
                .expect("Backend lives until managed close"),
        )
    }
}

impl Drop for ManagedDatabase<'_> {
    fn drop(&mut self) {
        // Release the physical database before the directory lease permits a
        // subsequent independent Open.
        self.backend.take();
    }
}

fn open_database<'a>(
    directory: &'a DatabaseDirectory,
    config: &BenchConfig,
) -> Result<ManagedDatabase<'a>, Box<dyn Error + Send + Sync>> {
    let lease = directory.begin_open()?;
    let backend: Arc<dyn BenchBackend> = match directory.backend_kind() {
        BackendKind::RustKv => Arc::new(RustKvBackend::open(directory.path(), config)?),
        BackendKind::LevelDb => Arc::new(LevelDbBackend::open(directory.path(), config)?),
    };
    Ok(ManagedDatabase {
        backend: Some(backend),
        _lease: lease,
        _directory: PhantomData,
    })
}

fn open_existing_database<'a>(
    directory: &'a DatabaseDirectory,
    config: &BenchConfig,
) -> Result<ManagedDatabase<'a>, Box<dyn Error + Send + Sync>> {
    directory.require_existing_database()?;
    open_database(directory, config)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::load_full_dataset;
    use crate::{
        BackendError, BackendKind, BackendOperation, BackendResult, BatchItem, BenchBackend,
        BenchConfig, GetResult, ScanRequest, ScanResult, decode_key, fixed_value,
    };

    type RecordedBatch = Vec<(Vec<u8>, Vec<u8>)>;

    struct RecordingLoadBackend {
        batches: Mutex<Vec<RecordedBatch>>,
        fail_on_call: Option<usize>,
    }

    impl RecordingLoadBackend {
        fn successful() -> Self {
            Self {
                batches: Mutex::new(Vec::new()),
                fail_on_call: None,
            }
        }

        fn failing_on(call: usize) -> Self {
            Self {
                batches: Mutex::new(Vec::new()),
                fail_on_call: Some(call),
            }
        }

        fn recorded(&self) -> Vec<RecordedBatch> {
            self.batches.lock().unwrap().clone()
        }
    }

    impl BenchBackend for RecordingLoadBackend {
        fn get(&self, _key: &[u8]) -> BackendResult<GetResult> {
            panic!("Load must not issue Get")
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> BackendResult<()> {
            panic!("Load must not issue individual Put")
        }

        fn delete(&self, _key: &[u8]) -> BackendResult<()> {
            panic!("Load must not issue Delete")
        }

        fn write_batch(&self, items: &[BatchItem<'_>]) -> BackendResult<()> {
            let batch = items
                .iter()
                .map(|item| match item {
                    BatchItem::Put { key, value } => Ok((key.to_vec(), value.to_vec())),
                    BatchItem::Delete { .. } => Err(BackendError::new(
                        BackendKind::RustKv,
                        BackendOperation::WriteBatch,
                        "Load emitted Delete",
                    )),
                })
                .collect::<BackendResult<RecordedBatch>>()?;
            let mut batches = self.batches.lock().unwrap();
            batches.push(batch);
            if self.fail_on_call == Some(batches.len()) {
                return Err(BackendError::new(
                    BackendKind::RustKv,
                    BackendOperation::WriteBatch,
                    "injected Load write failure",
                ));
            }
            Ok(())
        }

        fn iterator_scan(&self, _request: ScanRequest<'_>) -> BackendResult<ScanResult> {
            panic!("Load must not issue Iterator scan")
        }
    }

    #[test]
    fn load_batches_are_contiguous_increasing_puts_with_the_frozen_value() {
        let config = BenchConfig::test_only(2_500, 100, 100, 100, 20);
        let backend = RecordingLoadBackend::successful();
        let (records, batch_sizes) = load_full_dataset(&backend, &config).unwrap();
        assert_eq!(records, 2_500);
        assert_eq!(batch_sizes, [1_000, 1_000, 500]);

        let batches = backend.recorded();
        assert_eq!(batches.len(), 3);
        let expected_value = fixed_value(&config);
        let mut expected_id = 0_u64;
        for batch in batches {
            for (key, value) in batch {
                assert_eq!(decode_key(&config, &key).unwrap(), expected_id);
                assert_eq!(value, expected_value);
                expected_id += 1;
            }
        }
        assert_eq!(expected_id, config.record_count());
    }

    #[test]
    fn load_stops_at_the_first_backend_batch_error_without_issuing_a_later_batch() {
        let config = BenchConfig::test_only(2_500, 100, 100, 100, 20);
        let backend = RecordingLoadBackend::failing_on(2);
        let error = load_full_dataset(&backend, &config).unwrap_err();
        assert_eq!(error.stage(), "Load write_batch");
        assert!(error.message().contains("injected Load write failure"));

        let batches = backend.recorded();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1_000);
        assert_eq!(batches[1].len(), 1_000);
        assert_eq!(decode_key(&config, &batches[0][0].0).unwrap(), 0);
        assert_eq!(decode_key(&config, &batches[1][0].0).unwrap(), 1_000);
        assert_eq!(decode_key(&config, &batches[1][999].0).unwrap(), 1_999);
    }
}
