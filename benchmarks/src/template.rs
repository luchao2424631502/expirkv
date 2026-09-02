//! Closed template construction, independent run preparation, and reopen validation.

use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::fs::{DirectoryManifest, OpenLease};
use crate::{
    BackendError, BackendKind, BatchItem, BenchBackend, BenchConfig, BenchmarkWorkspace,
    DatabaseDirectory, FsError, LevelDbBackend, RunResult, RustKvBackend, Trace, ValidationError,
    ValidationSummary, Workload, WorkloadRun, encode_key, fixed_value, prewarm_full_dataset,
    validate_empty_dataset, validate_final_dataset, validate_full_dataset,
};

const TEMPLATE_LOAD_BATCH: u64 = 1_000;
const RUN_READY: u8 = 0;
const RUN_OPEN: u8 = 1;
const RUN_EXECUTED: u8 = 2;
const RUN_CLOSED_UNEXECUTED: u8 = 3;
const RUN_CLOSED_EXECUTED: u8 = 4;
const RUN_VALIDATING: u8 = 5;
const RUN_VALIDATED: u8 = 6;
const RUN_FAILED_TO_OPEN: u8 = 7;
const RUN_CLEANING: u8 = 8;
const RUN_CLEANED: u8 = 9;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TemplateErrorKind {
    InvalidConfiguration,
    MissingTemplate,
    TemplateMismatch,
    InvalidLifecycle,
    FileSystem,
    Backend,
    Validation,
    Injected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateError {
    kind: TemplateErrorKind,
    message: String,
}

impl TemplateError {
    fn new(kind: TemplateErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> TemplateErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TemplateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TemplateError {}

impl From<FsError> for TemplateError {
    fn from(error: FsError) -> Self {
        Self::new(TemplateErrorKind::FileSystem, error.to_string())
    }
}

impl From<BackendError> for TemplateError {
    fn from(error: BackendError) -> Self {
        Self::new(TemplateErrorKind::Backend, error.to_string())
    }
}

impl From<ValidationError> for TemplateError {
    fn from(error: ValidationError) -> Self {
        Self::new(TemplateErrorKind::Validation, error.to_string())
    }
}

#[derive(Debug)]
pub struct DatabaseTemplate {
    workspace: BenchmarkWorkspace,
    directory: DatabaseDirectory,
    config: BenchConfig,
    manifest: DirectoryManifest,
}

impl DatabaseTemplate {
    pub const fn backend_kind(&self) -> BackendKind {
        self.directory.backend_kind()
    }

    pub fn config(&self) -> &BenchConfig {
        &self.config
    }

    pub fn validate(&self) -> Result<ValidationSummary, TemplateError> {
        let label = self
            .workspace
            .next_internal_label(&format!("validate-{}", self.backend_kind().as_str()))?;
        let copy = self.restore(&self.workspace, &label)?;
        let validation = (|| {
            let database = open_existing_database(&copy, &self.config)?;
            validate_full_dataset(database.backend(), &self.config).map_err(Into::into)
        })();
        let cleanup = self
            .workspace
            .cleanup_run(&copy)
            .map_err(TemplateError::from);
        match (validation, cleanup) {
            (Ok(summary), Ok(())) => Ok(summary),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(error), Err(cleanup)) => Err(TemplateError::new(
                TemplateErrorKind::FileSystem,
                format!("{error}; validation copy cleanup also failed: {cleanup}"),
            )),
        }
    }

    #[doc(hidden)]
    pub fn path_for_test(&self) -> &std::path::Path {
        self.directory.path_for_test()
    }

    #[doc(hidden)]
    pub fn open_for_test(&self) -> Result<TemplateOpenGuard<'_>, TemplateError> {
        Ok(TemplateOpenGuard {
            database: open_existing_database(&self.directory, &self.config)?,
        })
    }

    fn restore(
        &self,
        workspace: &BenchmarkWorkspace,
        label: &str,
    ) -> Result<DatabaseDirectory, TemplateError> {
        if !workspace.same_workspace(&self.directory) {
            return Err(TemplateError::new(
                TemplateErrorKind::TemplateMismatch,
                "template belongs to another benchmark workspace",
            ));
        }
        workspace
            .restore_template(&self.directory, &self.manifest, label)
            .map_err(Into::into)
    }
}

#[doc(hidden)]
pub struct TemplateOpenGuard<'a> {
    database: ManagedDatabase<'a>,
}

impl TemplateOpenGuard<'_> {
    #[doc(hidden)]
    pub fn is_open_for_test(&self) -> bool {
        self.database.backend.is_some()
    }
}

#[derive(Debug)]
pub struct PreparedRun {
    directory: DatabaseDirectory,
    config: BenchConfig,
    workload: Workload,
    lifecycle: AtomicU8,
}

impl PreparedRun {
    pub const fn backend_kind(&self) -> BackendKind {
        self.directory.backend_kind()
    }

    pub const fn workload(&self) -> Workload {
        self.workload
    }

    pub fn config(&self) -> &BenchConfig {
        &self.config
    }

    /// Opens the run database. Read workloads complete their one full warmup
    /// scan before this returns; insert/delete workloads perform no warmup.
    pub fn open(&self) -> Result<OpenRun<'_>, TemplateError> {
        self.lifecycle
            .compare_exchange(RUN_READY, RUN_OPEN, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|actual| lifecycle_error("open", RUN_READY, actual))?;

        let opened = (|| {
            let database = match self.workload {
                Workload::SinglePut | Workload::BatchPut => {
                    open_database(&self.directory, &self.config)?
                }
                Workload::RandomGet
                | Workload::RangeScan
                | Workload::SingleDelete
                | Workload::BatchDelete => open_existing_database(&self.directory, &self.config)?,
            };
            let prewarm = match self.workload {
                Workload::RandomGet | Workload::RangeScan => {
                    Some(prewarm_full_dataset(database.backend(), &self.config)?)
                }
                Workload::SinglePut | Workload::BatchPut => {
                    validate_empty_dataset(database.backend())?;
                    None
                }
                Workload::SingleDelete | Workload::BatchDelete => None,
            };
            Ok(OpenRun {
                database: Some(database),
                config: &self.config,
                workload: self.workload,
                prewarm,
                lifecycle: &self.lifecycle,
            })
        })();
        if opened.is_err() {
            self.lifecycle.store(RUN_FAILED_TO_OPEN, Ordering::Release);
        }
        opened
    }

    /// Must be called after OpenRun has been consumed/dropped. It performs a
    /// genuine reopen followed by the full untimed terminal-state validation.
    pub fn validate_after_close(&self) -> Result<ValidationSummary, TemplateError> {
        self.lifecycle
            .compare_exchange(
                RUN_CLOSED_EXECUTED,
                RUN_VALIDATING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|actual| lifecycle_error("validate", RUN_CLOSED_EXECUTED, actual))?;
        let validation = (|| {
            let database = open_existing_database(&self.directory, &self.config)?;
            let summary = validate_final_dataset(database.backend(), &self.config, self.workload)?;
            drop(database);
            Ok(summary)
        })();
        self.lifecycle.store(RUN_VALIDATED, Ordering::Release);
        validation
    }

    pub fn cleanup(&self, workspace: &BenchmarkWorkspace) -> Result<(), TemplateError> {
        if !workspace.same_workspace(&self.directory) {
            return Err(TemplateError::new(
                TemplateErrorKind::TemplateMismatch,
                "run directory belongs to another benchmark workspace",
            ));
        }
        let lifecycle = self.lifecycle.load(Ordering::Acquire);
        if !matches!(
            lifecycle,
            RUN_READY | RUN_CLOSED_UNEXECUTED | RUN_VALIDATED | RUN_FAILED_TO_OPEN
        ) {
            return Err(lifecycle_error("cleanup", RUN_VALIDATED, lifecycle));
        }
        self.lifecycle
            .compare_exchange(lifecycle, RUN_CLEANING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|actual| lifecycle_error("cleanup", lifecycle, actual))?;
        match workspace.cleanup_run(&self.directory) {
            Ok(()) => {
                self.lifecycle.store(RUN_CLEANED, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                self.lifecycle.store(lifecycle, Ordering::Release);
                Err(error.into())
            }
        }
    }

    #[doc(hidden)]
    pub fn path_for_test(&self) -> &std::path::Path {
        self.directory.path_for_test()
    }
}

pub struct OpenRun<'a> {
    database: Option<ManagedDatabase<'a>>,
    config: &'a BenchConfig,
    workload: Workload,
    prewarm: Option<ValidationSummary>,
    lifecycle: &'a AtomicU8,
}

impl OpenRun<'_> {
    pub const fn prewarm_summary(&self) -> Option<ValidationSummary> {
        self.prewarm
    }

    pub fn execute(
        &mut self,
        trace: &Trace,
        thread_count: usize,
    ) -> Result<RunResult, TemplateError> {
        self.lifecycle
            .compare_exchange(RUN_OPEN, RUN_EXECUTED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|actual| lifecycle_error("execute", RUN_OPEN, actual))?;
        if trace.workload() != self.workload {
            return Err(TemplateError::new(
                TemplateErrorKind::TemplateMismatch,
                format!(
                    "prepared workload {} cannot execute {} Trace",
                    self.workload,
                    trace.workload()
                ),
            ));
        }
        Ok(WorkloadRun::new(
            self.config,
            self.database().backend_kind,
            self.database().shared_backend(),
            trace,
            thread_count,
        )
        .execute())
    }

    pub fn close(self) {}

    #[doc(hidden)]
    pub fn put_for_test(&self, key: &[u8], value: &[u8]) -> Result<(), TemplateError> {
        self.database()
            .backend()
            .put(key, value)
            .map_err(Into::into)
    }

    #[doc(hidden)]
    pub fn delete_for_test(&self, key: &[u8]) -> Result<(), TemplateError> {
        self.database().backend().delete(key).map_err(Into::into)
    }

    #[doc(hidden)]
    pub const fn workload_for_test(&self) -> Workload {
        self.workload
    }

    fn database(&self) -> &ManagedDatabase<'_> {
        self.database
            .as_ref()
            .expect("OpenRun owns its database until close")
    }
}

impl Drop for OpenRun<'_> {
    fn drop(&mut self) {
        let closed_state = match self.lifecycle.load(Ordering::Acquire) {
            RUN_EXECUTED => RUN_CLOSED_EXECUTED,
            _ => RUN_CLOSED_UNEXECUTED,
        };
        // The physical Backend and its open-directory lease must be released
        // before the run becomes eligible for one terminal reopen.
        drop(self.database.take());
        self.lifecycle.store(closed_state, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum TemplateBuildFault {
    None,
    BeforePublish,
}

pub fn build_formal_template(
    workspace: &BenchmarkWorkspace,
    backend_kind: BackendKind,
) -> Result<DatabaseTemplate, TemplateError> {
    build_template(
        workspace,
        backend_kind,
        BenchConfig::formal(),
        false,
        TemplateBuildFault::None,
    )
}

#[doc(hidden)]
pub fn build_test_template(
    workspace: &BenchmarkWorkspace,
    backend_kind: BackendKind,
    config: &BenchConfig,
) -> Result<DatabaseTemplate, TemplateError> {
    build_template(
        workspace,
        backend_kind,
        config.clone(),
        true,
        TemplateBuildFault::None,
    )
}

#[doc(hidden)]
pub fn build_test_template_with_fault(
    workspace: &BenchmarkWorkspace,
    backend_kind: BackendKind,
    config: &BenchConfig,
    fault: TemplateBuildFault,
) -> Result<DatabaseTemplate, TemplateError> {
    build_template(workspace, backend_kind, config.clone(), true, fault)
}

pub fn prepare_run(
    workspace: &BenchmarkWorkspace,
    backend_kind: BackendKind,
    config: &BenchConfig,
    workload: Workload,
    template: Option<&DatabaseTemplate>,
    label: &str,
) -> Result<PreparedRun, TemplateError> {
    let directory = match workload {
        Workload::RandomGet
        | Workload::RangeScan
        | Workload::SingleDelete
        | Workload::BatchDelete => {
            let template = template.ok_or_else(|| {
                TemplateError::new(
                    TemplateErrorKind::MissingTemplate,
                    "read and delete workloads require a full template",
                )
            })?;
            if template.backend_kind() != backend_kind || template.config != *config {
                return Err(TemplateError::new(
                    TemplateErrorKind::TemplateMismatch,
                    "template Backend or configuration does not match the run",
                ));
            }
            template.restore(workspace, label)?
        }
        Workload::SinglePut | Workload::BatchPut => {
            if template.is_some() {
                return Err(TemplateError::new(
                    TemplateErrorKind::TemplateMismatch,
                    "insert workloads must start from a new empty directory",
                ));
            }
            workspace.create_empty_run(backend_kind, label)?
        }
    };
    Ok(PreparedRun {
        directory,
        config: config.clone(),
        workload,
        lifecycle: AtomicU8::new(RUN_READY),
    })
}

fn build_template(
    workspace: &BenchmarkWorkspace,
    backend_kind: BackendKind,
    config: BenchConfig,
    allow_smoke: bool,
    fault: TemplateBuildFault,
) -> Result<DatabaseTemplate, TemplateError> {
    if config.is_formal() == allow_smoke {
        return Err(TemplateError::new(
            TemplateErrorKind::InvalidConfiguration,
            if allow_smoke {
                "test template requires an explicitly smoke configuration"
            } else {
                "formal template requires BenchConfig::formal()"
            },
        ));
    }
    if !config.record_count().is_multiple_of(TEMPLATE_LOAD_BATCH) {
        return Err(TemplateError::new(
            TemplateErrorKind::InvalidConfiguration,
            "template record count must be divisible by the fixed 1000-record load batch",
        ));
    }

    let build = workspace.create_template_build(backend_kind)?;
    let prepared = (|| -> Result<DirectoryManifest, TemplateError> {
        {
            let database = open_database(&build, &config)?;
            load_template_records(database.backend(), &config)?;
        }
        {
            let database = open_existing_database(&build, &config)?;
            validate_full_dataset(database.backend(), &config)?;
        }
        if fault == TemplateBuildFault::BeforePublish {
            return Err(TemplateError::new(
                TemplateErrorKind::Injected,
                "injected template interruption before atomic publish",
            ));
        }
        workspace.capture_manifest(&build).map_err(Into::into)
    })();

    let manifest = match prepared {
        Ok(manifest) => manifest,
        Err(error) => return Err(cleanup_build_error(workspace, &build, error)),
    };
    let directory = match workspace.publish_template(&build) {
        Ok(directory) => directory,
        Err(error) => {
            return Err(cleanup_build_error(
                workspace,
                &build,
                TemplateError::from(error),
            ));
        }
    };
    Ok(DatabaseTemplate {
        workspace: workspace.clone(),
        directory,
        config,
        manifest,
    })
}

fn cleanup_build_error(
    workspace: &BenchmarkWorkspace,
    build: &DatabaseDirectory,
    original: TemplateError,
) -> TemplateError {
    match workspace.cleanup_build(build) {
        Ok(()) => original,
        Err(cleanup) => TemplateError::new(
            TemplateErrorKind::FileSystem,
            format!("{original}; cleanup of incomplete template also failed: {cleanup}"),
        ),
    }
}

fn load_template_records(
    backend: &dyn BenchBackend,
    config: &BenchConfig,
) -> Result<(), TemplateError> {
    let value = fixed_value(config);
    let mut start = 0_u64;
    while start < config.record_count() {
        let end = start.checked_add(TEMPLATE_LOAD_BATCH).ok_or_else(|| {
            TemplateError::new(
                TemplateErrorKind::InvalidConfiguration,
                "template load range overflowed",
            )
        })?;
        let keys = (start..end)
            .map(|id| {
                encode_key(config, id).map_err(|error| {
                    TemplateError::new(
                        TemplateErrorKind::InvalidConfiguration,
                        format!("template key {id} cannot be encoded: {error:?}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let items = keys
            .iter()
            .map(|key| BatchItem::Put { key, value: &value })
            .collect::<Vec<_>>();
        backend.write_batch(&items)?;
        start = end;
    }
    Ok(())
}

struct ManagedDatabase<'a> {
    backend: Option<Arc<dyn BenchBackend>>,
    _lease: OpenLease,
    backend_kind: BackendKind,
    _directory: PhantomData<&'a DatabaseDirectory>,
}

impl ManagedDatabase<'_> {
    fn backend(&self) -> &dyn BenchBackend {
        self.backend
            .as_deref()
            .expect("backend lives until managed close")
    }

    fn shared_backend(&self) -> Arc<dyn BenchBackend> {
        Arc::clone(
            self.backend
                .as_ref()
                .expect("backend lives until managed close"),
        )
    }
}

impl Drop for ManagedDatabase<'_> {
    fn drop(&mut self) {
        // Drop every Backend Arc before OpenLease marks the directory closed.
        self.backend.take();
    }
}

fn open_database<'a>(
    directory: &'a DatabaseDirectory,
    config: &BenchConfig,
) -> Result<ManagedDatabase<'a>, TemplateError> {
    let lease = directory.begin_open()?;
    let backend: Arc<dyn BenchBackend> = match directory.backend_kind() {
        BackendKind::RustKv => Arc::new(RustKvBackend::open(directory.path(), config)?),
        BackendKind::LevelDb => Arc::new(LevelDbBackend::open(directory.path(), config)?),
    };
    Ok(ManagedDatabase {
        backend: Some(backend),
        _lease: lease,
        backend_kind: directory.backend_kind(),
        _directory: PhantomData,
    })
}

fn open_existing_database<'a>(
    directory: &'a DatabaseDirectory,
    config: &BenchConfig,
) -> Result<ManagedDatabase<'a>, TemplateError> {
    directory.require_existing_database()?;
    open_database(directory, config)
}

fn lifecycle_error(operation: &str, expected: u8, actual: u8) -> TemplateError {
    TemplateError::new(
        TemplateErrorKind::InvalidLifecycle,
        format!(
            "run lifecycle rejected {operation}: expected {}, actual {}",
            lifecycle_name(expected),
            lifecycle_name(actual)
        ),
    )
}

const fn lifecycle_name(state: u8) -> &'static str {
    match state {
        RUN_READY => "ready",
        RUN_OPEN => "open",
        RUN_EXECUTED => "executed",
        RUN_CLOSED_UNEXECUTED => "closed-unexecuted",
        RUN_CLOSED_EXECUTED => "closed-executed",
        RUN_VALIDATING => "validating",
        RUN_VALIDATED => "validated",
        RUN_FAILED_TO_OPEN => "failed-to-open",
        RUN_CLEANING => "cleaning",
        RUN_CLEANED => "cleaned",
        _ => "unknown",
    }
}
