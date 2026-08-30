//! Database lifecycle, public API implementation, and component assembly.
#![allow(dead_code)] // Test-only stage harnesses use selected preparation helpers.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(not(test))]
use crate::WriteOutcome;
#[cfg(not(test))]
use crate::commit::{
    CommitCoordinator, DurableVLogEnd, OsTxUuidSource, preflight_batch, preflight_delete,
    preflight_put,
};
use crate::format::{
    FORMAT_ENCODED_LEN, FORMAT_FILE_NAME, FORMAT_TEMP_FILE_NAME, FormatMetadataV0,
};
#[cfg(not(test))]
use crate::index::LateBoundFjallBackend;
#[cfg(test)]
use crate::index::TestCommitFailure;
use crate::index::{
    DATABASE_IDENTITY_KEY, DURABLE_FRONTIER_KEY, DatabaseIdentityV0, FjallBackend, HEAD_SEQ_KEY,
    IndexApplyState, IndexBackend, IndexCommitError, IndexCommitMode, IndexEntry,
    InternalIndexError, InternalIndexSpace, InternalKeyRange, initialization_batch,
    is_encoded_empty_durable_frontier, is_encoded_head_seq_zero,
};
use crate::lock::{
    ManagedEntryKind, RootLock, invalid_argument_error, layout_error, not_found_error,
    open_io_error, sync_directory_tree_nofollow, sync_file_data,
};
#[cfg(not(test))]
use crate::recovery::{analyze_recovery, execute_recovery};
#[cfg(not(test))]
use crate::runtime::{ExternalLease, LifecycleController, OperationGuard, RuntimeControl};
#[cfg(not(test))]
use crate::stats::StatsState;
#[cfg(not(test))]
use crate::vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
#[cfg(not(test))]
use crate::vlog::format::{VLogGeometry, VLogPosition};
#[cfg(not(test))]
use crate::vlog::reader::ValueLogReader;
#[cfg(not(test))]
use crate::vlog::writer::ValueLogRecovery;
use crate::{
    DbIterator, DbStats, InstanceState, KeyRange, Operation, Options, ProtocolStage, RangeCursor,
    ReadOptions, Result, RetryAdvice, Snapshot, StorageError, StorageErrorKind, WriteBatch,
    WriteOptions,
};

const INDEX_DIRECTORY_NAME: &str = "index";
const VLOG_DIRECTORY_NAME: &str = "vlog";
const MAX_KEY_VALUE_SIZE: usize = 60_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VLogInventoryEntry {
    pub(crate) file_id: u32,
    pub(crate) len: u64,
    pub(crate) path: PathBuf,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ManagedInventory {
    pub(crate) vlog_files: Vec<VLogInventoryEntry>,
}

pub(crate) struct OpenPreparation {
    // Fields are declared in dependency drop order. The root lock is last so
    // Fjall and every owned preparation artifact are released before LOCK.
    index: FjallBackend,
    inventory: ManagedInventory,
    format: FormatMetadataV0,
    root_lock: RootLock,
}

impl OpenPreparation {
    pub(crate) fn root_lock(&self) -> &RootLock {
        &self.root_lock
    }

    pub(crate) fn format(&self) -> &FormatMetadataV0 {
        &self.format
    }

    pub(crate) fn inventory(&self) -> &ManagedInventory {
        &self.inventory
    }

    pub(crate) fn index(&self) -> &FjallBackend {
        &self.index
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InitializationFault {
    None,
    #[cfg(test)]
    BeforeCommit,
    #[cfg(test)]
    CommitUnknown,
    #[cfg(test)]
    AfterCommitBeforeFormat,
    #[cfg(test)]
    CrashBeforeCommit,
    #[cfg(test)]
    CrashCommitUnknown,
    #[cfg(test)]
    CrashAfterCommitBeforeFormat,
}

#[cfg(test)]
pub(crate) const INITIALIZATION_CRASH_EXIT_CODE: i32 = 86;

fn read_format(root: &RootLock, temporary: bool) -> Result<Option<FormatMetadataV0>> {
    let name = if temporary {
        FORMAT_TEMP_FILE_NAME
    } else {
        FORMAT_FILE_NAME
    };
    let Some(mut file) = root.open_existing_regular(name)? else {
        return Ok(None);
    };
    let mut encoded = [0_u8; FORMAT_ENCODED_LEN];
    if let Err(error) = file.read_exact(&mut encoded) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Err(metadata_error(StorageErrorKind::Corruption))
        } else {
            Err(open_io_error(error))
        };
    }
    let mut trailing = [0_u8; 1];
    match file.read(&mut trailing) {
        Ok(0) => FormatMetadataV0::decode(&encoded).map(Some),
        Ok(_) => Err(metadata_error(StorageErrorKind::Corruption)),
        Err(error) => Err(open_io_error(error)),
    }
}

fn create_synced_format_temp(root: &RootLock, format: &FormatMetadataV0) -> Result<()> {
    let encoded = format.encode()?;
    let mut file = root.create_new_regular(FORMAT_TEMP_FILE_NAME)?;
    file.write_all(&encoded).map_err(open_io_error)?;
    sync_file_data(&file).map_err(open_io_error)?;
    root.sync_root()
}

fn publish_format_temp(root: &RootLock) -> Result<()> {
    if !matches!(
        root.inspect_child(FORMAT_FILE_NAME)?,
        ManagedEntryKind::Missing
    ) || !matches!(
        root.inspect_child(FORMAT_TEMP_FILE_NAME)?,
        ManagedEntryKind::RegularFile { .. }
    ) {
        return Err(layout_error());
    }
    root.rename_child(FORMAT_TEMP_FILE_NAME, FORMAT_FILE_NAME)?;
    root.sync_root()
}

fn generate_database_uuid() -> Result<[u8; 16]> {
    for _ in 0..4 {
        let mut source = File::open("/dev/urandom").map_err(open_io_error)?;
        let mut database_uuid = [0_u8; 16];
        source
            .read_exact(&mut database_uuid)
            .map_err(open_io_error)?;
        if database_uuid != [0; 16] {
            return Ok(database_uuid);
        }
    }
    Err(StorageError::codec_error(
        StorageErrorKind::Unrecoverable,
        Operation::Open,
        ProtocolStage::Preflight,
        None,
        RetryAdvice::DoNotRetry,
    ))
}

pub(crate) fn prepare_open(options: &Options, path: &Path) -> Result<OpenPreparation> {
    prepare_open_inner(options, path, InitializationFault::None)
}

#[cfg(test)]
pub(crate) fn prepare_open_with_fault(
    options: &Options,
    path: &Path,
    fault: InitializationFault,
) -> Result<OpenPreparation> {
    prepare_open_inner(options, path, fault)
}

fn prepare_open_inner(
    options: &Options,
    path: &Path,
    fault: InitializationFault,
) -> Result<OpenPreparation> {
    let Some(root_lock) = RootLock::acquire(path, options.create_if_missing)? else {
        return Err(not_found_error());
    };

    // The coexistence rule is based only on directory-entry presence. Decode
    // neither file before enforcing it: even a malformed FORMAT or FORMAT.tmp
    // must not change the required InvalidLayout classification.
    let format_present = !matches!(
        root_lock.inspect_child(FORMAT_FILE_NAME)?,
        ManagedEntryKind::Missing
    );
    let temporary_format_present = !matches!(
        root_lock.inspect_child(FORMAT_TEMP_FILE_NAME)?,
        ManagedEntryKind::Missing
    );
    if format_present && temporary_format_present {
        return Err(layout_error());
    }
    let format = read_format(&root_lock, false)?;
    let temporary_format = read_format(&root_lock, true)?;

    match (format, temporary_format) {
        (Some(_), Some(_)) => Err(layout_error()),
        (Some(format), None) => prepare_existing(options, root_lock, format),
        (None, Some(temporary_format)) => {
            validate_interrupted_initialization(options, &root_lock, &temporary_format)?;
            if !options.create_if_missing {
                return Err(not_found_error());
            }
            cleanup_interrupted_initialization(&root_lock)?;
            initialize_database(options, root_lock, fault)
        }
        (None, None) => {
            if !matches!(
                root_lock.inspect_child(INDEX_DIRECTORY_NAME)?,
                ManagedEntryKind::Missing
            ) || !matches!(
                root_lock.inspect_child(VLOG_DIRECTORY_NAME)?,
                ManagedEntryKind::Missing
            ) {
                return Err(layout_error());
            }
            if !options.create_if_missing {
                return Err(not_found_error());
            }
            initialize_database(options, root_lock, fault)
        }
    }
}

fn prepare_existing(
    options: &Options,
    root_lock: RootLock,
    format: FormatMetadataV0,
) -> Result<OpenPreparation> {
    let inventory = ManagedInventory::inspect(&root_lock, &format)?;
    let index_path = root_lock.canonical_path().join(INDEX_DIRECTORY_NAME);
    let index = FjallBackend::open_existing_for_open_preparation(
        &index_path,
        options.fjall_index_options(),
    )?;
    validate_final_identity(&index, &format)?;
    if options.error_if_exists {
        return Err(invalid_argument_error());
    }
    Ok(OpenPreparation {
        root_lock,
        format,
        inventory,
        index,
    })
}

fn initialize_database(
    options: &Options,
    root_lock: RootLock,
    _fault: InitializationFault,
) -> Result<OpenPreparation> {
    let format = FormatMetadataV0::new(generate_database_uuid()?)?;
    create_synced_format_temp(&root_lock, &format)?;
    root_lock.create_directory(INDEX_DIRECTORY_NAME)?;
    root_lock.create_directory(VLOG_DIRECTORY_NAME)?;
    root_lock.sync_root()?;

    let index_path = root_lock.canonical_path().join(INDEX_DIRECTORY_NAME);
    // 创建Fjall index索引目录
    let index =
        FjallBackend::create_for_open_preparation(&index_path, options.fjall_index_options())?;

    #[cfg(test)]
    match _fault {
        InitializationFault::BeforeCommit => return Err(initialization_io_error()),
        InitializationFault::CrashBeforeCommit => {
            std::process::exit(INITIALIZATION_CRASH_EXIT_CODE)
        }
        _ => {}
    }
    #[cfg(test)]
    if matches!(
        _fault,
        InitializationFault::CommitUnknown | InitializationFault::CrashCommitUnknown
    ) {
        index.set_commit_failure(TestCommitFailure::AfterCommitReturned);
    }

    let batch = initialization_batch(format.format_version, format.database_uuid)
        .map_err(index_preflight_error)?;
    let commit_result = index
        .commit_atomic(batch, IndexCommitMode::SyncAll)
        .map_err(initialization_commit_error);

    #[cfg(test)]
    if _fault == InitializationFault::CrashCommitUnknown {
        debug_assert!(commit_result.is_err());
        std::process::exit(INITIALIZATION_CRASH_EXIT_CODE);
    }
    commit_result?;

    #[cfg(test)]
    match _fault {
        InitializationFault::AfterCommitBeforeFormat => return Err(initialization_io_error()),
        InitializationFault::CrashAfterCommitBeforeFormat => {
            std::process::exit(INITIALIZATION_CRASH_EXIT_CODE)
        }
        _ => {}
    }

    sync_directory_tree_nofollow(&index_path)?;
    root_lock.sync_directory(VLOG_DIRECTORY_NAME)?;
    root_lock.sync_root()?;
    if read_format(&root_lock, true)?.as_ref() != Some(&format) {
        return Err(metadata_error(StorageErrorKind::Corruption));
    }
    publish_format_temp(&root_lock)?;

    let inventory = ManagedInventory::inspect(&root_lock, &format)?;
    validate_final_identity(&index, &format)?;
    Ok(OpenPreparation {
        root_lock,
        format,
        inventory,
        index,
    })
}

fn validate_final_identity(index: &FjallBackend, format: &FormatMetadataV0) -> Result<()> {
    let encoded = index
        .get_database_identity()
        .map_err(open_recovery_error)?
        .ok_or_else(|| metadata_error(StorageErrorKind::Corruption))?;
    DatabaseIdentityV0::decode(&encoded)?
        .validate_against(format.format_version, format.database_uuid)?;
    if index
        .get_internal(InternalIndexSpace::System, HEAD_SEQ_KEY)
        .map_err(open_recovery_error)?
        .is_none()
        || index
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)
            .map_err(open_recovery_error)?
            .is_none()
    {
        return Err(metadata_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

fn validate_interrupted_initialization(
    options: &Options,
    root_lock: &RootLock,
    temporary_format: &FormatMetadataV0,
) -> Result<()> {
    match root_lock.inspect_child(VLOG_DIRECTORY_NAME)? {
        ManagedEntryKind::Missing => {}
        ManagedEntryKind::Directory => {
            let mut entries = root_lock.read_directory(VLOG_DIRECTORY_NAME)?;
            if entries
                .next()
                .transpose()
                .map_err(crate::lock::open_io_error)?
                .is_some()
            {
                return Err(layout_error());
            }
        }
        _ => return Err(layout_error()),
    }

    match root_lock.inspect_child(INDEX_DIRECTORY_NAME)? {
        ManagedEntryKind::Missing => Ok(()),
        ManagedEntryKind::Directory => {
            let index_path = root_lock.canonical_path().join(INDEX_DIRECTORY_NAME);
            let mut entries = root_lock.read_directory(INDEX_DIRECTORY_NAME)?;
            if entries
                .next()
                .transpose()
                .map_err(crate::lock::open_io_error)?
                .is_none()
            {
                return Ok(());
            }
            let index = FjallBackend::open_existing_for_open_preparation(
                &index_path,
                options.fjall_index_options(),
            )?;
            validate_interrupted_index(&index, temporary_format)
        }
        _ => Err(layout_error()),
    }
}

fn validate_interrupted_index(
    index: &FjallBackend,
    temporary_format: &FormatMetadataV0,
) -> Result<()> {
    let identity = index.get_database_identity().map_err(open_recovery_error)?;
    match identity {
        None => {
            if backend_is_completely_empty(index)? {
                Ok(())
            } else {
                Err(layout_error())
            }
        }
        Some(encoded_identity) => {
            DatabaseIdentityV0::decode(&encoded_identity)?.validate_against(
                temporary_format.format_version,
                temporary_format.database_uuid,
            )?;
            if index
                .iter_user(None)
                .map_err(open_recovery_error)?
                .next()
                .transpose()
                .map_err(open_recovery_error)?
                .is_some()
                || index
                    .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())
                    .map_err(open_recovery_error)?
                    .next()
                    .transpose()
                    .map_err(open_recovery_error)?
                    .is_some()
            {
                return Err(layout_error());
            }

            let mut found_identity = false;
            let mut found_head = false;
            let mut found_frontier = false;
            let entries = index
                .scan_internal(InternalIndexSpace::System, InternalKeyRange::all())
                .map_err(open_recovery_error)?;
            for entry in entries {
                let entry = entry.map_err(open_recovery_error)?;
                match entry.key.as_slice() {
                    DATABASE_IDENTITY_KEY => {
                        if found_identity || entry.value != encoded_identity {
                            return Err(metadata_error(StorageErrorKind::Corruption));
                        }
                        found_identity = true;
                    }
                    HEAD_SEQ_KEY => {
                        if found_head || !is_encoded_head_seq_zero(&entry.value) {
                            return Err(metadata_error(StorageErrorKind::Corruption));
                        }
                        found_head = true;
                    }
                    DURABLE_FRONTIER_KEY => {
                        if found_frontier || !is_encoded_empty_durable_frontier(&entry.value) {
                            return Err(metadata_error(StorageErrorKind::Corruption));
                        }
                        found_frontier = true;
                    }
                    _ => return Err(layout_error()),
                }
            }
            if found_identity && found_head && found_frontier {
                Ok(())
            } else {
                Err(metadata_error(StorageErrorKind::Corruption))
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn validate_interrupted_index_for_test(
    index: &FjallBackend,
    temporary_format: &FormatMetadataV0,
) -> Result<()> {
    validate_interrupted_index(index, temporary_format)
}

fn backend_is_completely_empty(index: &FjallBackend) -> Result<bool> {
    if index
        .iter_user(None)
        .map_err(open_recovery_error)?
        .next()
        .transpose()
        .map_err(open_recovery_error)?
        .is_some()
    {
        return Ok(false);
    }
    for space in [InternalIndexSpace::Transaction, InternalIndexSpace::System] {
        if index
            .scan_internal(space, InternalKeyRange::all())
            .map_err(open_recovery_error)?
            .next()
            .transpose()
            .map_err(open_recovery_error)?
            .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cleanup_interrupted_initialization(root_lock: &RootLock) -> Result<()> {
    root_lock.remove_directory_tree_if_present(INDEX_DIRECTORY_NAME)?;
    root_lock.remove_directory_tree_if_present(VLOG_DIRECTORY_NAME)?;
    root_lock.remove_regular_child_if_present(FORMAT_TEMP_FILE_NAME)?;
    root_lock.sync_root()
}

fn index_preflight_error(error: InternalIndexError) -> StorageError {
    let mut storage_error = metadata_error(error.kind);
    storage_error.protocol_stage = ProtocolStage::Preflight;
    storage_error.os_code = error.os_code;
    storage_error
}

fn initialization_commit_error(error: IndexCommitError) -> StorageError {
    let retry_advice = if error.apply_state == IndexApplyState::Unknown {
        RetryAdvice::ReopenAndVerify
    } else {
        retry_advice_for(error.source.kind)
    };
    let mut storage_error = StorageError::codec_error(
        error.source.kind,
        Operation::Open,
        ProtocolStage::IndexCommit,
        None,
        retry_advice,
    );
    storage_error.os_code = error.source.os_code;
    storage_error
}

fn initialization_io_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Open,
        ProtocolStage::IndexCommit,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    )
}

fn metadata_error(kind: StorageErrorKind) -> StorageError {
    StorageError::codec_error(
        kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        retry_advice_for(kind),
    )
}

fn open_recovery_error(mut error: StorageError) -> StorageError {
    error.operation = Operation::Open;
    error.protocol_stage = ProtocolStage::Recovery;
    error.write_outcome = None;
    error.instance_state = None;
    error
}

fn retry_advice_for(kind: StorageErrorKind) -> RetryAdvice {
    match kind {
        StorageErrorKind::InvalidArgument => RetryAdvice::FixRequestAndRetrySameInstance,
        StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted => {
            RetryAdvice::RetrySameInstance
        }
        StorageErrorKind::Io => RetryAdvice::FixEnvironmentAndReopen,
        StorageErrorKind::StoragePoisoned => RetryAdvice::ReopenAndVerify,
        StorageErrorKind::IncompatibleFormat => RetryAdvice::DoNotRetry,
        StorageErrorKind::Corruption
        | StorageErrorKind::InvalidLayout
        | StorageErrorKind::Unrecoverable => RetryAdvice::RestoreOrRepair,
        _ => RetryAdvice::DoNotRetry,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadStateSnapshot {
    pub(crate) instance_state: InstanceState,
    pub(crate) state_epoch: u64,
}

pub(crate) trait ReadRuntime: Send + Sync {
    fn state_snapshot(&self) -> ReadStateSnapshot;

    fn latch_read_failure(&self, target: InstanceState, error: &StorageError) -> ReadStateSnapshot;

    fn read_stats(&self) -> DbStats;
}

pub(crate) trait UserIndexReader: Send + Sync {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn snapshot_view(self: Arc<Self>) -> Result<Arc<dyn UserIndexSnapshot>> {
        let _ = self;
        Err(StorageError::unsupported(
            Operation::Snapshot,
            ProtocolStage::Read,
            None,
        ))
    }
}

pub(crate) type UserIndexIterator = Box<dyn DoubleEndedIterator<Item = Result<IndexEntry>> + Send>;

pub(crate) trait UserIndexSnapshot: Send + Sync {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn iter_user(&self) -> Result<UserIndexIterator>;
}

struct BackendIndexSnapshot<T: IndexBackend> {
    backend: Arc<T>,
    snapshot: T::Snapshot,
}

impl<T: IndexBackend + 'static> UserIndexSnapshot for BackendIndexSnapshot<T> {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.backend.get_user(key, Some(&self.snapshot))
    }

    fn iter_user(&self) -> Result<UserIndexIterator> {
        self.backend
            .iter_user(Some(&self.snapshot))
            .map(|iterator| Box::new(iterator) as UserIndexIterator)
    }
}

impl<T: IndexBackend + 'static> UserIndexReader for T {
    fn get_user_pointer(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_user(key, None)
    }

    fn snapshot_view(self: Arc<Self>) -> Result<Arc<dyn UserIndexSnapshot>> {
        let snapshot = self.snapshot()?;
        Ok(Arc::new(BackendIndexSnapshot {
            backend: self,
            snapshot,
        }))
    }
}

pub(crate) trait ValueReader: Send + Sync {
    fn read_value(&self, encoded_pointer: &[u8], expected_key: &[u8]) -> Result<Vec<u8>>;
}

struct ReadPath {
    runtime: Arc<dyn ReadRuntime>,
    index: Arc<dyn UserIndexReader>,
    values: Arc<dyn ValueReader>,
}

#[cfg(not(test))]
type LiveCommitCoordinator = CommitCoordinator<LateBoundFjallBackend, OsTxUuidSource>;

#[cfg(not(test))]
struct LiveComponents {
    // The lifecycle controller closes admission before protocol resources are
    // released. RootLock remains last and therefore outlives every component.
    lifecycle: Arc<LifecycleController>,
    runtime: Arc<RuntimeControl>,
    coordinator: Arc<LiveCommitCoordinator>,
    _options: Arc<Options>,
    _root_lock: RootLock,
}

pub(crate) struct DbInner {
    // Read-only Arcs are released before the live protocol components. The
    // final RootLock is owned by LiveComponents and is always released last.
    read_path: ReadPath,
    #[cfg(not(test))]
    live: LiveComponents,
}

impl DbInner {
    pub(crate) fn instance_state(&self) -> InstanceState {
        self.read_path.runtime.state_snapshot().instance_state
    }

    pub(crate) fn unsupported_error(
        &self,
        operation: Operation,
        protocol_stage: ProtocolStage,
    ) -> StorageError {
        StorageError::unsupported(operation, protocol_stage, Some(self.instance_state()))
    }

    fn get(self: &Arc<Self>, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let started = self.begin_read(Operation::Get)?;
        if key.is_empty() || key.len() > MAX_KEY_VALUE_SIZE {
            return Err(StorageError::read_error(
                StorageErrorKind::InvalidArgument,
                started.instance_state,
                RetryAdvice::FixRequestAndRetrySameInstance,
            ));
        }

        let index_view = match options.snapshot {
            Some(snapshot) => {
                if !snapshot.belongs_to(self) {
                    return Err(invalid_read_argument(
                        Operation::Get,
                        started.instance_state,
                    ));
                }
                Some(snapshot.view())
            }
            None => None,
        };

        let pointer_result = match index_view.as_ref() {
            Some(view) => view.get_user_pointer(key),
            None => self.read_path.index.get_user_pointer(key),
        };
        let encoded_pointer = match pointer_result {
            Ok(pointer) => pointer,
            Err(error) => {
                return Err(self.handle_read_failure(
                    error,
                    ReadFailureDomain::Index,
                    Operation::Get,
                ));
            }
        };
        let Some(encoded_pointer) = encoded_pointer else {
            self.complete_read(started, Operation::Get)?;
            return Ok(None);
        };

        let value = self
            .read_path
            .values
            .read_value(&encoded_pointer, key)
            .map_err(|error| {
                self.handle_read_failure(error, ReadFailureDomain::ValueLog, Operation::Get)
            })?;
        self.complete_read(started, Operation::Get)?;
        Ok(Some(value))
    }

    pub(crate) fn begin_read(&self, operation: Operation) -> Result<ReadStateSnapshot> {
        let state = self.read_path.runtime.state_snapshot();
        if state.instance_state == InstanceState::Poisoned {
            return Err(poisoned_read_error(operation));
        }
        Ok(state)
    }

    pub(crate) fn complete_read(
        &self,
        started: ReadStateSnapshot,
        operation: Operation,
    ) -> Result<()> {
        let current = self.read_path.runtime.state_snapshot();
        if current.instance_state == InstanceState::Poisoned
            && current.state_epoch != started.state_epoch
        {
            return Err(poisoned_read_error(operation));
        }
        Ok(())
    }

    pub(crate) fn select_read_view(
        self: &Arc<Self>,
        options: &ReadOptions<'_>,
        operation: Operation,
        started: ReadStateSnapshot,
    ) -> Result<Arc<dyn UserIndexSnapshot>> {
        match options.snapshot {
            Some(snapshot) if snapshot.belongs_to(self) => Ok(snapshot.view()),
            Some(_) => Err(invalid_read_argument(operation, started.instance_state)),
            None => Arc::clone(&self.read_path.index)
                .snapshot_view()
                .map_err(|error| {
                    self.handle_read_failure(error, ReadFailureDomain::Index, operation)
                }),
        }
    }

    pub(crate) fn map_index_read_failure(
        &self,
        error: StorageError,
        operation: Operation,
    ) -> StorageError {
        self.handle_read_failure(error, ReadFailureDomain::Index, operation)
    }

    pub(crate) fn materialize_index_entry(
        &self,
        started: ReadStateSnapshot,
        operation: Operation,
        entry: IndexEntry,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let value = self
            .read_path
            .values
            .read_value(&entry.value, &entry.key)
            .map_err(|error| {
                self.handle_read_failure(error, ReadFailureDomain::ValueLog, operation)
            })?;
        self.complete_read(started, operation)?;
        Ok((entry.key, value))
    }

    fn handle_read_failure(
        &self,
        mut error: StorageError,
        domain: ReadFailureDomain,
        operation: Operation,
    ) -> StorageError {
        error.operation = operation;
        error.protocol_stage = ProtocolStage::Read;
        error.write_outcome = None;

        let target = failure_target(domain, error.kind);
        let expected_state =
            target.unwrap_or_else(|| self.read_path.runtime.state_snapshot().instance_state);
        error.retry_advice = read_retry_advice(domain, error.kind, expected_state);
        let state = match target {
            Some(target) => self.read_path.runtime.latch_read_failure(target, &error),
            None => self.read_path.runtime.state_snapshot(),
        };
        error.instance_state = Some(state.instance_state);
        error.retry_advice = read_retry_advice(domain, error.kind, state.instance_state);
        error
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadFailureDomain {
    Index,
    ValueLog,
}

fn failure_target(domain: ReadFailureDomain, kind: StorageErrorKind) -> Option<InstanceState> {
    match (domain, kind) {
        (_, StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted) => None,
        (_, StorageErrorKind::Io | StorageErrorKind::StorageWriteStopped)
            if domain == ReadFailureDomain::ValueLog =>
        {
            Some(InstanceState::WriteStopped)
        }
        (ReadFailureDomain::Index, StorageErrorKind::StorageWriteStopped) => {
            Some(InstanceState::WriteStopped)
        }
        (ReadFailureDomain::Index, _) | (ReadFailureDomain::ValueLog, _) => {
            Some(InstanceState::Poisoned)
        }
    }
}

fn read_retry_advice(
    domain: ReadFailureDomain,
    kind: StorageErrorKind,
    final_state: InstanceState,
) -> RetryAdvice {
    if final_state == InstanceState::Poisoned {
        return match kind {
            StorageErrorKind::Corruption
            | StorageErrorKind::InvalidLayout
            | StorageErrorKind::Unrecoverable => RetryAdvice::RestoreOrRepair,
            StorageErrorKind::IncompatibleFormat
            | StorageErrorKind::InvalidArgument
            | StorageErrorKind::NotFound
            | StorageErrorKind::Unsupported
            | StorageErrorKind::CapacityExceeded => RetryAdvice::DoNotRetry,
            StorageErrorKind::Busy
            | StorageErrorKind::ResourceExhausted
            | StorageErrorKind::Io
            | StorageErrorKind::StorageWriteStopped
            | StorageErrorKind::StoragePoisoned => RetryAdvice::ReopenAndVerify,
        };
    }

    match (domain, kind) {
        (_, StorageErrorKind::InvalidArgument) => RetryAdvice::FixRequestAndRetrySameInstance,
        (_, StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted) => {
            RetryAdvice::RetrySameInstance
        }
        (_, StorageErrorKind::Io | StorageErrorKind::StorageWriteStopped) => {
            RetryAdvice::FixEnvironmentAndReopen
        }
        (_, StorageErrorKind::StoragePoisoned) => RetryAdvice::ReopenAndVerify,
        (
            _,
            StorageErrorKind::Corruption
            | StorageErrorKind::InvalidLayout
            | StorageErrorKind::Unrecoverable,
        ) => RetryAdvice::RestoreOrRepair,
        (_, StorageErrorKind::IncompatibleFormat)
        | (
            ReadFailureDomain::Index,
            StorageErrorKind::NotFound
            | StorageErrorKind::Unsupported
            | StorageErrorKind::CapacityExceeded,
        )
        | (
            ReadFailureDomain::ValueLog,
            StorageErrorKind::NotFound
            | StorageErrorKind::Unsupported
            | StorageErrorKind::CapacityExceeded,
        ) => RetryAdvice::DoNotRetry,
    }
}

fn poisoned_read_error(operation: Operation) -> StorageError {
    StorageError::read_operation_error(
        StorageErrorKind::StoragePoisoned,
        operation,
        InstanceState::Poisoned,
        RetryAdvice::ReopenAndVerify,
    )
}

fn invalid_read_argument(operation: Operation, state: InstanceState) -> StorageError {
    StorageError::read_operation_error(
        StorageErrorKind::InvalidArgument,
        operation,
        state,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

pub struct Db {
    // Field order is intentional: the external lease closes admission before
    // the final Arc can release DbInner and its root lock.
    #[cfg(not(test))]
    lease: ExternalLease,
    inner: Arc<DbInner>,
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Self {
            #[cfg(not(test))]
            lease: self.lease.clone(),
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(not(test))]
pub(crate) struct PublicOperationGuard {
    // Drop the lifecycle guard before releasing the Arc that keeps all storage
    // resources and the root lock alive for the operation.
    _operation: OperationGuard,
    _inner: Arc<DbInner>,
}

#[cfg(not(test))]
impl DbInner {
    pub(crate) fn begin_operation(
        self: &Arc<Self>,
        operation: Operation,
    ) -> Result<PublicOperationGuard> {
        let operation_guard = self
            .live
            .lifecycle
            .acquire_operation()
            .ok_or_else(|| lifecycle_admission_error(operation, self.instance_state()))?;
        Ok(PublicOperationGuard {
            _operation: operation_guard,
            _inner: Arc::clone(self),
        })
    }
}

impl Db {
    #[cfg(test)]
    pub(crate) fn from_read_components<R, I, V>(
        runtime: Arc<R>,
        index: Arc<I>,
        values: Arc<V>,
    ) -> Self
    where
        R: ReadRuntime + 'static,
        I: UserIndexReader + 'static,
        V: ValueReader + 'static,
    {
        Self {
            inner: Arc::new(DbInner {
                read_path: ReadPath {
                    runtime,
                    index,
                    values,
                },
            }),
        }
    }

    #[cfg(not(test))]
    pub fn open(options: &Options, path: impl AsRef<Path>) -> Result<Self> {
        let preparation = prepare_open(options, path.as_ref())?;
        let OpenPreparation {
            index: recovery_index,
            inventory,
            format,
            root_lock,
        } = preparation;

        let vlog_path = root_lock.canonical_path().join(VLOG_DIRECTORY_NAME);
        let directory = Arc::new(VLogDirectory::open(&vlog_path).map_err(open_recovery_error)?);
        let catalog = Arc::new(FileCatalog::new());
        register_inventory(&directory, &catalog, &inventory)?;
        let files = Arc::new(
            FileSet::new(
                directory,
                format.database_uuid,
                VLogGeometry::PRODUCTION,
                catalog,
                options.vlog_read_handle_cache_capacity,
            )
            .map_err(open_recovery_error)?,
        );
        let reader = Arc::new(
            ValueLogReader::new(Arc::clone(&files), VLogGeometry::PRODUCTION)
                .map_err(open_recovery_error)?,
        );
        let plan = analyze_recovery(&recovery_index, &format, &inventory, &reader)?;
        let recovery = ValueLogRecovery::new(Arc::clone(&files)).map_err(open_recovery_error)?;
        let recovered = execute_recovery(
            &recovery_index,
            plan,
            &root_lock,
            &format,
            &reader,
            recovery,
        )?;

        let index_path = root_lock.canonical_path().join(INDEX_DIRECTORY_NAME);
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let head_vlog_end = durable_end_position(recovered.durable_frontier.durable_vlog_end);
        let index_binding = Arc::new(LateBoundFjallBackend::new());
        let coordinator = Arc::new(CommitCoordinator::new(
            Arc::clone(&runtime),
            stats,
            Arc::clone(&index_binding),
            recovered.writer,
            OsTxUuidSource,
            recovered.head_seq,
            recovered.durable_frontier,
            head_vlog_end,
        )?);
        let (lifecycle, lease) = LifecycleController::new_with_external_lease();
        let read_path = ReadPath {
            runtime: Arc::clone(&runtime) as Arc<dyn ReadRuntime>,
            index: Arc::clone(&index_binding) as Arc<dyn UserIndexReader>,
            values: reader as Arc<dyn ValueReader>,
        };
        let inner = Arc::new(DbInner {
            read_path,
            live: LiveComponents {
                lifecycle,
                runtime,
                coordinator,
                _options: Arc::new(copy_options(options)),
                _root_lock: root_lock,
            },
        });

        // Recovery and the complete Healthy runtime are now assembled against
        // the one-shot index binding. Only at this point may Fjall start its
        // flush/compaction workers. The binding is published before `Db`
        // escapes, so public requests cannot observe the unbound state.
        drop(recovery_index);
        let index = Arc::new(FjallBackend::open_existing(
            &index_path,
            options.fjall_index_options(),
        )?);
        validate_final_identity(&index, &format)?;
        index_binding
            .bind(index)
            .map_err(|_| late_bound_index_error())?;

        Ok(Self { lease, inner })
    }

    #[cfg(test)]
    pub fn open(options: &Options, path: impl AsRef<Path>) -> Result<Self> {
        let _ = (options, path.as_ref());
        Err(StorageError::unsupported(
            Operation::Open,
            ProtocolStage::Lifecycle,
            None,
        ))
    }

    #[cfg(not(test))]
    pub fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        let _operation = self.begin_operation(Operation::Put)?;
        self.inner
            .live
            .runtime
            .check_write_admission(Operation::Put)?;
        let write = preflight_put(key, value, options.sync)?;
        self.inner.live.coordinator.commit_nonempty(&write)
    }

    #[cfg(test)]
    pub fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        let _ = (options, key, value);
        Err(self
            .inner
            .unsupported_error(Operation::Put, ProtocolStage::Preflight))
    }

    pub fn get(&self, options: &ReadOptions<'_>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(not(test))]
        let _operation = self.begin_operation(Operation::Get)?;
        self.inner.get(options, key)
    }

    #[cfg(not(test))]
    pub fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()> {
        let _operation = self.begin_operation(Operation::Delete)?;
        self.inner
            .live
            .runtime
            .check_write_admission(Operation::Delete)?;
        let write = preflight_delete(key, options.sync)?;
        self.inner.live.coordinator.commit_nonempty(&write)
    }

    #[cfg(test)]
    pub fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()> {
        let _ = (options, key);
        Err(self
            .inner
            .unsupported_error(Operation::Delete, ProtocolStage::Preflight))
    }

    #[cfg(not(test))]
    pub fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<()> {
        let _operation = self.begin_operation(Operation::WriteBatch)?;
        if batch.is_empty() {
            return self.inner.live.coordinator.commit_empty_batch(options.sync);
        }
        self.inner
            .live
            .runtime
            .check_write_admission(Operation::WriteBatch)?;
        let write = preflight_batch(batch, options.sync)?;
        self.inner.live.coordinator.commit_nonempty(&write)
    }

    #[cfg(test)]
    pub fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<()> {
        let _ = (options, batch);
        Err(self
            .inner
            .unsupported_error(Operation::WriteBatch, ProtocolStage::Preflight))
    }

    pub fn snapshot(&self) -> Result<Snapshot> {
        #[cfg(not(test))]
        let _operation = self.begin_operation(Operation::Snapshot)?;
        let started = self.inner.begin_read(Operation::Snapshot)?;
        let view =
            self.inner
                .select_read_view(&ReadOptions::default(), Operation::Snapshot, started)?;
        self.inner.complete_read(started, Operation::Snapshot)?;
        #[cfg(not(test))]
        let snapshot = Snapshot::new(Arc::clone(&self.inner), view, self.lease.clone());
        #[cfg(test)]
        let snapshot = Snapshot::new_for_test(Arc::clone(&self.inner), view);
        Ok(snapshot)
    }

    pub fn iter(&self, options: &ReadOptions<'_>) -> Result<DbIterator> {
        #[cfg(not(test))]
        let _operation = self.begin_operation(Operation::Iterator)?;
        let started = self.inner.begin_read(Operation::Iterator)?;
        let view = self
            .inner
            .select_read_view(options, Operation::Iterator, started)?;
        let iterator = view.iter_user().map_err(|error| {
            self.inner
                .map_index_read_failure(error, Operation::Iterator)
        })?;
        self.inner.complete_read(started, Operation::Iterator)?;
        #[cfg(not(test))]
        let iterator = DbIterator::new(
            Arc::clone(&self.inner),
            view,
            iterator,
            Operation::Iterator,
            self.lease.clone(),
        );
        #[cfg(test)]
        let iterator =
            DbIterator::new_for_test(Arc::clone(&self.inner), view, iterator, Operation::Iterator);
        Ok(iterator)
    }

    pub fn range(
        &self,
        options: &ReadOptions<'_>,
        range: KeyRange<'_>,
        limit: usize,
    ) -> Result<RangeCursor> {
        #[cfg(not(test))]
        let _operation = self.begin_operation(Operation::Range)?;
        let started = self.inner.begin_read(Operation::Range)?;
        validate_range_bound(range.start, started.instance_state)?;
        validate_range_bound(range.end, started.instance_state)?;
        if let Some(snapshot) = options.snapshot
            && !snapshot.belongs_to(&self.inner)
        {
            return Err(invalid_read_argument(
                Operation::Range,
                started.instance_state,
            ));
        }

        let is_empty = limit == 0
            || matches!((range.start, range.end), (None, Some(end)) if end.is_empty())
            || matches!((range.start, range.end), (Some(start), Some(end)) if start >= end);
        if is_empty {
            self.inner.complete_read(started, Operation::Range)?;
            #[cfg(not(test))]
            let inner = DbIterator::empty(
                Arc::clone(&self.inner),
                Operation::Range,
                self.lease.clone(),
            );
            #[cfg(test)]
            let inner = DbIterator::empty_for_test(Arc::clone(&self.inner), Operation::Range);
            #[cfg(not(test))]
            let cursor = RangeCursor::new(inner, None, 0);
            #[cfg(test)]
            let cursor = RangeCursor::new_for_test(inner, None, 0);
            return Ok(cursor);
        }

        let end = range
            .end
            .map(|bound| clone_range_bound(bound, started.instance_state))
            .transpose()?;
        let view = self
            .inner
            .select_read_view(options, Operation::Range, started)?;
        let iterator = view
            .iter_user()
            .map_err(|error| self.inner.map_index_read_failure(error, Operation::Range))?;
        self.inner.complete_read(started, Operation::Range)?;
        #[cfg(not(test))]
        let mut inner = DbIterator::new(
            Arc::clone(&self.inner),
            Arc::clone(&view),
            iterator,
            Operation::Range,
            self.lease.clone(),
        );
        #[cfg(test)]
        let mut inner =
            DbIterator::new_for_test(Arc::clone(&self.inner), view, iterator, Operation::Range);
        match range.start {
            Some(start) => inner.seek_before(start, end.as_deref()),
            None => inner.seek_to_first_before(end.as_deref()),
        }
        #[cfg(not(test))]
        let cursor = RangeCursor::new(inner, end, limit);
        #[cfg(test)]
        let cursor = RangeCursor::new_for_test(inner, end, limit);
        Ok(cursor)
    }

    pub fn stats(&self) -> DbStats {
        self.inner.read_path.runtime.read_stats()
    }

    pub fn destroy(path: impl AsRef<Path>, options: &Options) -> Result<()> {
        let _ = (path.as_ref(), options);
        Err(StorageError::unsupported(
            Operation::Destroy,
            ProtocolStage::Lifecycle,
            None,
        ))
    }

    #[cfg(not(test))]
    fn begin_operation(&self, operation: Operation) -> Result<PublicOperationGuard> {
        self.inner.begin_operation(operation)
    }
}

#[cfg(not(test))]
fn register_inventory(
    directory: &Arc<VLogDirectory>,
    catalog: &Arc<FileCatalog>,
    inventory: &ManagedInventory,
) -> Result<()> {
    for entry in &inventory.vlog_files {
        let file = directory
            .open_read_only(entry.file_id)
            .map_err(|error| inventory_file_error(error, entry.file_id))?;
        let len = file
            .metadata()
            .map_err(|error| inventory_file_error(error, entry.file_id))?
            .len();
        if len != entry.len {
            return Err(inventory_corruption(entry.file_id));
        }
        catalog
            .register(entry.file_id, &file)
            .map_err(open_recovery_error)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn inventory_file_error(error: io::Error, file_id: u32) -> StorageError {
    let mut error = open_recovery_error(open_io_error(error));
    error.vlog_file_id = Some(file_id);
    error
}

#[cfg(not(test))]
fn inventory_corruption(file_id: u32) -> StorageError {
    let mut error = metadata_error(StorageErrorKind::Corruption);
    error.vlog_file_id = Some(file_id);
    error
}

#[cfg(not(test))]
fn durable_end_position(end: DurableVLogEnd) -> Option<VLogPosition> {
    match end {
        DurableVLogEnd::Empty => None,
        DurableVLogEnd::Position(position) => Some(VLogPosition {
            file_id: position.file_id,
            offset: position.offset,
        }),
    }
}

#[cfg(not(test))]
fn copy_options(options: &Options) -> Options {
    Options {
        create_if_missing: options.create_if_missing,
        error_if_exists: options.error_if_exists,
        write_buffer_size: options.write_buffer_size,
        max_open_files: options.max_open_files,
        block_cache_size: options.block_cache_size,
        block_size: options.block_size,
        block_restart_interval: options.block_restart_interval,
        max_file_size: options.max_file_size,
        compression: options.compression,
        vlog_read_handle_cache_capacity: options.vlog_read_handle_cache_capacity,
    }
}

fn validate_range_bound(bound: Option<&[u8]>, state: InstanceState) -> Result<()> {
    if bound.is_some_and(|bound| !bound.is_empty() && bound.len() > MAX_KEY_VALUE_SIZE) {
        return Err(invalid_read_argument(Operation::Range, state));
    }
    Ok(())
}

fn clone_range_bound(bound: &[u8], state: InstanceState) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(bound.len()).map_err(|_| {
        StorageError::read_operation_error(
            StorageErrorKind::ResourceExhausted,
            Operation::Range,
            state,
            RetryAdvice::RetrySameInstance,
        )
    })?;
    owned.extend_from_slice(bound);
    Ok(owned)
}

#[cfg(not(test))]
fn lifecycle_admission_error(operation: Operation, state: InstanceState) -> StorageError {
    let write_outcome = matches!(
        operation,
        Operation::Put | Operation::Delete | Operation::WriteBatch | Operation::Sync
    )
    .then_some(WriteOutcome::NotCommitted);
    let mut error = StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        operation,
        ProtocolStage::Admission,
        write_outcome,
        RetryAdvice::ReopenAndVerify,
    );
    error.instance_state = Some(state);
    error
}

#[cfg(not(test))]
fn late_bound_index_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        Operation::Open,
        ProtocolStage::Lifecycle,
        None,
        RetryAdvice::ReopenAndVerify,
    )
}
