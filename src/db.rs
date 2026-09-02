//! Database lifecycle, public API implementation, and component assembly.
#![allow(dead_code)] // Test-only stage harnesses use selected preparation helpers.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(test))]
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
#[cfg(not(test))]
use std::thread::{self, JoinHandle};

#[cfg(not(test))]
use crate::WriteOutcome;
#[cfg(not(test))]
use crate::commit::{
    CommitCoordinator, DurableVLogEnd, OsTxUuidSource, preflight_batch, preflight_delete,
    preflight_put,
};
use crate::commit::{
    DurableFrontier, RECOVERY_STATE_KEY, decode_tx_meta_key, decode_tx_mutation_key,
    encode_tx_meta_key,
};
use crate::error::{DestroyFailureContext, DestroyStage, ManagedObject};
use crate::format::{
    FORMAT_ENCODED_LEN, FORMAT_FILE_NAME, FORMAT_TEMP_FILE_NAME, FormatMetadataV0,
};
#[cfg(not(test))]
use crate::index::LateBoundFjallBackend;
#[cfg(test)]
use crate::index::TestCommitFailure;
use crate::index::{
    DATABASE_IDENTITY_KEY, DURABLE_FRONTIER_KEY, DatabaseIdentityV0, FjallBackend, HEAD_SEQ_KEY,
    IndexApplyState, IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexEntry,
    IndexMutation, InternalIndexError, InternalIndexSpace, InternalKeyRange, UserKeyRange,
    initialization_batch, is_encoded_empty_durable_frontier, is_encoded_head_seq_zero,
};
use crate::lock::{
    ManagedEntryKind, RootLock, invalid_argument_error, layout_error, not_found_error,
    open_io_error, sync_directory_tree_nofollow, sync_file_data, validate_directory_tree_nofollow,
};
#[cfg(not(test))]
use crate::recovery::{analyze_recovery, execute_recovery};
#[cfg(not(test))]
use crate::runtime::{ExternalLease, LifecycleController, OperationGuard, RuntimeControl};
#[cfg(not(test))]
use crate::stats::StatsState;
#[cfg(not(test))]
use crate::vlog::file_set::{FileCatalog, FileSet};
use crate::vlog::file_set::{VLogDirectory, read_exact_at};
use crate::vlog::format::{
    FILE_HEADER_ENCODED_LEN, MAX_VLOG_FILE_SIZE, PAGE_HEADER_ENCODED_LEN, PageHeader,
    VLogFileHeader,
};
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

    fn iter_user_range(&self, range: UserKeyRange) -> Result<UserIndexIterator> {
        let iterator = self.iter_user()?;
        Ok(Box::new(iterator.filter(move |entry| match entry {
            Ok(entry) => range.contains(&entry.key),
            Err(_) => true,
        })))
    }
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

    fn iter_user_range(&self, range: UserKeyRange) -> Result<UserIndexIterator> {
        self.backend.iter_user_range(Some(&self.snapshot), range)
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

const CLEANUP_MAX_KEYS_PER_BATCH: usize = 1_024;
const CLEANUP_MAX_ENCODED_KEY_BYTES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DescriptorCleanupProgress {
    pub(crate) captured_durable_seq: u64,
    pub(crate) deleted_mutations: usize,
    pub(crate) deleted_meta: usize,
    pub(crate) committed_batches: usize,
    pub(crate) blocked_by_recovery: bool,
}

pub(crate) fn cleanup_descriptors_once<B: IndexBackend>(
    backend: &B,
    stop: &AtomicBool,
) -> Result<DescriptorCleanupProgress> {
    let recovery_state = backend
        .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)
        .map_err(background_index_error)?;
    if recovery_state.is_some() {
        return Ok(DescriptorCleanupProgress {
            blocked_by_recovery: true,
            ..DescriptorCleanupProgress::default()
        });
    }

    let encoded_frontier = backend
        .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)
        .map_err(background_index_error)?
        .ok_or_else(background_corruption)?;
    let captured_durable_seq = DurableFrontier::decode(&encoded_frontier)
        .map_err(background_index_error)?
        .durable_seq;
    let mut progress = DescriptorCleanupProgress {
        captured_durable_seq,
        ..DescriptorCleanupProgress::default()
    };
    if stop.load(Ordering::Acquire) {
        return Ok(progress);
    }

    let range = stable_descriptor_range(captured_durable_seq)?;
    let entries = backend
        .scan_internal(InternalIndexSpace::Transaction, range)
        .map_err(background_index_error)?;
    let mut current_seq = None;
    let mut meta_key = None;
    let mut saw_mutation = false;
    let mut last_ordinal = None;
    let mut pending_mutations = Vec::new();
    let mut pending_key_bytes = 0_usize;

    for entry in entries {
        if stop.load(Ordering::Acquire) {
            return Ok(progress);
        }
        let entry = entry.map_err(background_index_error)?;
        let (commit_seq, kind) = decode_cleanup_key(&entry.key)?;
        if commit_seq > captured_durable_seq {
            return Err(background_corruption());
        }
        if current_seq.is_some_and(|current| current != commit_seq) {
            flush_cleanup_transaction(
                backend,
                stop,
                &mut pending_mutations,
                &mut pending_key_bytes,
                &mut meta_key,
                &mut progress,
            )?;
            if stop.load(Ordering::Acquire) {
                return Ok(progress);
            }
            saw_mutation = false;
            last_ordinal = None;
        }
        current_seq = Some(commit_seq);

        match kind {
            CleanupKeyKind::Meta => {
                if saw_mutation || meta_key.replace(entry.key).is_some() {
                    return Err(background_corruption());
                }
            }
            CleanupKeyKind::Mutation(ordinal) => {
                if last_ordinal.is_some_and(|previous| previous >= ordinal) {
                    return Err(background_corruption());
                }
                last_ordinal = Some(ordinal);
                saw_mutation = true;
                pending_key_bytes = pending_key_bytes
                    .checked_add(entry.key.len())
                    .ok_or_else(background_resource_exhausted)?;
                pending_mutations
                    .try_reserve(1)
                    .map_err(|_| background_resource_exhausted())?;
                pending_mutations.push(entry.key);
                if pending_mutations.len() >= CLEANUP_MAX_KEYS_PER_BATCH
                    || pending_key_bytes >= CLEANUP_MAX_ENCODED_KEY_BYTES
                {
                    commit_cleanup_keys(
                        backend,
                        stop,
                        &mut pending_mutations,
                        &mut pending_key_bytes,
                        false,
                        &mut progress,
                    )?;
                }
            }
        }
    }

    flush_cleanup_transaction(
        backend,
        stop,
        &mut pending_mutations,
        &mut pending_key_bytes,
        &mut meta_key,
        &mut progress,
    )?;
    Ok(progress)
}

#[derive(Clone, Copy)]
enum CleanupKeyKind {
    Meta,
    Mutation(u64),
}

fn decode_cleanup_key(key: &[u8]) -> Result<(u64, CleanupKeyKind)> {
    match key.len() {
        11 => decode_tx_meta_key(key)
            .map(|seq| (seq, CleanupKeyKind::Meta))
            .map_err(background_index_error),
        19 => decode_tx_mutation_key(key)
            .map(|(seq, ordinal)| (seq, CleanupKeyKind::Mutation(ordinal)))
            .map_err(background_index_error),
        _ => Err(background_corruption()),
    }
}

fn stable_descriptor_range(captured_durable_seq: u64) -> Result<InternalKeyRange> {
    let end_exclusive = captured_durable_seq
        .checked_add(1)
        .map(|next_seq| {
            encode_tx_meta_key(next_seq)
                .map_err(background_index_error)
                .and_then(|key| try_copy_background_bytes(&key[..10]))
        })
        .transpose()?;
    InternalKeyRange::new(None, end_exclusive).map_err(background_internal_index_error)
}

fn flush_cleanup_transaction<B: IndexBackend>(
    backend: &B,
    stop: &AtomicBool,
    pending_mutations: &mut Vec<Vec<u8>>,
    pending_key_bytes: &mut usize,
    meta_key: &mut Option<Vec<u8>>,
    progress: &mut DescriptorCleanupProgress,
) -> Result<()> {
    commit_cleanup_keys(
        backend,
        stop,
        pending_mutations,
        pending_key_bytes,
        false,
        progress,
    )?;
    if stop.load(Ordering::Acquire) {
        return Ok(());
    }
    if let Some(key) = meta_key.take() {
        let mut keys = Vec::new();
        keys.try_reserve_exact(1)
            .map_err(|_| background_resource_exhausted())?;
        keys.push(key);
        let mut encoded_key_bytes = keys[0].len();
        commit_cleanup_keys(
            backend,
            stop,
            &mut keys,
            &mut encoded_key_bytes,
            true,
            progress,
        )?;
    }
    Ok(())
}

fn commit_cleanup_keys<B: IndexBackend>(
    backend: &B,
    stop: &AtomicBool,
    keys: &mut Vec<Vec<u8>>,
    encoded_key_bytes: &mut usize,
    deleting_meta: bool,
    progress: &mut DescriptorCleanupProgress,
) -> Result<()> {
    if keys.is_empty() || stop.load(Ordering::Acquire) {
        return Ok(());
    }
    let key_count = keys.len();
    let mut batch =
        IndexAtomicBatch::try_with_capacity(key_count).map_err(background_internal_index_error)?;
    for key in keys.drain(..) {
        batch
            .try_push(IndexMutation::DeleteInternal {
                space: InternalIndexSpace::Transaction,
                key,
            })
            .map_err(background_internal_index_error)?;
    }
    *encoded_key_bytes = 0;
    backend
        .commit_atomic(batch, IndexCommitMode::Buffer)
        .map_err(background_cleanup_commit_error)?;
    if deleting_meta {
        progress.deleted_meta = progress
            .deleted_meta
            .checked_add(key_count)
            .ok_or_else(background_resource_exhausted)?;
    } else {
        progress.deleted_mutations = progress
            .deleted_mutations
            .checked_add(key_count)
            .ok_or_else(background_resource_exhausted)?;
    }
    progress.committed_batches = progress
        .committed_batches
        .checked_add(1)
        .ok_or_else(background_resource_exhausted)?;
    Ok(())
}

fn try_copy_background_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| background_resource_exhausted())?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn background_index_error(mut error: StorageError) -> StorageError {
    error.operation = Operation::Background;
    error.protocol_stage = ProtocolStage::Maintenance;
    error.write_outcome = None;
    error.instance_state = None;
    if matches!(
        error.kind,
        StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted
    ) {
        error.retry_advice = RetryAdvice::RetrySameInstance;
    }
    error
}

fn background_internal_index_error(error: InternalIndexError) -> StorageError {
    let mut mapped = StorageError::codec_error(
        error.kind,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        retry_advice_for(error.kind),
    );
    mapped.os_code = error.os_code;
    mapped
}

pub(crate) fn background_cleanup_commit_error(error: IndexCommitError) -> StorageError {
    let mut mapped = background_internal_index_error(error.source);
    match error.apply_state {
        IndexApplyState::Unknown => mapped.retry_advice = RetryAdvice::ReopenAndVerify,
        IndexApplyState::NotApplied
            if matches!(
                mapped.kind,
                StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted
            ) =>
        {
            mapped.retry_advice = RetryAdvice::RetrySameInstance;
        }
        // Even before Fjall commit is entered, an I/O failure may originate
        // from reading DatabaseIdentity. The batch is known not to have been
        // applied, but the live index can no longer be treated as trustworthy.
        IndexApplyState::NotApplied if mapped.kind == StorageErrorKind::Io => {
            mapped.retry_advice = RetryAdvice::ReopenAndVerify;
        }
        IndexApplyState::NotApplied => {}
    }
    mapped
}

fn background_corruption() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Corruption,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn background_resource_exhausted() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::RetrySameInstance,
    )
}

#[cfg(not(test))]
struct DescriptorCleanupWorker {
    sender: Option<SyncSender<()>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    runtime: Arc<RuntimeControl>,
}

#[cfg(not(test))]
impl DescriptorCleanupWorker {
    fn start(backend: Arc<FjallBackend>, runtime: Arc<RuntimeControl>) -> Result<Self> {
        let (sender, receiver) = sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_runtime = Arc::clone(&runtime);
        let handle = thread::Builder::new()
            .name("rustkv-descriptor-cleanup".to_owned())
            .spawn(move || {
                while receiver.recv().is_ok() {
                    if worker_stop.load(Ordering::Acquire) {
                        break;
                    }
                    if worker_runtime.state().instance_state != InstanceState::Healthy {
                        continue;
                    }
                    if let Err(error) = cleanup_descriptors_once(backend.as_ref(), &worker_stop) {
                        latch_background_failure(&worker_runtime, &error);
                    }
                }
            })
            .map_err(background_spawn_error)?;
        Ok(Self {
            sender: Some(sender),
            stop,
            handle: Some(handle),
            runtime,
        })
    }

    fn trigger(&self) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        match sender.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {}
            Err(TrySendError::Disconnected(())) => {
                let error = background_worker_stopped_error();
                self.runtime.latch_failure(InstanceState::Poisoned, &error);
            }
        }
    }
}

#[cfg(not(test))]
impl Drop for DescriptorCleanupWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn background_failure_target(error: &StorageError) -> Option<InstanceState> {
    // The final retry advice carries the apply-state decision. In particular,
    // an Unknown cleanup commit is always mapped to ReopenAndVerify and must
    // not become retryable merely because its source kind is Busy or
    // ResourceExhausted.
    if error.retry_advice == RetryAdvice::RetrySameInstance {
        return None;
    }
    Some(if error.kind == StorageErrorKind::StorageWriteStopped {
        InstanceState::WriteStopped
    } else {
        InstanceState::Poisoned
    })
}

#[cfg(not(test))]
fn latch_background_failure(runtime: &RuntimeControl, error: &StorageError) {
    if let Some(target) = background_failure_target(error) {
        runtime.latch_failure(target, error);
    }
}

#[cfg(not(test))]
fn background_spawn_error(error: io::Error) -> StorageError {
    let mut mapped = StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Open,
        ProtocolStage::Lifecycle,
        None,
        RetryAdvice::RetrySameInstance,
    );
    mapped.os_code = error.raw_os_error();
    mapped
}

#[cfg(not(test))]
fn background_worker_stopped_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        Operation::Background,
        ProtocolStage::Maintenance,
        None,
        RetryAdvice::ReopenAndVerify,
    )
}

#[cfg(not(test))]
type LiveCommitCoordinator = CommitCoordinator<LateBoundFjallBackend, OsTxUuidSource>;

#[cfg(not(test))]
struct LiveComponents {
    // The lifecycle controller closes admission before protocol resources are
    // released. RootLock remains last and therefore outlives every component.
    cleanup: DescriptorCleanupWorker,
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
        if !lifecycle.bind_runtime(Arc::clone(&runtime)) {
            return Err(lifecycle_binding_error());
        }
        let read_path = ReadPath {
            runtime: Arc::clone(&runtime) as Arc<dyn ReadRuntime>,
            index: Arc::clone(&index_binding) as Arc<dyn UserIndexReader>,
            values: reader as Arc<dyn ValueReader>,
        };

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
            .bind(Arc::clone(&index))
            .map_err(|_| late_bound_index_error())?;

        let cleanup = DescriptorCleanupWorker::start(index, Arc::clone(&runtime))?;
        let inner = Arc::new(DbInner {
            read_path,
            live: LiveComponents {
                cleanup,
                lifecycle,
                runtime,
                coordinator,
                _options: Arc::new(copy_options(options)),
                _root_lock: root_lock,
            },
        });
        inner.live.cleanup.trigger();

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
        let result = self.inner.live.coordinator.commit_nonempty(&write);
        if result.is_ok() && options.sync {
            self.inner.live.cleanup.trigger();
        }
        result
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
        let result = self.inner.live.coordinator.commit_nonempty(&write);
        if result.is_ok() && options.sync {
            self.inner.live.cleanup.trigger();
        }
        result
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
            let durable_before = self.inner.live.coordinator.state_snapshot().durable_seq;
            let result = self.inner.live.coordinator.commit_empty_batch(options.sync);
            let durable_after = self.inner.live.coordinator.state_snapshot().durable_seq;
            if result.is_ok() && options.sync && durable_after > durable_before {
                self.inner.live.cleanup.trigger();
            }
            return result;
        }
        self.inner
            .live
            .runtime
            .check_write_admission(Operation::WriteBatch)?;
        let write = preflight_batch(batch, options.sync)?;
        let result = self.inner.live.coordinator.commit_nonempty(&write);
        if result.is_ok() && options.sync {
            self.inner.live.cleanup.trigger();
        }
        result
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
        let start = range
            .start
            .map(|bound| clone_range_bound(bound, started.instance_state))
            .transpose()?;
        let view = self
            .inner
            .select_read_view(options, Operation::Range, started)?;
        let iterator = view
            .iter_user_range(UserKeyRange::forward(start, end.clone()))
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
        inner.seek_to_first_in_initial_range();
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
        destroy_database(path.as_ref(), options, DestroyFaultPoint::None)
    }

    #[cfg(not(test))]
    fn begin_operation(&self, operation: Operation) -> Result<PublicOperationGuard> {
        self.inner.begin_operation(operation)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DestroyFaultPoint {
    None,
    Inventory,
    DatabaseIdentity,
    VLogFileRemove,
    VLogDirectorySync,
    VLogDirectoryRemove,
    VLogRootSync,
    IndexRemove,
    #[cfg(test)]
    IndexRemoveAfterEntry,
    IndexRootSync,
    FormatTemporaryRemove,
    FormatTemporarySync,
    FormatRemove,
    FormatSync,
    #[cfg(test)]
    EmptyInventoryRootSync,
}

struct DestroyInventory {
    format: Option<FormatMetadataV0>,
    temporary_format: Option<FormatMetadataV0>,
    index_present: bool,
    vlog_present: bool,
    vlog_files: Vec<VLogInventoryEntry>,
}

#[cfg(test)]
pub(crate) fn destroy_with_fault_for_test(
    path: &Path,
    options: &Options,
    fault: DestroyFaultPoint,
) -> Result<()> {
    destroy_database(path, options, fault)
}

fn destroy_database(path: &Path, options: &Options, fault: DestroyFaultPoint) -> Result<()> {
    let root = match RootLock::acquire(path, false) {
        Ok(Some(root)) => root,
        Ok(None) => return Ok(()),
        Err(error) => {
            return Err(destroy_failure(
                error,
                ManagedObject::Lock,
                DestroyStage::AcquireLock,
                false,
            ));
        }
    };
    inject_destroy_fault(
        fault,
        DestroyFaultPoint::Inventory,
        ManagedObject::Format,
        DestroyStage::Inventory,
        false,
    )?;
    let inventory = inventory_for_destroy(&root, options, fault)?;
    if inventory.format.is_none() && inventory.temporary_format.is_none() {
        #[cfg(test)]
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::EmptyInventoryRootSync,
            ManagedObject::Format,
            DestroyStage::SyncDirectory,
            false,
        )?;
        root.sync_root().map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::Format,
                DestroyStage::SyncDirectory,
                false,
            )
        })?;
        return Ok(());
    }
    let mut partially_deleted = false;

    if inventory.vlog_present {
        let vlog_path = root.child_path(VLOG_DIRECTORY_NAME).map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::Inventory,
                partially_deleted,
            )
        })?;
        let directory = VLogDirectory::open(&vlog_path).map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::Inventory,
                partially_deleted,
            )
        })?;
        for entry in inventory.vlog_files.iter().rev() {
            inject_destroy_fault(
                fault,
                DestroyFaultPoint::VLogFileRemove,
                ManagedObject::VLogFile {
                    file_id: entry.file_id,
                },
                DestroyStage::RemoveFile,
                partially_deleted,
            )?;
            directory
                .remove_file_for_destroy(entry.file_id)
                .map_err(|error| {
                    destroy_io_failure(
                        error,
                        ManagedObject::VLogFile {
                            file_id: entry.file_id,
                        },
                        DestroyStage::RemoveFile,
                        partially_deleted,
                    )
                })?;
            partially_deleted = true;
        }
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::VLogDirectorySync,
            ManagedObject::VLogDirectory,
            DestroyStage::SyncDirectory,
            partially_deleted,
        )?;
        directory.sync().map_err(|error| {
            destroy_io_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::SyncDirectory,
                partially_deleted,
            )
        })?;
        drop(directory);

        inject_destroy_fault(
            fault,
            DestroyFaultPoint::VLogDirectoryRemove,
            ManagedObject::VLogDirectory,
            DestroyStage::RemoveTree,
            partially_deleted,
        )?;
        if root
            .remove_empty_directory_tracked(VLOG_DIRECTORY_NAME)
            .map_err(|error| {
                destroy_failure(
                    error,
                    ManagedObject::VLogDirectory,
                    DestroyStage::RemoveTree,
                    partially_deleted,
                )
            })?
        {
            partially_deleted = true;
        }
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::VLogRootSync,
            ManagedObject::VLogDirectory,
            DestroyStage::SyncDirectory,
            partially_deleted,
        )?;
        root.sync_root().map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::SyncDirectory,
                partially_deleted,
            )
        })?;
    } else if inventory.index_present {
        // Absence alone does not prove that a prior VLog unlink, if any, is
        // durable. Reissue that barrier before beginning the next destructive
        // stage so a crash cannot resurrect VLog beside a partially deleted
        // index.
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::VLogRootSync,
            ManagedObject::VLogDirectory,
            DestroyStage::SyncDirectory,
            partially_deleted,
        )?;
        root.sync_root().map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::SyncDirectory,
                partially_deleted,
            )
        })?;
    }

    if inventory.index_present {
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::IndexRemove,
            ManagedObject::IndexDirectory,
            DestroyStage::RemoveTree,
            partially_deleted,
        )?;
        #[cfg(test)]
        let index_removal = if fault == DestroyFaultPoint::IndexRemoveAfterEntry {
            root.remove_directory_tree_with_midway_failure_for_test(
                INDEX_DIRECTORY_NAME,
                &mut partially_deleted,
            )
        } else {
            root.remove_directory_tree_tracked(INDEX_DIRECTORY_NAME, &mut partially_deleted)
        };
        #[cfg(not(test))]
        let index_removal =
            root.remove_directory_tree_tracked(INDEX_DIRECTORY_NAME, &mut partially_deleted);
        index_removal.map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::IndexDirectory,
                DestroyStage::RemoveTree,
                partially_deleted,
            )
        })?;
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::IndexRootSync,
            ManagedObject::IndexDirectory,
            DestroyStage::SyncDirectory,
            partially_deleted,
        )?;
        root.sync_root().map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::IndexDirectory,
                DestroyStage::SyncDirectory,
                partially_deleted,
            )
        })?;
    } else {
        // As above, absence alone does not prove that the index unlink is
        // durable. Confirm the prior stage before removing either FORMAT
        // marker, which is the last evidence needed for safe Destroy reentry.
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::IndexRootSync,
            ManagedObject::IndexDirectory,
            DestroyStage::SyncDirectory,
            partially_deleted,
        )?;
        root.sync_root().map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::IndexDirectory,
                DestroyStage::SyncDirectory,
                partially_deleted,
            )
        })?;
    }

    if inventory.temporary_format.is_some() {
        remove_destroy_format(&root, fault, true, &mut partially_deleted)?;
    }
    if inventory.format.is_some() {
        remove_destroy_format(&root, fault, false, &mut partially_deleted)?;
    }
    Ok(())
}

fn remove_destroy_format(
    root: &RootLock,
    fault: DestroyFaultPoint,
    temporary: bool,
    partially_deleted: &mut bool,
) -> Result<()> {
    let (name, object, remove_fault, sync_fault) = if temporary {
        (
            FORMAT_TEMP_FILE_NAME,
            ManagedObject::FormatTemporary,
            DestroyFaultPoint::FormatTemporaryRemove,
            DestroyFaultPoint::FormatTemporarySync,
        )
    } else {
        (
            FORMAT_FILE_NAME,
            ManagedObject::Format,
            DestroyFaultPoint::FormatRemove,
            DestroyFaultPoint::FormatSync,
        )
    };
    inject_destroy_fault(
        fault,
        remove_fault,
        object,
        DestroyStage::RemoveFile,
        *partially_deleted,
    )?;
    let object = if temporary {
        ManagedObject::FormatTemporary
    } else {
        ManagedObject::Format
    };
    if root.remove_regular_child_tracked(name).map_err(|error| {
        destroy_failure(error, object, DestroyStage::RemoveFile, *partially_deleted)
    })? {
        *partially_deleted = true;
    }
    let object = if temporary {
        ManagedObject::FormatTemporary
    } else {
        ManagedObject::Format
    };
    inject_destroy_fault(
        fault,
        sync_fault,
        object,
        DestroyStage::SyncDirectory,
        *partially_deleted,
    )?;
    let object = if temporary {
        ManagedObject::FormatTemporary
    } else {
        ManagedObject::Format
    };
    root.sync_root().map_err(|error| {
        destroy_failure(
            error,
            object,
            DestroyStage::SyncDirectory,
            *partially_deleted,
        )
    })
}

fn inventory_for_destroy(
    root: &RootLock,
    options: &Options,
    fault: DestroyFaultPoint,
) -> Result<DestroyInventory> {
    let format_kind = root.inspect_child(FORMAT_FILE_NAME).map_err(|error| {
        destroy_failure(error, ManagedObject::Format, DestroyStage::Inventory, false)
    })?;
    let temporary_kind = root.inspect_child(FORMAT_TEMP_FILE_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::FormatTemporary,
            DestroyStage::Inventory,
            false,
        )
    })?;
    validate_destroy_file_kind(format_kind, ManagedObject::Format)?;
    validate_destroy_file_kind(temporary_kind, ManagedObject::FormatTemporary)?;
    if !matches!(format_kind, ManagedEntryKind::Missing)
        && !matches!(temporary_kind, ManagedEntryKind::Missing)
    {
        return Err(destroy_layout_failure(
            ManagedObject::FormatTemporary,
            DestroyStage::Inventory,
            false,
        ));
    }

    let format = read_format(root, false).map_err(|error| {
        destroy_failure(error, ManagedObject::Format, DestroyStage::Inventory, false)
    })?;
    let temporary_format = read_format(root, true).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::FormatTemporary,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let index_kind = root.inspect_child(INDEX_DIRECTORY_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::IndexDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let vlog_kind = root.inspect_child(VLOG_DIRECTORY_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::VLogDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let index_present = validate_destroy_directory_kind(index_kind, ManagedObject::IndexDirectory)?;
    let vlog_present = validate_destroy_directory_kind(vlog_kind, ManagedObject::VLogDirectory)?;

    if format.is_none() && temporary_format.is_none() {
        if index_present {
            return Err(destroy_layout_failure(
                ManagedObject::IndexDirectory,
                DestroyStage::Inventory,
                false,
            ));
        }
        if vlog_present {
            return Err(destroy_layout_failure(
                ManagedObject::VLogDirectory,
                DestroyStage::Inventory,
                false,
            ));
        }
        return Ok(DestroyInventory {
            format,
            temporary_format,
            index_present,
            vlog_present,
            vlog_files: Vec::new(),
        });
    }

    if index_present {
        validate_destroy_index(
            root,
            options,
            format.as_ref(),
            temporary_format.as_ref(),
            format.is_some() && !vlog_present,
            fault,
        )?;
    } else if format.is_some() && vlog_present {
        return Err(destroy_corruption_failure(
            ManagedObject::DatabaseIdentity,
            DestroyStage::Inventory,
            false,
        ));
    }

    let vlog_files = if vlog_present {
        if temporary_format.is_some() {
            if destroy_directory_has_entries(
                root,
                VLOG_DIRECTORY_NAME,
                ManagedObject::VLogDirectory,
            )? {
                return Err(destroy_layout_failure(
                    ManagedObject::VLogDirectory,
                    DestroyStage::Inventory,
                    false,
                ));
            }
            Vec::new()
        } else {
            inventory_vlog_for_destroy(
                root,
                format.as_ref().ok_or_else(|| {
                    destroy_layout_failure(ManagedObject::Format, DestroyStage::Inventory, false)
                })?,
            )?
        }
    } else {
        Vec::new()
    };

    Ok(DestroyInventory {
        format,
        temporary_format,
        index_present,
        vlog_present,
        vlog_files,
    })
}

fn validate_destroy_index(
    root: &RootLock,
    options: &Options,
    format: Option<&FormatMetadataV0>,
    temporary_format: Option<&FormatMetadataV0>,
    allow_partially_deleted_index: bool,
    fault: DestroyFaultPoint,
) -> Result<()> {
    let index_path = root.child_path(INDEX_DIRECTORY_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::IndexDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    validate_directory_tree_nofollow(&index_path).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::IndexDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let is_empty =
        !destroy_directory_has_entries(root, INDEX_DIRECTORY_NAME, ManagedObject::IndexDirectory)?;

    if let Some(format) = format {
        if is_empty && allow_partially_deleted_index {
            return Ok(());
        }
        if is_empty {
            return Err(destroy_corruption_failure(
                ManagedObject::DatabaseIdentity,
                DestroyStage::Inventory,
                false,
            ));
        }
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::DatabaseIdentity,
            ManagedObject::DatabaseIdentity,
            DestroyStage::Inventory,
            false,
        )?;
        let validation = FjallBackend::open_existing_read_only_for_destroy(
            &index_path,
            options.fjall_index_options(),
        )
        .and_then(|verification| {
            let result = validate_final_identity(verification.backend(), format);
            let close = verification.close();
            result.and(close)
        });
        match validation {
            Ok(()) => {}
            Err(error) if allow_partially_deleted_index => {
                let _ = error;
            }
            Err(error) => {
                return Err(destroy_failure(
                    error,
                    ManagedObject::DatabaseIdentity,
                    DestroyStage::Inventory,
                    false,
                ));
            }
        }
    } else if let Some(temporary) = temporary_format {
        if is_empty {
            return Ok(());
        }
        inject_destroy_fault(
            fault,
            DestroyFaultPoint::DatabaseIdentity,
            ManagedObject::DatabaseIdentity,
            DestroyStage::Inventory,
            false,
        )?;
        let validation = FjallBackend::open_existing_read_only_for_destroy(
            &index_path,
            options.fjall_index_options(),
        )
        .and_then(|verification| {
            let result = validate_interrupted_index(verification.backend(), temporary);
            let close = verification.close();
            result.and(close)
        });
        match validation {
            Ok(()) => {}
            Err(error) if allow_partially_deleted_index => {
                let _ = error;
            }
            Err(error) => {
                return Err(destroy_failure(
                    error,
                    ManagedObject::DatabaseIdentity,
                    DestroyStage::Inventory,
                    false,
                ));
            }
        }
    }
    Ok(())
}

fn destroy_directory_has_entries(
    root: &RootLock,
    name: &str,
    object: ManagedObject,
) -> Result<bool> {
    root.read_directory(name)
        .and_then(|mut entries| entries.next().transpose().map_err(open_io_error))
        .map(|entry| entry.is_some())
        .map_err(|error| destroy_failure(error, object, DestroyStage::Inventory, false))
}

fn inventory_vlog_for_destroy(
    root: &RootLock,
    format: &FormatMetadataV0,
) -> Result<Vec<VLogInventoryEntry>> {
    let path = root.child_path(VLOG_DIRECTORY_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::VLogDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let directory = VLogDirectory::open(&path).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::VLogDirectory,
            DestroyStage::Inventory,
            false,
        )
    })?;
    let mut files = Vec::new();
    for entry in root.read_directory(VLOG_DIRECTORY_NAME).map_err(|error| {
        destroy_failure(
            error,
            ManagedObject::VLogDirectory,
            DestroyStage::Inventory,
            false,
        )
    })? {
        let entry = entry.map_err(|error| {
            destroy_io_failure(
                error,
                ManagedObject::VLogDirectory,
                DestroyStage::Inventory,
                false,
            )
        })?;
        let name = entry.file_name().into_string().map_err(|_| {
            destroy_layout_failure(ManagedObject::VLogDirectory, DestroyStage::Inventory, false)
        })?;
        let file_id = parse_destroy_vlog_name(&name).ok_or_else(|| {
            destroy_layout_failure(ManagedObject::VLogDirectory, DestroyStage::Inventory, false)
        })?;
        let file_type = entry.file_type().map_err(|error| {
            destroy_io_failure(
                error,
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            )
        })?;
        if !file_type.is_file() {
            return Err(destroy_layout_failure(
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            ));
        }
        let file = directory.open_read_only(file_id).map_err(|error| {
            destroy_io_failure(
                error,
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            )
        })?;
        let len = file
            .metadata()
            .map_err(|error| {
                destroy_io_failure(
                    error,
                    ManagedObject::VLogFile { file_id },
                    DestroyStage::Inventory,
                    false,
                )
            })?
            .len();
        let minimum_header_len =
            u64::try_from(PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN)
                .map_err(|_| destroy_resource_failure(ManagedObject::VLogFile { file_id }))?;
        if len > MAX_VLOG_FILE_SIZE || len < minimum_header_len {
            return Err(destroy_corruption_failure(
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            ));
        }

        let mut page = [0_u8; PAGE_HEADER_ENCODED_LEN];
        read_exact_at(&file, &mut page, 0, file_id).map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            )
        })?;
        let page = PageHeader::decode(&page).map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            )
        })?;
        let mut header = [0_u8; FILE_HEADER_ENCODED_LEN];
        read_exact_at(&file, &mut header, PAGE_HEADER_ENCODED_LEN as u64, file_id).map_err(
            |error| {
                destroy_failure(
                    error,
                    ManagedObject::VLogFile { file_id },
                    DestroyStage::Inventory,
                    false,
                )
            },
        )?;
        let header = VLogFileHeader::decode(&header).map_err(|error| {
            destroy_failure(
                error,
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            )
        })?;
        if page.file_id != file_id
            || page.page_no != 0
            || header.file_id != file_id
            || header.database_uuid != format.database_uuid
            || header.format_version != format.format_version
        {
            return Err(destroy_corruption_failure(
                ManagedObject::VLogFile { file_id },
                DestroyStage::Inventory,
                false,
            ));
        }
        files
            .try_reserve(1)
            .map_err(|_| destroy_resource_failure(ManagedObject::VLogDirectory))?;
        files.push(VLogInventoryEntry {
            file_id,
            len,
            path: entry.path(),
        });
    }
    files.sort_unstable_by_key(|entry| entry.file_id);
    Ok(files)
}

fn parse_destroy_vlog_name(name: &str) -> Option<u32> {
    let digits = name.strip_prefix('D')?.strip_suffix(".data")?;
    if digits.len() != 6 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().filter(|file_id| *file_id <= 999_999)
}

fn validate_destroy_file_kind(kind: ManagedEntryKind, object: ManagedObject) -> Result<()> {
    if matches!(
        kind,
        ManagedEntryKind::Missing | ManagedEntryKind::RegularFile { .. }
    ) {
        Ok(())
    } else {
        Err(destroy_layout_failure(
            object,
            DestroyStage::Inventory,
            false,
        ))
    }
}

fn validate_destroy_directory_kind(kind: ManagedEntryKind, object: ManagedObject) -> Result<bool> {
    match kind {
        ManagedEntryKind::Missing => Ok(false),
        ManagedEntryKind::Directory => Ok(true),
        _ => Err(destroy_layout_failure(
            object,
            DestroyStage::Inventory,
            false,
        )),
    }
}

fn inject_destroy_fault(
    actual: DestroyFaultPoint,
    expected: DestroyFaultPoint,
    object: ManagedObject,
    stage: DestroyStage,
    partially_deleted: bool,
) -> Result<()> {
    if actual == expected {
        Err(destroy_io_failure(
            io::Error::from_raw_os_error(5),
            object,
            stage,
            partially_deleted,
        ))
    } else {
        Ok(())
    }
}

fn destroy_failure(
    mut error: StorageError,
    object: ManagedObject,
    stage: DestroyStage,
    partially_deleted: bool,
) -> StorageError {
    error.operation = Operation::Destroy;
    error.protocol_stage = ProtocolStage::Lifecycle;
    error.write_outcome = None;
    error.instance_state = None;
    error.retry_advice = retry_advice_for(error.kind);
    error.destroy_failure = Some(DestroyFailureContext {
        failed_object: object,
        stage,
        partially_deleted,
        os_code: error.os_code,
    });
    error
}

fn destroy_io_failure(
    error: io::Error,
    object: ManagedObject,
    stage: DestroyStage,
    partially_deleted: bool,
) -> StorageError {
    let mut mapped = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Destroy,
        ProtocolStage::Lifecycle,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    mapped.os_code = error.raw_os_error();
    destroy_failure(mapped, object, stage, partially_deleted)
}

fn destroy_layout_failure(
    object: ManagedObject,
    stage: DestroyStage,
    partially_deleted: bool,
) -> StorageError {
    destroy_failure(layout_error(), object, stage, partially_deleted)
}

fn destroy_corruption_failure(
    object: ManagedObject,
    stage: DestroyStage,
    partially_deleted: bool,
) -> StorageError {
    destroy_failure(
        metadata_error(StorageErrorKind::Corruption),
        object,
        stage,
        partially_deleted,
    )
}

fn destroy_resource_failure(object: ManagedObject) -> StorageError {
    destroy_failure(
        StorageError::codec_error(
            StorageErrorKind::ResourceExhausted,
            Operation::Destroy,
            ProtocolStage::Lifecycle,
            None,
            RetryAdvice::RetrySameInstance,
        ),
        object,
        DestroyStage::Inventory,
        false,
    )
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

#[cfg(not(test))]
fn lifecycle_binding_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::StoragePoisoned,
        Operation::Open,
        ProtocolStage::Lifecycle,
        None,
        RetryAdvice::ReopenAndVerify,
    )
}
