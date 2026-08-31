//! Fjall 3.1.8 index-backend adapter.

use std::error::Error as StdError;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(test)]
use std::sync::atomic::AtomicBool;

use ::fjall::compaction::Leveled;
use ::fjall::config::{BlockSizePolicy, CompressionPolicy, RestartIntervalPolicy};
use ::fjall::{
    CompressionType, Database, Guard, Iter, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch,
    PersistMode, Readable,
};

use super::{
    DATABASE_IDENTITY_KEY, FjallIndexOptions, IndexAtomicBatch, IndexBackend, IndexCommitError,
    IndexCommitMode, IndexCompression, IndexEntry, IndexMutation, InternalIndexError,
    InternalIndexSpace, InternalKeyRange,
};
use crate::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

const USER_INDEX_NAME: &str = "rustkv_user_index";
const TX_METADATA_NAME: &str = "rustkv_txn_metadata";
const SYSTEM_METADATA_NAME: &str = "rustkv_system_metadata";
const FJALL_VERSION_MARKER: &str = "version";
const FJALL_LOCK_FILE: &str = "lock";
const FJALL_KEYSPACES_DIRECTORY: &str = "keyspaces";
const FJALL_LSM_CURRENT_MARKER: &str = "current";
const FJALL_LSM_TABLES_DIRECTORY: &str = "tables";
const FJALL_LSM_CURRENT_LEN: usize = 25;
const MIN_FJALL_OPEN_FILES: usize = 10;
const MIN_FJALL_BLOCK_SIZE: usize = 1_024;
const MAX_FJALL_BLOCK_SIZE: usize = 4 * 1_024 * 1_024;
// Fjall 3.1.8's default leveled strategy calculates L1 as target_size * 4.
const MAX_FJALL_TABLE_TARGET_SIZE: u64 = u64::MAX / 4;
static NEXT_READ_ONLY_SHADOW_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
struct ValidatedOptions {
    original: FjallIndexOptions,
    write_buffer_size: u64,
    block_cache_size: u64,
    block_size: u32,
    block_restart_interval: u8,
    max_file_size: u64,
    compression: CompressionType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackgroundWorkerMode {
    Enabled,
    Disabled,
}

#[derive(Clone)]
pub(crate) struct FjallSnapshot {
    inner: ::fjall::Snapshot,
    owner: Arc<()>,
}

pub(crate) struct FjallEntryIterator {
    inner: Iter,
    operation: Operation,
    protocol_stage: ProtocolStage,
    #[cfg(test)]
    injected_error_after: Option<usize>,
    #[cfg(test)]
    successful_entries: usize,
    #[cfg(test)]
    injected_error_emitted: bool,
}

impl FjallEntryIterator {
    fn new(inner: Iter, operation: Operation, protocol_stage: ProtocolStage) -> Self {
        Self {
            inner,
            operation,
            protocol_stage,
            #[cfg(test)]
            injected_error_after: None,
            #[cfg(test)]
            successful_entries: 0,
            #[cfg(test)]
            injected_error_emitted: false,
        }
    }

    #[cfg(test)]
    fn inject_error_after(mut self, successful_entries: Option<usize>) -> Self {
        self.injected_error_after = successful_entries;
        self
    }

    #[cfg(test)]
    fn take_injected_error(&mut self) -> Option<Result<IndexEntry>> {
        if !self.injected_error_emitted
            && self.injected_error_after == Some(self.successful_entries)
        {
            self.injected_error_emitted = true;
            return Some(Err(sanitized_storage_error(
                StorageErrorKind::Io,
                None,
                self.operation,
                self.protocol_stage,
            )));
        }
        None
    }

    fn map_guard(&mut self, guard: Guard) -> Result<IndexEntry> {
        let entry = guard_to_entry(guard, self.operation, self.protocol_stage);
        #[cfg(test)]
        if entry.is_ok() {
            self.successful_entries += 1;
        }
        entry
    }
}

impl Iterator for FjallEntryIterator {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        #[cfg(test)]
        if let Some(error) = self.take_injected_error() {
            return Some(error);
        }
        self.inner.next().map(|guard| self.map_guard(guard))
    }
}

impl DoubleEndedIterator for FjallEntryIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        #[cfg(test)]
        if let Some(error) = self.take_injected_error() {
            return Some(error);
        }
        self.inner.next_back().map(|guard| self.map_guard(guard))
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum TestCommitFailure {
    #[default]
    None,
    BeforeCommit,
    AfterCommitReturned,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestCompression {
    None,
    Lz4,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestFjallError {
    DirectIo,
    NestedIo,
    StorageCorruption,
    Decompress,
    InvalidTrailer,
    InvalidTag,
    InvalidVersion,
    Poisoned,
    KeyspaceDeleted,
    Locked,
    Unrecoverable,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TestUserKeyspaceConfiguration {
    pub(crate) block_sizes: Vec<u32>,
    pub(crate) block_restart_intervals: Vec<u8>,
    pub(crate) compressions: Vec<TestCompression>,
    pub(crate) compaction_strategy: String,
    pub(crate) table_target_size: Option<u64>,
}

/// Private production adapter. No Fjall type is exposed outside the index module.
pub(crate) struct FjallBackend {
    database: Database,
    user: Keyspace,
    transaction: Keyspace,
    system: Keyspace,
    snapshot_owner: Arc<()>,
    commit_lock: Mutex<()>,
    #[cfg(test)]
    commit_failure: Mutex<TestCommitFailure>,
    #[cfg(test)]
    last_commit_entered: AtomicBool,
    #[cfg(test)]
    iterator_error_after: Mutex<Option<usize>>,
}

/// A Destroy-only read view recovered inside an isolated shadow directory.
///
/// Fjall 3.1.8 has no side-effect-free open API: ordinary recovery may remove
/// orphaned manifests, tables, or keyspaces. The shadow contains byte copies of
/// regular files and omits unmanaged symlinks, so Fjall may recover the shadow
/// while the locked source index remains read-only.
pub(crate) struct FjallReadOnlyVerification {
    // Field order is intentional: Fjall must release every shadow handle before
    // the shadow tree is removed.
    backend: FjallBackend,
    shadow: ReadOnlyShadowDirectory,
}

impl FjallReadOnlyVerification {
    pub(crate) fn backend(&self) -> &FjallBackend {
        &self.backend
    }

    /// Closes Fjall first and reports failure to clean the isolated shadow while
    /// Destroy is still in its non-destructive inventory phase.
    pub(crate) fn close(self) -> Result<()> {
        let Self {
            backend,
            mut shadow,
        } = self;
        drop(backend);
        shadow.remove()
    }
}

struct ReadOnlyShadowDirectory {
    path: Option<PathBuf>,
}

impl ReadOnlyShadowDirectory {
    fn create() -> Result<Self> {
        let temporary_root = std::env::temp_dir();
        let process_id = std::process::id();
        for _ in 0..64 {
            let nonce = NEXT_READ_ONLY_SHADOW_ID.fetch_add(1, Ordering::Relaxed);
            let path = temporary_root.join(format!("rustkv-fjall-read-only-{process_id}-{nonce}"));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path: Some(path) }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(shadow_io_error(error)),
            }
        }
        Err(shadow_io_error(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique Fjall read-only shadow directory",
        )))
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("read-only shadow path exists until removal")
    }

    fn remove(&mut self) -> Result<()> {
        self.remove_with(|path| fs::remove_dir_all(path))
    }

    fn remove_with(&mut self, remove: impl FnOnce(&Path) -> std::io::Result<()>) -> Result<()> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        remove(path).map_err(shadow_io_error)?;
        self.path = None;
        Ok(())
    }
}

impl Drop for ReadOnlyShadowDirectory {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_dir_all(path);
        }
    }
}

#[cfg(test)]
pub(crate) fn shadow_remove_retry_probe_for_test() -> Result<(StorageError, bool, bool)> {
    let mut shadow = ReadOnlyShadowDirectory::create()?;
    let path = shadow.path().to_path_buf();
    fs::write(path.join("sentinel"), b"shadow must be removed by Drop").map_err(shadow_io_error)?;
    let Err(error) = shadow.remove_with(|_| Err(io::Error::from_raw_os_error(5))) else {
        return Err(open_layout_error());
    };
    let retained_after_failure = shadow.path.as_deref() == Some(path.as_path()) && path.is_dir();
    drop(shadow);
    Ok((error, retained_after_failure, path.exists()))
}

impl FjallBackend {
    /// Creates a new Fjall container and all three fixed keyspaces.
    ///
    /// Fjall keyspace creation options are persisted and its keyspace factory is
    /// not called again for an existing keyspace. Refusing an existing container
    /// here prevents a partially created layout from silently mixing old and new
    /// creation-time options.
    pub(crate) fn create(path: &Path, options: FjallIndexOptions) -> Result<Self> {
        let validated = validate_options(options)?;
        validate_create_target(path)?;
        Self::open(path, validated, false, BackgroundWorkerMode::Enabled)
    }

    /// Creates the container for the ordered database-open preparation path.
    ///
    /// Fjall normally starts its flush/compaction pool from `Database::open`.
    /// Stage 7 must finish identity validation and recovery before any background
    /// worker can run, so this entry point deliberately opens Fjall with a
    /// zero-sized worker pool. Later lifecycle assembly must activate or reopen
    /// the backend only after the remaining open steps have succeeded.
    pub(crate) fn create_for_open_preparation(
        path: &Path,
        options: FjallIndexOptions,
    ) -> Result<Self> {
        let validated = validate_options(options)?;
        validate_create_target(path)?;
        Self::open(path, validated, false, BackgroundWorkerMode::Disabled)
    }

    /// Opens an existing Fjall database without creating a directory or keyspace.
    ///
    /// `block_cache_size` and `max_open_files` are Database-level settings and
    /// are applied on every open. The remaining settings are Keyspace creation
    /// options: Fjall restores their persisted values and ignores new factories.
    pub(crate) fn open_existing(path: &Path, options: FjallIndexOptions) -> Result<Self> {
        let validated = validate_options(options)?;
        validate_existing_layout(path)?;
        Self::open(path, validated, true, BackgroundWorkerMode::Enabled)
    }

    /// Opens an existing container without starting Fjall background workers.
    /// This is restricted to the ordered database-open preparation path.
    pub(crate) fn open_existing_for_open_preparation(
        path: &Path,
        options: FjallIndexOptions,
    ) -> Result<Self> {
        let validated = validate_options(options)?;
        validate_existing_layout(path)?;
        Self::open(path, validated, true, BackgroundWorkerMode::Disabled)
    }

    /// Produces a side-effect-free read view for Destroy identity verification.
    ///
    /// The source is validated and copied into an isolated shadow before Fjall
    /// recovery is invoked. Recovery can mutate only the shadow; it never opens
    /// a writable handle to a regular file in the source index.
    pub(crate) fn open_existing_read_only_for_destroy(
        path: &Path,
        options: FjallIndexOptions,
    ) -> Result<FjallReadOnlyVerification> {
        let validated = validate_options(options)?;
        validate_existing_layout_for_destroy(path)?;
        let shadow = copy_read_only_shadow(path)?;
        let backend = Self::open(
            shadow.path(),
            validated,
            true,
            BackgroundWorkerMode::Disabled,
        )?;
        Ok(FjallReadOnlyVerification { backend, shadow })
    }

    fn open(
        path: &Path,
        validated: ValidatedOptions,
        existing_only: bool,
        background_worker_mode: BackgroundWorkerMode,
    ) -> Result<Self> {
        let mut builder = Database::builder(path)
            .cache_size(validated.block_cache_size)
            .max_cached_files(Some(validated.original.max_open_files))
            .manual_journal_persist(true);
        if background_worker_mode == BackgroundWorkerMode::Disabled {
            // Fjall 3.1.8 exposes this hidden builder hook for zero-worker
            // recovery/open flows; the checked API intentionally rejects zero.
            builder = builder.worker_threads_unchecked(0);
        }
        let database = builder.open().map_err(|error| {
            storage_error_from_fjall(error, Operation::Open, ProtocolStage::Preflight)
        })?;

        if existing_only {
            validate_exact_keyspace_names(&database)?;
        } else if database.keyspace_count() != 0 {
            return Err(open_layout_error());
        }

        let (user, transaction, system) = if existing_only {
            (
                open_existing_keyspace(&database, USER_INDEX_NAME)?,
                open_existing_keyspace(&database, TX_METADATA_NAME)?,
                open_existing_keyspace(&database, SYSTEM_METADATA_NAME)?,
            )
        } else {
            (
                create_keyspace(&database, USER_INDEX_NAME, || {
                    user_keyspace_options(validated)
                })?,
                create_keyspace(&database, TX_METADATA_NAME, metadata_keyspace_options)?,
                create_keyspace(&database, SYSTEM_METADATA_NAME, metadata_keyspace_options)?,
            )
        };

        validate_exact_keyspace_names(&database)?;
        if user.is_kv_separated() || transaction.is_kv_separated() || system.is_kv_separated() {
            return Err(open_layout_error());
        }

        Ok(Self {
            database,
            user,
            transaction,
            system,
            snapshot_owner: Arc::new(()),
            commit_lock: Mutex::new(()),
            #[cfg(test)]
            commit_failure: Mutex::new(TestCommitFailure::None),
            #[cfg(test)]
            last_commit_entered: AtomicBool::new(false),
            #[cfg(test)]
            iterator_error_after: Mutex::new(None),
        })
    }

    fn keyspace(&self, space: InternalIndexSpace) -> &Keyspace {
        match space {
            InternalIndexSpace::Transaction => &self.transaction,
            InternalIndexSpace::System => &self.system,
        }
    }

    fn ensure_initialization_keyspaces_are_empty(
        &self,
    ) -> std::result::Result<(), IndexCommitError> {
        for keyspace in [&self.user, &self.transaction, &self.system] {
            let is_empty = keyspace.is_empty().map_err(|error| {
                IndexCommitError::not_applied(internal_error_from_fjall(&error))
            })?;
            if !is_empty {
                return Err(IndexCommitError::not_applied(InternalIndexError::new(
                    StorageErrorKind::InvalidLayout,
                    None,
                )));
            }
        }
        Ok(())
    }

    fn ensure_identity_is_initialized(&self) -> std::result::Result<(), IndexCommitError> {
        match self.system.get(DATABASE_IDENTITY_KEY) {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::InvalidLayout,
                None,
            ))),
            Err(error) => Err(IndexCommitError::not_applied(internal_error_from_fjall(
                &error,
            ))),
        }
    }

    fn validate_snapshot(&self, snapshot: &FjallSnapshot, operation: Operation) -> Result<()> {
        if Arc::ptr_eq(&self.snapshot_owner, &snapshot.owner) {
            Ok(())
        } else {
            Err(sanitized_storage_error(
                StorageErrorKind::InvalidArgument,
                None,
                operation,
                ProtocolStage::Read,
            ))
        }
    }

    #[cfg(test)]
    pub(crate) fn set_commit_failure(&self, failure: TestCommitFailure) {
        *self.commit_failure.lock().expect("test mutex poisoned") = failure;
    }

    #[cfg(test)]
    pub(crate) fn last_commit_entered(&self) -> bool {
        self.last_commit_entered.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn inject_iterator_error_after(&self, successful_entries: Option<usize>) {
        *self
            .iterator_error_after
            .lock()
            .expect("test mutex poisoned") = successful_entries;
    }

    #[cfg(test)]
    pub(crate) fn cache_capacity(&self) -> u64 {
        self.database.cache_capacity()
    }

    #[cfg(test)]
    pub(crate) fn descriptor_cache_size(&self) -> usize {
        ::fjall::AbstractTree::table_file_cache_size(&self.user.tree)
    }

    #[cfg(test)]
    pub(crate) const fn max_table_target_size() -> u64 {
        MAX_FJALL_TABLE_TARGET_SIZE
    }

    #[cfg(test)]
    pub(crate) fn rotate_user_memtable_and_wait(&self) -> Result<()> {
        self.user.rotate_memtable_and_wait().map_err(|error| {
            storage_error_from_fjall(error, Operation::WriteBatch, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn rotate_user_memtable_without_wait(&self) -> Result<bool> {
        self.user.rotate_memtable().map_err(|error| {
            storage_error_from_fjall(error, Operation::WriteBatch, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn outstanding_flushes(&self) -> usize {
        self.database.outstanding_flushes()
    }

    #[cfg(test)]
    pub(crate) fn commit_database_batch_without_durability(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut batch = self.database.batch();
        batch.insert(&self.user, key, value);
        batch.commit().map_err(|error| {
            storage_error_from_fjall(error, Operation::WriteBatch, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn insert_without_keyspace_durability(
        &self,
        space: Option<InternalIndexSpace>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let keyspace = space.map_or(&self.user, |space| self.keyspace(space));
        keyspace.insert(key, value).map_err(|error| {
            storage_error_from_fjall(error, Operation::WriteBatch, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn overwrite_database_identity_for_test(
        &self,
        encoded_identity: Option<&[u8]>,
    ) -> Result<()> {
        let mut batch = OwnedWriteBatch::with_capacity(self.database.clone(), 1)
            .durability(Some(PersistMode::SyncAll));
        if let Some(encoded_identity) = encoded_identity {
            batch.insert(&self.system, DATABASE_IDENTITY_KEY, encoded_identity);
        } else {
            batch.remove(&self.system, DATABASE_IDENTITY_KEY);
        }
        batch.commit().map_err(|error| {
            storage_error_from_fjall(error, Operation::Open, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test_sync_all(
        &self,
        space: Option<InternalIndexSpace>,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let keyspace = space.map_or(&self.user, |space| self.keyspace(space));
        let mut batch = OwnedWriteBatch::with_capacity(self.database.clone(), 1)
            .durability(Some(PersistMode::SyncAll));
        batch.insert(keyspace, key, value);
        batch.commit().map_err(|error| {
            storage_error_from_fjall(error, Operation::Open, ProtocolStage::IndexCommit)
        })
    }

    #[cfg(test)]
    pub(crate) fn classify_error_for_test(error: TestFjallError) -> StorageError {
        let error = match error {
            TestFjallError::DirectIo => ::fjall::Error::Io(io::Error::from_raw_os_error(5)),
            TestFjallError::NestedIo => {
                ::fjall::Error::Storage(::fjall::LsmError::Io(io::Error::from_raw_os_error(5)))
            }
            TestFjallError::StorageCorruption => {
                ::fjall::Error::Storage(::fjall::LsmError::InvalidTrailer)
            }
            TestFjallError::Decompress => ::fjall::Error::Decompress(CompressionType::Lz4),
            TestFjallError::InvalidTrailer => ::fjall::Error::InvalidTrailer,
            TestFjallError::InvalidTag => ::fjall::Error::InvalidTag(("test", u8::MAX)),
            TestFjallError::InvalidVersion => ::fjall::Error::InvalidVersion(None),
            TestFjallError::Poisoned => ::fjall::Error::Poisoned,
            TestFjallError::KeyspaceDeleted => ::fjall::Error::KeyspaceDeleted,
            TestFjallError::Locked => ::fjall::Error::Locked,
            TestFjallError::Unrecoverable => ::fjall::Error::Unrecoverable,
        };
        storage_error_from_fjall(error, Operation::Open, ProtocolStage::Preflight)
    }

    #[cfg(test)]
    pub(crate) fn keyspace_names(&self) -> Vec<String> {
        let mut names = self
            .database
            .list_keyspace_names()
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[cfg(test)]
    pub(crate) fn all_keyspaces_disable_kv_separation(&self) -> bool {
        !self.user.is_kv_separated()
            && !self.transaction.is_kv_separated()
            && !self.system.is_kv_separated()
    }

    #[cfg(test)]
    pub(crate) fn user_keyspace_configuration(&self) -> TestUserKeyspaceConfiguration {
        let config = &self.user.config;
        let compaction_strategy = config.compaction_strategy.get_name().to_owned();
        let table_target_size = config
            .compaction_strategy
            .get_config()
            .into_iter()
            .find_map(|(key, value)| {
                (key.as_ref() == b"leveled_target_size")
                    .then(|| <[u8; 8]>::try_from(value.as_ref()).ok())
                    .flatten()
                    .map(u64::from_le_bytes)
            });

        TestUserKeyspaceConfiguration {
            block_sizes: config.data_block_size_policy.to_vec(),
            block_restart_intervals: config.data_block_restart_interval_policy.to_vec(),
            compressions: config
                .data_block_compression_policy
                .iter()
                .map(|compression| match compression {
                    CompressionType::None => TestCompression::None,
                    CompressionType::Lz4 => TestCompression::Lz4,
                })
                .collect(),
            compaction_strategy,
            table_target_size,
        }
    }

    #[cfg(test)]
    pub(crate) fn user_table_count(&self) -> usize {
        self.user.table_count()
    }

    #[cfg(test)]
    pub(crate) fn get_internal_at_snapshot(
        &self,
        snapshot: &FjallSnapshot,
        space: InternalIndexSpace,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.validate_snapshot(snapshot, Operation::Snapshot)?;
        snapshot
            .inner
            .get(self.keyspace(space), key)
            .map_err(|error| {
                storage_error_from_fjall(error, Operation::Snapshot, ProtocolStage::Read)
            })?
            .map(|value| try_copy_bytes(value.as_ref(), Operation::Snapshot, ProtocolStage::Read))
            .transpose()
    }
}

impl IndexBackend for FjallBackend {
    type Snapshot = FjallSnapshot;
    type UserIterator = FjallEntryIterator;
    type InternalIterator = FjallEntryIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        #[cfg(test)]
        self.last_commit_entered.store(false, Ordering::Release);

        batch.validate_for_commit(mode)?;
        let is_initialization = batch.is_database_initialization();
        let _commit_guard = self.commit_lock.lock().map_err(|_| {
            IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::StoragePoisoned,
                None,
            ))
        })?;
        if is_initialization {
            self.ensure_initialization_keyspaces_are_empty()?;
        } else {
            self.ensure_identity_is_initialized()?;
        }

        #[cfg(test)]
        if *self.commit_failure.lock().expect("test mutex poisoned")
            == TestCommitFailure::BeforeCommit
        {
            return Err(IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::Io,
                None,
            )));
        }

        let operation_count = batch.len();
        let mut fjall_batch =
            OwnedWriteBatch::with_capacity(self.database.clone(), operation_count).durability(
                Some(match mode {
                    IndexCommitMode::Buffer => PersistMode::Buffer,
                    IndexCommitMode::SyncAll => PersistMode::SyncAll,
                }),
            );

        for operation in batch.into_operations() {
            match operation {
                IndexMutation::InitializeDatabaseIdentity { encoded_identity } => {
                    fjall_batch.insert(&self.system, DATABASE_IDENTITY_KEY, encoded_identity);
                }
                IndexMutation::PutUser {
                    user_key,
                    encoded_pointer,
                } => fjall_batch.insert(&self.user, user_key, encoded_pointer),
                IndexMutation::DeleteUser { user_key } => {
                    fjall_batch.remove(&self.user, user_key);
                }
                IndexMutation::PutInternal { space, key, value } => {
                    fjall_batch.insert(self.keyspace(space), key, value);
                }
                IndexMutation::DeleteInternal { space, key } => {
                    fjall_batch.remove(self.keyspace(space), key);
                }
            }
        }

        #[cfg(test)]
        self.last_commit_entered.store(true, Ordering::Release);

        fjall_batch
            .commit()
            .map_err(|error| IndexCommitError::unknown(internal_error_from_fjall(&error)))?;

        #[cfg(test)]
        if *self.commit_failure.lock().expect("test mutex poisoned")
            == TestCommitFailure::AfterCommitReturned
        {
            return Err(IndexCommitError::unknown(InternalIndexError::new(
                StorageErrorKind::Io,
                None,
            )));
        }

        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        self.system
            .get(DATABASE_IDENTITY_KEY)
            .map_err(|error| {
                storage_error_from_fjall(error, Operation::Open, ProtocolStage::Preflight)
            })?
            .map(|value| try_copy_bytes(value.as_ref(), Operation::Open, ProtocolStage::Preflight))
            .transpose()
    }

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        let value = match snapshot {
            Some(snapshot) => {
                self.validate_snapshot(snapshot, Operation::Get)?;
                snapshot.inner.get(&self.user, key)
            }
            None => self.user.get(key),
        }
        .map_err(|error| storage_error_from_fjall(error, Operation::Get, ProtocolStage::Read))?;

        value
            .map(|value| try_copy_bytes(value.as_ref(), Operation::Get, ProtocolStage::Read))
            .transpose()
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.keyspace(space)
            .get(key)
            .map_err(|error| {
                storage_error_from_fjall(error, Operation::Recovery, ProtocolStage::Recovery)
            })?
            .map(|value| {
                try_copy_bytes(value.as_ref(), Operation::Recovery, ProtocolStage::Recovery)
            })
            .transpose()
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        let keyspace = self.keyspace(space);
        let inner = match (range.start_inclusive, range.end_exclusive) {
            (None, None) => keyspace.iter(),
            (Some(start), None) => keyspace.range(start..),
            (None, Some(end)) => keyspace.range(..end),
            (Some(start), Some(end)) => keyspace.range(start..end),
        };
        let iterator = FjallEntryIterator::new(inner, Operation::Recovery, ProtocolStage::Recovery);
        #[cfg(test)]
        let iterator = iterator.inject_error_after(
            *self
                .iterator_error_after
                .lock()
                .expect("test mutex poisoned"),
        );
        Ok(iterator)
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(FjallSnapshot {
            inner: self.database.snapshot(),
            owner: Arc::clone(&self.snapshot_owner),
        })
    }

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        let inner = match snapshot {
            Some(snapshot) => {
                self.validate_snapshot(snapshot, Operation::Iterator)?;
                snapshot.inner.iter(&self.user)
            }
            None => self.user.iter(),
        };
        Ok(FjallEntryIterator::new(
            inner,
            Operation::Iterator,
            ProtocolStage::Read,
        ))
    }
}

fn validate_options(options: FjallIndexOptions) -> Result<ValidatedOptions> {
    let max_file_size =
        u64::try_from(options.max_file_size).map_err(|_| open_invalid_argument())?;
    if options.write_buffer_size == 0
        || options.max_open_files < MIN_FJALL_OPEN_FILES
        || !(MIN_FJALL_BLOCK_SIZE..=MAX_FJALL_BLOCK_SIZE).contains(&options.block_size)
        || options.block_restart_interval == 0
        || options.block_restart_interval > usize::from(u8::MAX)
        || max_file_size == 0
        || max_file_size > MAX_FJALL_TABLE_TARGET_SIZE
    {
        return Err(open_invalid_argument());
    }

    Ok(ValidatedOptions {
        original: options,
        write_buffer_size: u64::try_from(options.write_buffer_size)
            .map_err(|_| open_invalid_argument())?,
        block_cache_size: u64::try_from(options.block_cache_size)
            .map_err(|_| open_invalid_argument())?,
        block_size: u32::try_from(options.block_size).map_err(|_| open_invalid_argument())?,
        block_restart_interval: u8::try_from(options.block_restart_interval)
            .map_err(|_| open_invalid_argument())?,
        max_file_size,
        compression: match options.compression {
            IndexCompression::None => CompressionType::None,
            IndexCompression::Lz4 => CompressionType::Lz4,
        },
    })
}

fn user_keyspace_options(options: ValidatedOptions) -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .max_memtable_size(options.write_buffer_size)
        .data_block_size_policy(BlockSizePolicy::all(options.block_size))
        .data_block_restart_interval_policy(RestartIntervalPolicy::all(
            options.block_restart_interval,
        ))
        .data_block_compression_policy(CompressionPolicy::all(options.compression))
        .compaction_strategy(Arc::new(
            Leveled::default().with_table_target_size(options.max_file_size),
        ))
        .manual_journal_persist(true)
        .with_kv_separation(None)
}

fn metadata_keyspace_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default()
        .manual_journal_persist(true)
        .with_kv_separation(None)
}

/// Fjall 3.1.8 has a create-or-recover `open()` API, not an open-only API.
/// Its recovery path creates a journal when none exists and removes Keyspace
/// directories whose LSM `current` marker is absent. Validate the locked 3.1.8
/// layout before handing the path to Fjall so damaged state fails closed.
fn validate_existing_layout(path: &Path) -> Result<()> {
    validate_existing_layout_inner(path, false)
}

fn validate_existing_layout_for_destroy(path: &Path) -> Result<()> {
    validate_existing_layout_inner(path, true)
}

fn validate_existing_layout_inner(path: &Path, allow_unmanaged_symlinks: bool) -> Result<()> {
    require_directory(path, StorageErrorKind::InvalidLayout)?;
    require_regular_file(
        &path.join(FJALL_VERSION_MARKER),
        StorageErrorKind::InvalidLayout,
    )?;
    require_regular_file(&path.join(FJALL_LOCK_FILE), StorageErrorKind::InvalidLayout)?;

    let mut has_journal = false;
    let entries =
        std::fs::read_dir(path).map_err(|error| storage_error_from_io(error, Operation::Open))?;
    for entry in entries {
        let entry = entry.map_err(|error| storage_error_from_io(error, Operation::Open))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(open_layout_error());
        };
        let file_type = entry
            .file_type()
            .map_err(|error| storage_error_from_io(error, Operation::Open))?;
        if allow_unmanaged_symlinks && file_type.is_symlink() {
            continue;
        }
        if matches!(
            name,
            FJALL_VERSION_MARKER | FJALL_LOCK_FILE | FJALL_KEYSPACES_DIRECTORY
        ) {
            continue;
        }

        let Some(journal_id) = name
            .strip_suffix(".jnl")
            .and_then(|id| id.parse::<u64>().ok())
        else {
            return Err(open_layout_error());
        };
        if name != format!("{journal_id}.jnl") {
            return Err(open_layout_error());
        }
        if !file_type.is_file() {
            return Err(open_layout_error());
        }
        has_journal = true;
    }
    if !has_journal {
        return Err(open_corruption_error());
    }

    validate_existing_keyspace_trees(
        &path.join(FJALL_KEYSPACES_DIRECTORY),
        allow_unmanaged_symlinks,
    )
}

fn validate_existing_keyspace_trees(path: &Path, allow_unmanaged_symlinks: bool) -> Result<()> {
    require_directory(path, StorageErrorKind::InvalidLayout)?;
    let mut found = [false; 4];
    let entries =
        std::fs::read_dir(path).map_err(|error| storage_error_from_io(error, Operation::Open))?;
    for entry in entries {
        let entry = entry.map_err(|error| storage_error_from_io(error, Operation::Open))?;
        let file_type = entry
            .file_type()
            .map_err(|error| storage_error_from_io(error, Operation::Open))?;
        if allow_unmanaged_symlinks && file_type.is_symlink() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(open_layout_error());
        };
        let index = match name.as_str() {
            "0" => 0,
            "1" => 1,
            "2" => 2,
            "3" => 3,
            _ => return Err(open_layout_error()),
        };
        if found[index] || !file_type.is_dir() {
            return Err(open_layout_error());
        }
        found[index] = true;
        validate_existing_lsm_tree(&entry.path(), allow_unmanaged_symlinks)?;
    }

    if found.into_iter().all(|present| present) {
        Ok(())
    } else {
        Err(open_layout_error())
    }
}

fn validate_existing_lsm_tree(path: &Path, allow_unmanaged_symlinks: bool) -> Result<()> {
    let current_path = path.join(FJALL_LSM_CURRENT_MARKER);
    require_regular_file(&current_path, StorageErrorKind::Corruption)?;
    let current = std::fs::read(&current_path)
        .map_err(|error| storage_error_from_io(error, Operation::Open))?;
    if current.len() != FJALL_LSM_CURRENT_LEN || current[FJALL_LSM_CURRENT_LEN - 1] != 0 {
        return Err(open_corruption_error());
    }
    let mut version_bytes = [0_u8; 8];
    version_bytes.copy_from_slice(&current[..8]);
    let version = u64::from_le_bytes(version_bytes);
    require_regular_file(
        &path.join(format!("v{version}")),
        StorageErrorKind::Corruption,
    )?;

    let tables_path = path.join(FJALL_LSM_TABLES_DIRECTORY);
    require_directory(&tables_path, StorageErrorKind::Corruption)?;
    validate_lsm_table_files(&tables_path, allow_unmanaged_symlinks)?;

    let entries =
        std::fs::read_dir(path).map_err(|error| storage_error_from_io(error, Operation::Open))?;
    for entry in entries {
        let entry = entry.map_err(|error| storage_error_from_io(error, Operation::Open))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(open_layout_error());
        };
        let file_type = entry
            .file_type()
            .map_err(|error| storage_error_from_io(error, Operation::Open))?;
        if allow_unmanaged_symlinks && file_type.is_symlink() {
            continue;
        }
        match name.as_str() {
            FJALL_LSM_CURRENT_MARKER if file_type.is_file() => {}
            FJALL_LSM_TABLES_DIRECTORY if file_type.is_dir() => {}
            _ if name
                .strip_prefix('v')
                .is_some_and(|id| !id.is_empty() && id.parse::<u64>().is_ok())
                && file_type.is_file() => {}
            // Fjall ignores non-manifest regular scratch files, including a
            // temporary `current` rewrite left by a terminated process.
            _ if file_type.is_file() && !name.starts_with('v') => {}
            _ => return Err(open_layout_error()),
        }
    }
    Ok(())
}

fn validate_lsm_table_files(path: &Path, allow_unmanaged_symlinks: bool) -> Result<()> {
    let entries =
        std::fs::read_dir(path).map_err(|error| storage_error_from_io(error, Operation::Open))?;
    for entry in entries {
        let entry = entry.map_err(|error| storage_error_from_io(error, Operation::Open))?;
        let file_type = entry
            .file_type()
            .map_err(|error| storage_error_from_io(error, Operation::Open))?;
        if allow_unmanaged_symlinks && file_type.is_symlink() {
            continue;
        }
        if !file_type.is_file() {
            return Err(open_layout_error());
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(open_layout_error());
        };
        if name != ".DS_Store" && !name.starts_with("._") && name.parse::<u64>().is_err() {
            return Err(open_layout_error());
        }
    }
    Ok(())
}

fn require_regular_file(path: &Path, missing_kind: StorageErrorKind) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(open_layout_error()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(open_existing_error(missing_kind))
        }
        Err(error) => Err(storage_error_from_io(error, Operation::Open)),
    }
}

fn require_directory(path: &Path, missing_kind: StorageErrorKind) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        Ok(_) => Err(open_layout_error()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(open_existing_error(missing_kind))
        }
        Err(error) => Err(storage_error_from_io(error, Operation::Open)),
    }
}

fn copy_read_only_shadow(source: &Path) -> Result<ReadOnlyShadowDirectory> {
    let shadow = ReadOnlyShadowDirectory::create()?;
    copy_directory_contents_nofollow(source, shadow.path())?;
    Ok(shadow)
}

fn copy_directory_contents_nofollow(source: &Path, destination: &Path) -> Result<()> {
    let entries = fs::read_dir(source).map_err(shadow_io_error)?;
    for entry in entries {
        let entry = entry.map_err(shadow_io_error)?;
        let file_type = entry.file_type().map_err(shadow_io_error)?;
        let destination_entry = destination.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir(&destination_entry).map_err(shadow_io_error)?;
            copy_directory_contents_nofollow(&entry.path(), &destination_entry)?;
        } else if file_type.is_file() {
            fs::copy(entry.path(), destination_entry).map_err(shadow_io_error)?;
        } else if file_type.is_symlink() {
            // Destroy inventories symlinks separately and unlinks them without
            // traversal. They must not become inputs to Fjall shadow recovery.
        } else {
            return Err(open_layout_error());
        }
    }
    Ok(())
}

fn shadow_io_error(error: io::Error) -> StorageError {
    storage_error_from_io(error, Operation::Open)
}

fn validate_create_target(path: &Path) -> Result<()> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => match entries.next() {
            None => Ok(()),
            Some(Ok(_)) => Err(open_layout_error()),
            Some(Err(error)) => Err(storage_error_from_io(error, Operation::Open)),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => Err(open_layout_error()),
        Err(error) => Err(storage_error_from_io(error, Operation::Open)),
    }
}

fn create_keyspace(
    database: &Database,
    name: &str,
    create_options: impl FnOnce() -> KeyspaceCreateOptions,
) -> Result<Keyspace> {
    database
        .keyspace(name, create_options)
        .map_err(|error| storage_error_from_fjall(error, Operation::Open, ProtocolStage::Preflight))
}

fn open_existing_keyspace(database: &Database, name: &str) -> Result<Keyspace> {
    if !database.keyspace_exists(name) {
        return Err(open_layout_error());
    }

    // Fjall 3.1.8 has no get-only Keyspace API. The exact topology check above
    // and the adapter's private Database handle make this factory unreachable;
    // importantly, it does not capture or pretend to apply the current Options.
    create_keyspace(database, name, metadata_keyspace_options)
}

fn validate_exact_keyspace_names(database: &Database) -> Result<()> {
    if database.keyspace_count() == 3
        && database.keyspace_exists(USER_INDEX_NAME)
        && database.keyspace_exists(TX_METADATA_NAME)
        && database.keyspace_exists(SYSTEM_METADATA_NAME)
    {
        Ok(())
    } else {
        Err(open_layout_error())
    }
}

fn guard_to_entry(
    guard: Guard,
    operation: Operation,
    protocol_stage: ProtocolStage,
) -> Result<IndexEntry> {
    let (key, value) = guard
        .into_inner()
        .map_err(|error| storage_error_from_fjall(error, operation, protocol_stage))?;
    Ok(IndexEntry::new(
        try_copy_bytes(key.as_ref(), operation, protocol_stage)?,
        try_copy_bytes(value.as_ref(), operation, protocol_stage)?,
    ))
}

fn try_copy_bytes(
    bytes: &[u8],
    operation: Operation,
    protocol_stage: ProtocolStage,
) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(bytes.len()).map_err(|_| {
        sanitized_storage_error(
            StorageErrorKind::ResourceExhausted,
            None,
            operation,
            protocol_stage,
        )
    })?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn internal_error_from_fjall(error: &::fjall::Error) -> InternalIndexError {
    InternalIndexError::new(
        classify_fjall_error(error),
        find_io_error(error).and_then(io::Error::raw_os_error),
    )
}

fn storage_error_from_fjall(
    error: ::fjall::Error,
    operation: Operation,
    protocol_stage: ProtocolStage,
) -> StorageError {
    let kind = classify_fjall_error(&error);
    let os_code = find_io_error(&error).and_then(io::Error::raw_os_error);
    sanitized_storage_error(kind, os_code, operation, protocol_stage)
}

fn classify_fjall_error(error: &::fjall::Error) -> StorageErrorKind {
    match error {
        ::fjall::Error::Io(_) => StorageErrorKind::Io,
        ::fjall::Error::Storage(_) | ::fjall::Error::JournalRecovery(_)
            if find_io_error(error).is_some() =>
        {
            StorageErrorKind::Io
        }
        ::fjall::Error::Storage(_)
        | ::fjall::Error::JournalRecovery(_)
        | ::fjall::Error::Decompress(_)
        | ::fjall::Error::InvalidTrailer
        | ::fjall::Error::InvalidTag(_) => StorageErrorKind::Corruption,
        ::fjall::Error::InvalidVersion(_) => StorageErrorKind::IncompatibleFormat,
        ::fjall::Error::Poisoned => StorageErrorKind::StoragePoisoned,
        ::fjall::Error::KeyspaceDeleted => StorageErrorKind::InvalidLayout,
        ::fjall::Error::Locked => StorageErrorKind::Busy,
        ::fjall::Error::Unrecoverable => StorageErrorKind::Unrecoverable,
        _ => StorageErrorKind::Unrecoverable,
    }
}

fn find_io_error<'a>(error: &'a (dyn StdError + 'static)) -> Option<&'a io::Error> {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(io_error) = source.downcast_ref::<io::Error>() {
            return Some(io_error);
        }
        current = source.source();
    }
    None
}

fn storage_error_from_io(error: io::Error, operation: Operation) -> StorageError {
    sanitized_storage_error(
        StorageErrorKind::Io,
        error.raw_os_error(),
        operation,
        ProtocolStage::Preflight,
    )
}

fn open_invalid_argument() -> StorageError {
    sanitized_storage_error(
        StorageErrorKind::InvalidArgument,
        None,
        Operation::Open,
        ProtocolStage::Preflight,
    )
}

fn open_layout_error() -> StorageError {
    open_existing_error(StorageErrorKind::InvalidLayout)
}

fn open_corruption_error() -> StorageError {
    open_existing_error(StorageErrorKind::Corruption)
}

fn open_existing_error(kind: StorageErrorKind) -> StorageError {
    sanitized_storage_error(kind, None, Operation::Open, ProtocolStage::Preflight)
}

fn sanitized_storage_error(
    kind: StorageErrorKind,
    os_code: Option<i32>,
    operation: Operation,
    protocol_stage: ProtocolStage,
) -> StorageError {
    let retry_advice = match kind {
        StorageErrorKind::InvalidArgument => RetryAdvice::FixRequestAndRetrySameInstance,
        StorageErrorKind::Busy => RetryAdvice::RetrySameInstance,
        StorageErrorKind::Io => RetryAdvice::FixEnvironmentAndReopen,
        StorageErrorKind::StoragePoisoned => RetryAdvice::ReopenAndVerify,
        StorageErrorKind::Corruption
        | StorageErrorKind::InvalidLayout
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::Unrecoverable => RetryAdvice::RestoreOrRepair,
        _ => RetryAdvice::DoNotRetry,
    };
    let write_outcome = matches!(operation, Operation::WriteBatch | Operation::Sync)
        .then_some(WriteOutcome::NotCommitted);
    let mut error =
        StorageError::codec_error(kind, operation, protocol_stage, write_outcome, retry_advice);
    error.os_code = os_code;
    error
}
