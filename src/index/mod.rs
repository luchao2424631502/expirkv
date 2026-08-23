//! Private index-backend capability boundary.
#![allow(dead_code)] // Stage 4 boundary; the Fjall adapter is connected in stage 5.

use crc32c::crc32c;

use crate::{Result, StorageErrorKind};

const DATABASE_IDENTITY_KEY: &[u8] = b"database_identity";
const HEAD_SEQ_KEY: &[u8] = b"head_seq";
const DURABLE_FRONTIER_KEY: &[u8] = b"durable_frontier";
const DATABASE_IDENTITY_MAGIC: &[u8; 4] = b"RKDI";
const DURABLE_FRONTIER_MAGIC: &[u8; 4] = b"RKDF";
const DATABASE_IDENTITY_ENCODED_LEN: usize = 32;
const HEAD_SEQ_ENCODED_LEN: usize = 8;
const DURABLE_FRONTIER_ENCODED_LEN: usize = 31;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InternalIndexSpace {
    Transaction,
    System,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexMutation {
    InitializeDatabaseIdentity {
        encoded_identity: Vec<u8>,
    },
    PutUser {
        user_key: Vec<u8>,
        encoded_pointer: Vec<u8>,
    },
    DeleteUser {
        user_key: Vec<u8>,
    },
    PutInternal {
        space: InternalIndexSpace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    DeleteInternal {
        space: InternalIndexSpace,
        key: Vec<u8>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexCommitMode {
    Buffer,
    SyncAll,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexApplyState {
    NotApplied,
    Unknown,
}

/// Sanitized index-layer failure. It deliberately carries no backend error text,
/// user key, user value, or concrete backend source type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InternalIndexError {
    pub(crate) kind: StorageErrorKind,
    pub(crate) os_code: Option<i32>,
}

impl InternalIndexError {
    pub(crate) const fn new(kind: StorageErrorKind, os_code: Option<i32>) -> Self {
        Self { kind, os_code }
    }

    fn invalid_batch() -> Self {
        Self::new(StorageErrorKind::InvalidArgument, None)
    }

    fn resource_exhausted() -> Self {
        Self::new(StorageErrorKind::ResourceExhausted, None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexCommitError {
    pub(crate) apply_state: IndexApplyState,
    pub(crate) source: InternalIndexError,
}

impl IndexCommitError {
    pub(crate) const fn not_applied(source: InternalIndexError) -> Self {
        Self {
            apply_state: IndexApplyState::NotApplied,
            source,
        }
    }

    pub(crate) const fn unknown(source: InternalIndexError) -> Self {
        Self {
            apply_state: IndexApplyState::Unknown,
            source,
        }
    }
}

type InternalIndexResult<T> = std::result::Result<T, InternalIndexError>;

/// Ordered mutations for one indivisible backend commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexAtomicBatch {
    operations: Vec<IndexMutation>,
}

impl IndexAtomicBatch {
    pub(crate) fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Constructs the only legal batch containing `InitializeDatabaseIdentity`.
    ///
    /// The caller supplies values encoded by the metadata codec. This constructor
    /// verifies that they describe the current database identity format, head
    /// sequence zero, and the canonical empty durable frontier. The concrete
    /// backend must additionally prove that all three keyspaces are empty before
    /// invoking its underlying atomic commit.
    pub(crate) fn initialize_database(
        encoded_identity: Vec<u8>,
        encoded_head_seq_zero: Vec<u8>,
        encoded_empty_durable_frontier: Vec<u8>,
    ) -> InternalIndexResult<Self> {
        if !is_valid_initial_database_identity(&encoded_identity)
            || !is_encoded_head_seq_zero(&encoded_head_seq_zero)
            || !is_encoded_empty_durable_frontier(&encoded_empty_durable_frontier)
        {
            return Err(InternalIndexError::invalid_batch());
        }

        let mut operations = Vec::new();
        operations
            .try_reserve_exact(3)
            .map_err(|_| InternalIndexError::resource_exhausted())?;
        let head_seq_key = try_copy_static_key(HEAD_SEQ_KEY)?;
        let durable_frontier_key = try_copy_static_key(DURABLE_FRONTIER_KEY)?;
        operations.push(IndexMutation::InitializeDatabaseIdentity { encoded_identity });
        operations.push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: head_seq_key,
            value: encoded_head_seq_zero,
        });
        operations.push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: durable_frontier_key,
            value: encoded_empty_durable_frontier,
        });
        Ok(Self { operations })
    }

    pub(crate) fn try_push(&mut self, mutation: IndexMutation) -> InternalIndexResult<()> {
        validate_ordinary_mutation(&mutation)?;
        self.operations
            .try_reserve(1)
            .map_err(|_| InternalIndexError::resource_exhausted())?;
        self.operations.push(mutation);
        Ok(())
    }

    pub(crate) fn operations(&self) -> &[IndexMutation] {
        &self.operations
    }

    pub(crate) fn len(&self) -> usize {
        self.operations.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub(crate) fn is_database_initialization(&self) -> bool {
        is_initialization_triple(&self.operations)
    }

    /// Mandatory preflight for every backend implementation. It runs before the
    /// adapter marks the underlying backend commit as entered.
    pub(crate) fn validate_for_commit(
        &self,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        if self.operations.is_empty() {
            return Err(invalid_commit_batch());
        }

        let initialization_count = self
            .operations
            .iter()
            .filter(|mutation| matches!(mutation, IndexMutation::InitializeDatabaseIdentity { .. }))
            .count();
        if initialization_count == 0 {
            for mutation in &self.operations {
                validate_ordinary_mutation(mutation)
                    .map_err(|source| IndexCommitError::not_applied(source))?;
            }
            return Ok(());
        }

        if initialization_count != 1
            || mode != IndexCommitMode::SyncAll
            || !is_initialization_triple(&self.operations)
        {
            return Err(invalid_commit_batch());
        }
        Ok(())
    }
}

impl Default for IndexAtomicBatch {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_ordinary_mutation(mutation: &IndexMutation) -> InternalIndexResult<()> {
    match mutation {
        IndexMutation::InitializeDatabaseIdentity { .. } => {
            return Err(InternalIndexError::invalid_batch());
        }
        IndexMutation::PutUser {
            user_key,
            encoded_pointer,
        } => {
            if user_key.is_empty() || encoded_pointer.is_empty() {
                return Err(InternalIndexError::invalid_batch());
            }
        }
        IndexMutation::DeleteUser { user_key } => {
            if user_key.is_empty() {
                return Err(InternalIndexError::invalid_batch());
            }
        }
        IndexMutation::PutInternal { space, key, value } => {
            if key.is_empty()
                || value.is_empty()
                || is_database_identity_key(*space, key.as_slice())
            {
                return Err(InternalIndexError::invalid_batch());
            }
        }
        IndexMutation::DeleteInternal { space, key } => {
            if key.is_empty() || is_database_identity_key(*space, key.as_slice()) {
                return Err(InternalIndexError::invalid_batch());
            }
        }
    }
    Ok(())
}

fn is_database_identity_key(space: InternalIndexSpace, key: &[u8]) -> bool {
    space == InternalIndexSpace::System && key == DATABASE_IDENTITY_KEY
}

fn try_copy_static_key(key: &[u8]) -> InternalIndexResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(key.len())
        .map_err(|_| InternalIndexError::resource_exhausted())?;
    owned.extend_from_slice(key);
    Ok(owned)
}

fn is_initialization_triple(operations: &[IndexMutation]) -> bool {
    match operations {
        [
            IndexMutation::InitializeDatabaseIdentity { encoded_identity },
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: head_key,
                value: encoded_head_seq_zero,
            },
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: frontier_key,
                value: encoded_empty_frontier,
            },
        ] => {
            is_valid_initial_database_identity(encoded_identity)
                && head_key.as_slice() == HEAD_SEQ_KEY
                && is_encoded_head_seq_zero(encoded_head_seq_zero)
                && frontier_key.as_slice() == DURABLE_FRONTIER_KEY
                && is_encoded_empty_durable_frontier(encoded_empty_frontier)
        }
        _ => false,
    }
}

fn is_valid_initial_database_identity(encoded: &[u8]) -> bool {
    encoded.len() == DATABASE_IDENTITY_ENCODED_LEN
        && encoded.get(0..4) == Some(DATABASE_IDENTITY_MAGIC.as_slice())
        && encoded.get(4..10) == Some([0_u8; 6].as_slice())
        && encoded
            .get(10..26)
            .is_some_and(|database_uuid| database_uuid != [0_u8; 16])
        && encoded.get(26..28) == Some([0_u8; 2].as_slice())
        && has_valid_trailing_crc(encoded, 28)
}

fn is_encoded_head_seq_zero(encoded: &[u8]) -> bool {
    encoded.len() == HEAD_SEQ_ENCODED_LEN && encoded == [0_u8; HEAD_SEQ_ENCODED_LEN]
}

fn is_encoded_empty_durable_frontier(encoded: &[u8]) -> bool {
    encoded.len() == DURABLE_FRONTIER_ENCODED_LEN
        && encoded.get(0..4) == Some(DURABLE_FRONTIER_MAGIC.as_slice())
        && encoded.get(4..27) == Some([0_u8; 23].as_slice())
        && has_valid_trailing_crc(encoded, 27)
}

fn has_valid_trailing_crc(encoded: &[u8], crc_offset: usize) -> bool {
    let Some(covered) = encoded.get(..crc_offset) else {
        return false;
    };
    let Some(encoded_crc) = encoded.get(crc_offset..crc_offset.saturating_add(4)) else {
        return false;
    };
    let Ok(encoded_crc) = <[u8; 4]>::try_from(encoded_crc) else {
        return false;
    };
    crc32c(covered) == u32::from_le_bytes(encoded_crc)
}

fn invalid_commit_batch() -> IndexCommitError {
    IndexCommitError::not_applied(InternalIndexError::invalid_batch())
}

/// Owned key/value pair returned by both user and internal iterators.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl IndexEntry {
    pub(crate) fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

/// Owned half-open range for ordered internal metadata scans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InternalKeyRange {
    pub(crate) start_inclusive: Option<Vec<u8>>,
    pub(crate) end_exclusive: Option<Vec<u8>>,
}

impl InternalKeyRange {
    pub(crate) fn all() -> Self {
        Self {
            start_inclusive: None,
            end_exclusive: None,
        }
    }

    pub(crate) fn new(
        start_inclusive: Option<Vec<u8>>,
        end_exclusive: Option<Vec<u8>>,
    ) -> InternalIndexResult<Self> {
        if matches!(
            (&start_inclusive, &end_exclusive),
            (Some(start), Some(end)) if start > end
        ) {
            return Err(InternalIndexError::invalid_batch());
        }
        Ok(Self {
            start_inclusive,
            end_exclusive,
        })
    }
}

impl Default for InternalKeyRange {
    fn default() -> Self {
        Self::all()
    }
}

/// Private, backend-independent capability boundary shared by commit, recovery,
/// and reads. Associated iterator items are owned so no backend guard or Fjall
/// type can escape through this interface.
pub(crate) trait IndexBackend: Send + Sync {
    type Snapshot: Clone + Send + Sync;
    type UserIterator: DoubleEndedIterator<Item = Result<IndexEntry>> + Send;
    type InternalIterator: Iterator<Item = Result<IndexEntry>> + Send;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError>;

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>>;

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>>;

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator>;

    fn snapshot(&self) -> Result<Self::Snapshot>;

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator>;
}

mod fjall;
