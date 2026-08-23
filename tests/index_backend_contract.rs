#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
pub(crate) mod vlog_format;

mod vlog {
    pub(crate) use crate::vlog_format as format;
}

#[path = "../src/commit/descriptor.rs"]
mod descriptor;

#[path = "../src/index/mod.rs"]
mod index;

use index::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeSnapshot {
    user: BTreeMap<Vec<u8>, Vec<u8>>,
}

struct FakeUserIterator {
    entries: std::vec::IntoIter<Result<IndexEntry>>,
}

impl Iterator for FakeUserIterator {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
}

impl DoubleEndedIterator for FakeUserIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.entries.next_back()
    }
}

struct FakeInternalIterator {
    state: Arc<Mutex<FakeState>>,
    space: InternalIndexSpace,
    range: InternalKeyRange,
    last_key: Option<Vec<u8>>,
    successful_entries: usize,
    error_after: Option<usize>,
    error_emitted: bool,
}

impl Iterator for FakeInternalIterator {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.error_emitted && self.error_after == Some(self.successful_entries) {
            self.error_emitted = true;
            return Some(Err(injected_read_error()));
        }

        let state = self.state.lock().unwrap();
        let entry = internal_map(&state, self.space)
            .iter()
            .find(|(key, _)| {
                key_is_in_range(key.as_slice(), &self.range)
                    && self
                        .last_key
                        .as_deref()
                        .is_none_or(|last_key| key.as_slice() > last_key)
            })
            .map(|(key, value)| IndexEntry::new(key.clone(), value.clone()));
        drop(state);

        let entry = entry?;
        self.last_key = Some(entry.key.clone());
        self.successful_entries += 1;
        Some(Ok(entry))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectCommitFailure {
    None,
    BeforeCommit,
    AfterCommitEntered,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FakeCommitCall {
    batch: IndexAtomicBatch,
    mode: IndexCommitMode,
    entered_commit: bool,
}

struct FakeState {
    database_identity: Option<Vec<u8>>,
    user: BTreeMap<Vec<u8>, Vec<u8>>,
    transaction: BTreeMap<Vec<u8>, Vec<u8>>,
    system: BTreeMap<Vec<u8>, Vec<u8>>,
    calls: Vec<FakeCommitCall>,
    committed: Vec<(IndexAtomicBatch, IndexCommitMode)>,
    failure: InjectCommitFailure,
    scan_error_after: Option<usize>,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            database_identity: None,
            user: BTreeMap::new(),
            transaction: BTreeMap::new(),
            system: BTreeMap::new(),
            calls: Vec::new(),
            committed: Vec::new(),
            failure: InjectCommitFailure::None,
            scan_error_after: None,
        }
    }
}

#[derive(Default)]
struct FakeBackend {
    state: Arc<Mutex<FakeState>>,
}

impl FakeBackend {
    fn set_failure(&self, failure: InjectCommitFailure) {
        self.state.lock().unwrap().failure = failure;
    }

    fn last_call_entered_commit(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .calls
            .last()
            .is_some_and(|call| call.entered_commit)
    }

    fn committed(&self) -> Vec<(IndexAtomicBatch, IndexCommitMode)> {
        self.state.lock().unwrap().committed.clone()
    }

    fn calls(&self) -> Vec<FakeCommitCall> {
        self.state.lock().unwrap().calls.clone()
    }

    fn seed_user(&self, key: &[u8], value: &[u8]) {
        self.state
            .lock()
            .unwrap()
            .user
            .insert(key.to_vec(), value.to_vec());
    }

    fn seed_identity(&self, value: &[u8]) {
        self.state.lock().unwrap().database_identity = Some(value.to_vec());
    }

    fn seed_internal(&self, space: InternalIndexSpace, key: &[u8], value: &[u8]) {
        let mut state = self.state.lock().unwrap();
        internal_map_mut(&mut state, space).insert(key.to_vec(), value.to_vec());
    }

    fn inject_scan_error_after(&self, successful_entries: Option<usize>) {
        self.state.lock().unwrap().scan_error_after = successful_entries;
    }
}

impl IndexBackend for FakeBackend {
    type Snapshot = FakeSnapshot;
    type UserIterator = FakeUserIterator;
    type InternalIterator = FakeInternalIterator;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        let call_index = {
            let mut state = self.state.lock().unwrap();
            let call_index = state.calls.len();
            state.calls.push(FakeCommitCall {
                batch: batch.clone(),
                mode,
                entered_commit: false,
            });
            call_index
        };
        batch.validate_for_commit(mode)?;

        let mut state = self.state.lock().unwrap();
        if batch.is_database_initialization()
            && (state.database_identity.is_some()
                || !state.user.is_empty()
                || !state.transaction.is_empty()
                || !state.system.is_empty())
        {
            return Err(IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::InvalidLayout,
                None,
            )));
        }
        if state.failure == InjectCommitFailure::BeforeCommit {
            return Err(IndexCommitError::not_applied(InternalIndexError::new(
                StorageErrorKind::Io,
                Some(5),
            )));
        }

        state.calls[call_index].entered_commit = true;
        if state.failure == InjectCommitFailure::AfterCommitEntered {
            return Err(IndexCommitError::unknown(InternalIndexError::new(
                StorageErrorKind::Io,
                Some(5),
            )));
        }

        apply_batch(&mut state, &batch);
        state.committed.push((batch, mode));
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(self.state.lock().unwrap().database_identity.clone())
    }

    fn get_user(&self, key: &[u8], snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        Ok(match snapshot {
            Some(snapshot) => snapshot.user.get(key).cloned(),
            None => self.state.lock().unwrap().user.get(key).cloned(),
        })
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = self.state.lock().unwrap();
        Ok(internal_map(&state, space).get(key).cloned())
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        let error_after = self.state.lock().unwrap().scan_error_after;
        Ok(FakeInternalIterator {
            state: Arc::clone(&self.state),
            space,
            range,
            last_key: None,
            successful_entries: 0,
            error_after,
            error_emitted: false,
        })
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(FakeSnapshot {
            user: self.state.lock().unwrap().user.clone(),
        })
    }

    fn iter_user(&self, snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        let entries = match snapshot {
            Some(snapshot) => owned_entries(&snapshot.user),
            None => owned_entries(&self.state.lock().unwrap().user),
        };
        Ok(FakeUserIterator {
            entries: entries.into_iter(),
        })
    }
}

fn apply_batch(state: &mut FakeState, batch: &IndexAtomicBatch) {
    for operation in batch.operations() {
        match operation {
            IndexMutation::InitializeDatabaseIdentity { encoded_identity } => {
                state.database_identity = Some(encoded_identity.clone());
            }
            IndexMutation::PutUser {
                user_key,
                encoded_pointer,
            } => {
                state.user.insert(user_key.clone(), encoded_pointer.clone());
            }
            IndexMutation::DeleteUser { user_key } => {
                state.user.remove(user_key);
            }
            IndexMutation::PutInternal { space, key, value } => {
                internal_map_mut(state, *space).insert(key.clone(), value.clone());
            }
            IndexMutation::DeleteInternal { space, key } => {
                internal_map_mut(state, *space).remove(key);
            }
        }
    }
}

fn internal_map(state: &FakeState, space: InternalIndexSpace) -> &BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &state.transaction,
        InternalIndexSpace::System => &state.system,
    }
}

fn internal_map_mut(
    state: &mut FakeState,
    space: InternalIndexSpace,
) -> &mut BTreeMap<Vec<u8>, Vec<u8>> {
    match space {
        InternalIndexSpace::Transaction => &mut state.transaction,
        InternalIndexSpace::System => &mut state.system,
    }
}

fn owned_entries(map: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<Result<IndexEntry>> {
    map.iter()
        .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
        .collect()
}

fn key_is_in_range(key: &[u8], range: &InternalKeyRange) -> bool {
    range
        .start_inclusive
        .as_deref()
        .is_none_or(|start| key >= start)
        && range.end_exclusive.as_deref().is_none_or(|end| key < end)
}

fn injected_read_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Recovery,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::ReopenAndVerify,
    )
}

fn ordinary_three_space_batch() -> IndexAtomicBatch {
    let mut batch = IndexAtomicBatch::new();
    batch
        .try_push(IndexMutation::PutUser {
            user_key: b"user-key".to_vec(),
            encoded_pointer: encoded_user_pointer(),
        })
        .unwrap();
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key: b"txn-key".to_vec(),
            value: b"txn-metadata".to_vec(),
        })
        .unwrap();
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: b"head_seq".to_vec(),
            value: 9_u64.to_le_bytes().to_vec(),
        })
        .unwrap();
    batch
}

fn encoded_user_pointer() -> Vec<u8> {
    vlog_format::ValuePointer {
        format_version: 0,
        file_id: 1,
        record_offset: 64,
        record_len: 64,
        value_len: 1,
    }
    .encode()
    .unwrap()
    .to_vec()
}

fn encoded_initial_metadata() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let identity = descriptor::DatabaseIdentity {
        identity_format_version: 0,
        database_format_version: 0,
        database_uuid: [0x5a; 16],
        keyspace_layout_version: 0,
    }
    .encode()
    .unwrap()
    .to_vec();
    let head_seq = descriptor::encode_head_seq(0).to_vec();
    let frontier = descriptor::DurableFrontier {
        durable_seq: 0,
        durable_vlog_end: descriptor::DurableVLogEnd::Empty,
    }
    .encode()
    .unwrap()
    .to_vec();
    (identity, head_seq, frontier)
}

fn assert_backend_contract<B: IndexBackend>()
where
    B::Snapshot: Clone + Send + Sync,
    B::UserIterator: DoubleEndedIterator<Item = Result<IndexEntry>> + Send,
    B::InternalIterator: Iterator<Item = Result<IndexEntry>> + Send,
{
}

#[test]
fn backend_and_associated_types_satisfy_send_sync_contract() {
    assert_backend_contract::<FakeBackend>();
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FakeBackend>();
    assert_send_sync::<FakeSnapshot>();
}

#[test]
fn one_ordered_atomic_batch_expresses_all_three_logical_spaces() {
    let backend = FakeBackend::default();
    let batch = ordinary_three_space_batch();
    assert_eq!(batch.len(), 3);
    assert!(matches!(
        batch.operations()[0],
        IndexMutation::PutUser { .. }
    ));
    assert!(matches!(
        batch.operations()[1],
        IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            ..
        }
    ));
    assert!(matches!(
        batch.operations()[2],
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            ..
        }
    ));

    backend
        .commit_atomic(batch.clone(), IndexCommitMode::Buffer)
        .unwrap();
    assert!(backend.last_call_entered_commit());
    assert_eq!(
        backend.calls(),
        vec![FakeCommitCall {
            batch: batch.clone(),
            mode: IndexCommitMode::Buffer,
            entered_commit: true,
        }]
    );
    assert_eq!(backend.committed(), vec![(batch, IndexCommitMode::Buffer)]);
    assert_eq!(
        backend.get_user(b"user-key", None).unwrap(),
        Some(encoded_user_pointer())
    );
    assert_eq!(
        backend
            .get_internal(InternalIndexSpace::Transaction, b"txn-key")
            .unwrap(),
        Some(b"txn-metadata".to_vec())
    );
    assert_eq!(
        backend
            .get_internal(InternalIndexSpace::System, b"head_seq")
            .unwrap(),
        Some(9_u64.to_le_bytes().to_vec())
    );
}

#[test]
fn initialization_is_one_sync_all_triple_and_requires_an_empty_backend() {
    let backend = FakeBackend::default();
    let (encoded_identity, encoded_head_seq, encoded_frontier) = encoded_initial_metadata();
    let initialization = IndexAtomicBatch::initialize_database(
        encoded_identity.clone(),
        encoded_head_seq.clone(),
        encoded_frontier.clone(),
    )
    .unwrap();
    assert!(initialization.is_database_initialization());
    assert_eq!(initialization.len(), 3);
    assert!(matches!(
        &initialization.operations()[..],
        [
            IndexMutation::InitializeDatabaseIdentity { .. },
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key,
                ..
            },
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: frontier_key,
                ..
            }
        ] if key == b"head_seq" && frontier_key == b"durable_frontier"
    ));

    let error = backend
        .commit_atomic(initialization.clone(), IndexCommitMode::Buffer)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!backend.last_call_entered_commit());

    let nonempty_user = FakeBackend::default();
    nonempty_user.seed_user(b"existing-user", b"pointer");
    let error = nonempty_user
        .commit_atomic(initialization.clone(), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!nonempty_user.last_call_entered_commit());

    let nonempty_transaction = FakeBackend::default();
    nonempty_transaction.seed_internal(InternalIndexSpace::Transaction, b"existing-tx", b"value");
    let error = nonempty_transaction
        .commit_atomic(initialization.clone(), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!nonempty_transaction.last_call_entered_commit());

    let nonempty_system = FakeBackend::default();
    nonempty_system.seed_internal(InternalIndexSpace::System, b"existing-system", b"value");
    let error = nonempty_system
        .commit_atomic(initialization.clone(), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!nonempty_system.last_call_entered_commit());

    let existing_identity = FakeBackend::default();
    existing_identity.seed_identity(b"existing-identity");
    let error = existing_identity
        .commit_atomic(initialization.clone(), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert!(!existing_identity.last_call_entered_commit());

    backend
        .commit_atomic(initialization.clone(), IndexCommitMode::SyncAll)
        .unwrap();
    assert!(backend.last_call_entered_commit());
    assert_eq!(
        backend.get_database_identity().unwrap(),
        Some(encoded_identity)
    );
    assert_eq!(
        backend
            .get_internal(InternalIndexSpace::System, b"head_seq")
            .unwrap(),
        Some(encoded_head_seq)
    );
    assert_eq!(
        backend
            .get_internal(InternalIndexSpace::System, b"durable_frontier")
            .unwrap(),
        Some(encoded_frontier)
    );

    let error = backend
        .commit_atomic(initialization, IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert_eq!(error.source.kind, StorageErrorKind::InvalidLayout);
    assert!(!backend.last_call_entered_commit());
}

#[test]
fn initialization_rejects_noncanonical_metadata_before_commit() {
    let (encoded_identity, encoded_head_seq, encoded_frontier) = encoded_initial_metadata();

    let invalid_values = [
        (
            vec![0x11; 32],
            encoded_head_seq.clone(),
            encoded_frontier.clone(),
        ),
        (
            encoded_identity.clone(),
            descriptor::encode_head_seq(1).to_vec(),
            encoded_frontier.clone(),
        ),
        (
            encoded_identity.clone(),
            encoded_head_seq.clone(),
            descriptor::DurableFrontier {
                durable_seq: 1,
                durable_vlog_end: descriptor::DurableVLogEnd::Position(descriptor::VLogPos {
                    file_id: 0,
                    offset: 64,
                }),
            }
            .encode()
            .unwrap()
            .to_vec(),
        ),
    ];

    for (identity, head_seq, frontier) in invalid_values {
        let error =
            IndexAtomicBatch::initialize_database(identity, head_seq, frontier).unwrap_err();
        assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
    }

    let mut bad_identity_crc = encoded_identity.clone();
    bad_identity_crc[31] ^= 1;
    let error = IndexAtomicBatch::initialize_database(
        bad_identity_crc,
        encoded_head_seq.clone(),
        encoded_frontier.clone(),
    )
    .unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);

    let mut bad_frontier_crc = encoded_frontier;
    bad_frontier_crc[30] ^= 1;
    let error =
        IndexAtomicBatch::initialize_database(encoded_identity, encoded_head_seq, bad_frontier_crc)
            .unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
}

#[test]
fn ordinary_batches_reject_identity_put_delete_and_initializer_before_commit() {
    for mutation in [
        IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: b"database_identity".to_vec(),
            value: vec![1],
        },
        IndexMutation::DeleteInternal {
            space: InternalIndexSpace::System,
            key: b"database_identity".to_vec(),
        },
        IndexMutation::InitializeDatabaseIdentity {
            encoded_identity: vec![1],
        },
    ] {
        let mut batch = IndexAtomicBatch::new();
        let error = batch.try_push(mutation).unwrap_err();
        assert_eq!(error.kind, StorageErrorKind::InvalidArgument);
        assert!(batch.is_empty());
    }

    let mut transaction_batch = IndexAtomicBatch::new();
    transaction_batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key: b"database_identity".to_vec(),
            value: vec![1],
        })
        .unwrap();
    assert_eq!(transaction_batch.len(), 1);
}

#[test]
fn internal_scan_is_ordered_streaming_and_propagates_mid_iteration_error() {
    let backend = FakeBackend::default();
    backend.seed_internal(InternalIndexSpace::Transaction, b"a", b"1");
    backend.seed_internal(InternalIndexSpace::Transaction, b"b", b"2");
    backend.seed_internal(InternalIndexSpace::Transaction, b"c", b"3");
    backend.inject_scan_error_after(Some(1));

    let range = InternalKeyRange::new(Some(b"a".to_vec()), Some(b"d".to_vec())).unwrap();
    let mut iterator = backend
        .scan_internal(InternalIndexSpace::Transaction, range)
        .unwrap();
    assert_eq!(iterator.next().unwrap().unwrap().key, b"a");
    let error = iterator.next().unwrap().unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(iterator.next().unwrap().unwrap().key, b"b");
    assert_eq!(iterator.next().unwrap().unwrap().key, b"c");
    assert!(iterator.next().is_none());

    let empty_range = InternalKeyRange::new(Some(b"b".to_vec()), Some(b"b".to_vec())).unwrap();
    let mut empty_iterator = backend
        .scan_internal(InternalIndexSpace::Transaction, empty_range)
        .unwrap();
    assert!(empty_iterator.next().is_none());

    let invalid = InternalKeyRange::new(Some(b"z".to_vec()), Some(b"a".to_vec())).unwrap_err();
    assert_eq!(invalid.kind, StorageErrorKind::InvalidArgument);
}

#[test]
fn snapshots_and_user_iterators_return_owned_stable_entries() {
    let backend = FakeBackend::default();
    backend.seed_user(b"a", b"pointer-a");
    backend.seed_user(b"c", b"pointer-c");
    let snapshot = backend.snapshot().unwrap();

    backend.seed_user(b"b", b"pointer-b");
    assert_eq!(backend.get_user(b"b", Some(&snapshot)).unwrap(), None);
    assert_eq!(
        backend.get_user(b"b", None).unwrap(),
        Some(b"pointer-b".to_vec())
    );

    let mut iterator = backend.iter_user(Some(&snapshot)).unwrap();
    drop(backend);
    let first = iterator.next().unwrap().unwrap();
    let last = iterator.next_back().unwrap().unwrap();
    assert_eq!(first, IndexEntry::new(b"a".to_vec(), b"pointer-a".to_vec()));
    assert_eq!(last, IndexEntry::new(b"c".to_vec(), b"pointer-c".to_vec()));
    assert!(iterator.next().is_none());
}

#[test]
fn commit_failures_are_classified_by_whether_commit_was_entered() {
    let before = FakeBackend::default();
    before.set_failure(InjectCommitFailure::BeforeCommit);
    let before_batch = ordinary_three_space_batch();
    let error = before
        .commit_atomic(before_batch.clone(), IndexCommitMode::Buffer)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert_eq!(error.source.kind, StorageErrorKind::Io);
    assert!(!before.last_call_entered_commit());
    assert!(before.committed().is_empty());
    assert_eq!(
        before.calls(),
        vec![FakeCommitCall {
            batch: before_batch,
            mode: IndexCommitMode::Buffer,
            entered_commit: false,
        }]
    );

    let after = FakeBackend::default();
    after.set_failure(InjectCommitFailure::AfterCommitEntered);
    let after_batch = ordinary_three_space_batch();
    let error = after
        .commit_atomic(after_batch.clone(), IndexCommitMode::SyncAll)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::Unknown);
    assert_eq!(error.source.kind, StorageErrorKind::Io);
    assert!(after.last_call_entered_commit());
    assert!(after.committed().is_empty());
    assert_eq!(
        after.calls(),
        vec![FakeCommitCall {
            batch: after_batch,
            mode: IndexCommitMode::SyncAll,
            entered_commit: true,
        }]
    );
}

#[test]
fn empty_batches_fail_preflight_without_entering_commit() {
    let backend = FakeBackend::default();
    let error = backend
        .commit_atomic(IndexAtomicBatch::new(), IndexCommitMode::Buffer)
        .unwrap_err();
    assert_eq!(error.apply_state, IndexApplyState::NotApplied);
    assert_eq!(error.source.kind, StorageErrorKind::InvalidArgument);
    assert!(!backend.last_call_entered_commit());
}
