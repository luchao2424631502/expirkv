#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    DestroyFailureContext, DestroyStage, InstanceState, ManagedObject, Operation, ProtocolStage,
    Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition};
#[path = "../src/snapshot.rs"]
mod snapshot;
pub(crate) use snapshot::Snapshot;
#[path = "../src/cursor.rs"]
mod cursor;
pub(crate) use cursor::{DbIterator, KeyRange, RangeCursor};
#[path = "../src/batch.rs"]
mod batch;
pub(crate) use batch::WriteBatch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/options.rs"]
mod options;
pub(crate) use options::{Options, ReadOptions, WriteOptions};
#[path = "../src/db.rs"]
mod db;
#[path = "../src/format.rs"]
mod format;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/recovery/mod.rs"]
mod recovery;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use commit::{DurableFrontier, DurableVLogEnd, RECOVERY_STATE_KEY, VLogPos, encode_tx_meta_key};
use db::{background_cleanup_commit_error, background_failure_target, cleanup_descriptors_once};
use index::{
    DURABLE_FRONTIER_KEY, IndexApplyState, IndexAtomicBatch, IndexBackend, IndexCommitError,
    IndexCommitMode, IndexEntry, IndexMutation, InternalIndexError, InternalIndexSpace,
    InternalKeyRange,
};
use runtime::RuntimeControl;
use stats::StatsState;

#[derive(Default)]
struct FakeIndex {
    system: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    transaction: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    user: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    successful_commits: Mutex<Vec<(IndexCommitMode, Vec<IndexMutation>)>>,
    commit_attempt: AtomicUsize,
    fail_attempt: AtomicUsize,
}

impl FakeIndex {
    fn with_durable_seq(durable_seq: u64) -> Self {
        let backend = Self::default();
        let frontier = DurableFrontier {
            durable_seq,
            durable_vlog_end: if durable_seq == 0 {
                DurableVLogEnd::Empty
            } else {
                DurableVLogEnd::Position(VLogPos {
                    file_id: 0,
                    offset: 64,
                })
            },
        }
        .encode()
        .expect("frontier encodes");
        backend
            .system
            .lock()
            .unwrap()
            .insert(DURABLE_FRONTIER_KEY.to_vec(), frontier.to_vec());
        backend
    }

    fn insert_descriptor(&self, seq: u64, mutations: usize) {
        let mut transaction = self.transaction.lock().unwrap();
        transaction.insert(
            encode_tx_meta_key(seq).expect("meta key").to_vec(),
            vec![0xA0],
        );
        for ordinal in 0..mutations as u64 {
            transaction.insert(mutation_key(seq, ordinal), vec![0xB0]);
        }
    }

    fn contains_descriptor(&self, seq: u64) -> bool {
        let prefix = descriptor_prefix(seq);
        self.transaction
            .lock()
            .unwrap()
            .keys()
            .any(|key| key.starts_with(&prefix))
    }

    fn frontier_bytes(&self) -> Vec<u8> {
        self.system.lock().unwrap()[DURABLE_FRONTIER_KEY].clone()
    }

    fn fail_on_attempt(&self, attempt: usize) {
        self.fail_attempt.store(attempt, Ordering::Release);
    }
}

impl IndexBackend for FakeIndex {
    type Snapshot = ();
    type UserIterator = std::vec::IntoIter<Result<IndexEntry>>;
    type InternalIterator = std::vec::IntoIter<Result<IndexEntry>>;

    fn commit_atomic(
        &self,
        batch: IndexAtomicBatch,
        mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        let attempt = self.commit_attempt.fetch_add(1, Ordering::AcqRel) + 1;
        if self.fail_attempt.load(Ordering::Acquire) == attempt {
            return Err(IndexCommitError {
                apply_state: IndexApplyState::NotApplied,
                source: InternalIndexError::new(StorageErrorKind::Io, Some(5)),
            });
        }
        let operations = batch.into_operations();
        for operation in &operations {
            match operation {
                IndexMutation::DeleteInternal { space, key } => {
                    let map = match space {
                        InternalIndexSpace::System => &self.system,
                        InternalIndexSpace::Transaction => &self.transaction,
                    };
                    map.lock().unwrap().remove(key);
                }
                _ => panic!("cleanup emitted a non-delete mutation"),
            }
        }
        self.successful_commits
            .lock()
            .unwrap()
            .push((mode, operations));
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_user(&self, key: &[u8], _snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        Ok(self.user.lock().unwrap().get(key).cloned())
    }

    fn get_internal(&self, space: InternalIndexSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let map = match space {
            InternalIndexSpace::System => &self.system,
            InternalIndexSpace::Transaction => &self.transaction,
        };
        Ok(map.lock().unwrap().get(key).cloned())
    }

    fn scan_internal(
        &self,
        space: InternalIndexSpace,
        range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        let map = match space {
            InternalIndexSpace::System => &self.system,
            InternalIndexSpace::Transaction => &self.transaction,
        };
        let entries = map
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| {
                range
                    .start_inclusive
                    .as_ref()
                    .is_none_or(|start| key.as_slice() >= start.as_slice())
                    && range
                        .end_exclusive
                        .as_ref()
                        .is_none_or(|end| key.as_slice() < end.as_slice())
            })
            .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(entries.into_iter())
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(())
    }

    fn iter_user(&self, _snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        let entries = self
            .user
            .lock()
            .unwrap()
            .iter()
            .map(|(key, value)| Ok(IndexEntry::new(key.clone(), value.clone())))
            .collect::<Vec<_>>();
        Ok(entries.into_iter())
    }
}

fn descriptor_prefix(seq: u64) -> [u8; 10] {
    let mut prefix = [0_u8; 10];
    prefix[0..2].copy_from_slice(b"TX");
    prefix[2..10].copy_from_slice(&seq.to_be_bytes());
    prefix
}

fn mutation_key(seq: u64, ordinal: u64) -> Vec<u8> {
    let mut key = Vec::from(descriptor_prefix(seq));
    key.push(1);
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn deleted_keys(commit: &(IndexCommitMode, Vec<IndexMutation>)) -> Vec<Vec<u8>> {
    commit
        .1
        .iter()
        .map(|operation| match operation {
            IndexMutation::DeleteInternal {
                space: InternalIndexSpace::Transaction,
                key,
            } => key.clone(),
            _ => panic!("unexpected cleanup operation"),
        })
        .collect()
}

#[test]
fn cleanup_captures_the_persisted_frontier_and_deletes_meta_last() {
    let backend = FakeIndex::with_durable_seq(2);
    backend.insert_descriptor(1, 2);
    backend.insert_descriptor(2, 1);
    backend.insert_descriptor(3, 1);
    let frontier_before = backend.frontier_bytes();

    let progress = cleanup_descriptors_once(&backend, &AtomicBool::new(false)).unwrap();
    assert_eq!(progress.captured_durable_seq, 2);
    assert_eq!(progress.deleted_mutations, 3);
    assert_eq!(progress.deleted_meta, 2);
    assert!(!backend.contains_descriptor(1));
    assert!(!backend.contains_descriptor(2));
    assert!(backend.contains_descriptor(3));
    assert_eq!(backend.frontier_bytes(), frontier_before);

    let commits = backend.successful_commits.lock().unwrap();
    assert!(
        commits
            .iter()
            .all(|(mode, _)| *mode == IndexCommitMode::Buffer)
    );
    assert_eq!(commits.len(), 4);
    assert!(deleted_keys(&commits[0]).iter().all(|key| key.len() == 19));
    assert_eq!(
        deleted_keys(&commits[1]),
        vec![encode_tx_meta_key(1).unwrap()]
    );
    assert!(deleted_keys(&commits[2]).iter().all(|key| key.len() == 19));
    assert_eq!(
        deleted_keys(&commits[3]),
        vec![encode_tx_meta_key(2).unwrap()]
    );
}

#[test]
fn cleanup_batches_at_1024_keys_and_resumes_after_a_not_applied_failure() {
    let backend = FakeIndex::with_durable_seq(1);
    backend.insert_descriptor(1, 2_050);
    backend
        .user
        .lock()
        .unwrap()
        .insert(b"result".to_vec(), b"kept".to_vec());
    let frontier_before = backend.frontier_bytes();
    backend.fail_on_attempt(2);

    let error = cleanup_descriptors_once(&backend, &AtomicBool::new(false)).unwrap_err();
    assert_eq!(error.kind, StorageErrorKind::Io);
    assert_eq!(error.operation, Operation::Background);
    assert_eq!(error.protocol_stage, ProtocolStage::Maintenance);
    assert!(backend.contains_descriptor(1));
    assert_eq!(backend.frontier_bytes(), frontier_before);
    assert_eq!(
        backend.get_user(b"result", None).unwrap(),
        Some(b"kept".to_vec())
    );

    backend.fail_on_attempt(usize::MAX);
    let progress = cleanup_descriptors_once(&backend, &AtomicBool::new(false)).unwrap();
    assert_eq!(progress.deleted_mutations, 1_026);
    assert_eq!(progress.deleted_meta, 1);
    assert!(!backend.contains_descriptor(1));
    assert_eq!(backend.frontier_bytes(), frontier_before);

    let commits = backend.successful_commits.lock().unwrap();
    let sizes = commits
        .iter()
        .map(|commit| commit.1.len())
        .collect::<Vec<_>>();
    assert_eq!(sizes, vec![1_024, 1_024, 2, 1]);
    assert!(commits.iter().all(|commit| commit.1.len() <= 1_024));
    assert!(
        commits
            .iter()
            .all(|(mode, _)| *mode == IndexCommitMode::Buffer)
    );
}

#[test]
fn recovery_state_blocks_cleanup_without_scanning_or_committing() {
    let backend = FakeIndex::with_durable_seq(1);
    backend.insert_descriptor(1, 1);
    backend
        .system
        .lock()
        .unwrap()
        .insert(RECOVERY_STATE_KEY.to_vec(), vec![1]);

    let progress = cleanup_descriptors_once(&backend, &AtomicBool::new(false)).unwrap();
    assert!(progress.blocked_by_recovery);
    assert_eq!(progress.committed_batches, 0);
    assert!(backend.contains_descriptor(1));
    assert!(backend.successful_commits.lock().unwrap().is_empty());
}

#[test]
fn cleanup_commit_apply_state_and_error_kind_control_runtime_trust() {
    for apply_state in [IndexApplyState::NotApplied, IndexApplyState::Unknown] {
        for kind in [
            StorageErrorKind::Busy,
            StorageErrorKind::ResourceExhausted,
            StorageErrorKind::Io,
        ] {
            let runtime = RuntimeControl::new(Arc::new(StatsState::new()));
            let mapped = background_cleanup_commit_error(IndexCommitError {
                apply_state,
                source: InternalIndexError::new(kind, Some(5)),
            });

            assert_eq!(mapped.kind, kind);
            assert_eq!(mapped.operation, Operation::Background);
            assert_eq!(mapped.protocol_stage, ProtocolStage::Maintenance);
            assert_eq!(mapped.write_outcome, None);
            assert_eq!(mapped.instance_state, None);
            assert_eq!(mapped.os_code, Some(5));
            let retryable_same_instance = apply_state == IndexApplyState::NotApplied
                && matches!(
                    kind,
                    StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted
                );
            assert_eq!(
                mapped.retry_advice,
                if retryable_same_instance {
                    RetryAdvice::RetrySameInstance
                } else {
                    RetryAdvice::ReopenAndVerify
                }
            );

            if let Some(target) = background_failure_target(&mapped) {
                runtime.latch_failure(target, &mapped);
            }
            let state = runtime.state();
            if retryable_same_instance {
                assert_eq!(state.instance_state, InstanceState::Healthy);
                assert_eq!(state.state_epoch, 0);
                assert!(runtime.accepting_writes_for_test());
                assert!(runtime.first_latched_error().is_none());
            } else {
                assert_eq!(state.instance_state, InstanceState::Poisoned);
                assert_eq!(state.state_epoch, 1);
                assert!(!runtime.accepting_writes_for_test());
                let first = runtime.first_latched_error().expect("first error latched");
                assert_eq!(first.kind, kind);
                assert_eq!(first.operation, Operation::Background);
                assert_eq!(first.protocol_stage, ProtocolStage::Maintenance);
                assert_eq!(first.retry_advice, RetryAdvice::ReopenAndVerify);
            }
        }
    }
}
