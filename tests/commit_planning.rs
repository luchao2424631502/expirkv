#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::io;
use std::sync::Mutex;

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};

#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition as PublicVLogPosition};
#[path = "../src/vlog/mod.rs"]
mod vlog;
pub(crate) use vlog::format as vlog_format;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/runtime/mod.rs"]
mod runtime;

use batch::WriteBatch;
use commit::{
    TransactionDescriptor, TxUuidSource, ValueState, decode_descriptor, decode_head_seq,
    preflight_batch, preflight_delete, preflight_put, prepare_commit,
};
use index::{
    HEAD_SEQ_KEY, IndexAtomicBatch, IndexBackend, IndexCommitError, IndexCommitMode, IndexEntry,
    IndexMutation, InternalIndexSpace, InternalKeyRange,
};
use vlog_format::{
    DecodedRecord, VLogGeometry, VLogPosition, ValuePointer, decode_record_at,
    scan_prepared_envelope,
};

#[derive(Default)]
struct FakeBackend {
    pointers: BTreeMap<Vec<u8>, Vec<u8>>,
    reads: Mutex<Vec<Vec<u8>>>,
    commits: Mutex<usize>,
}

impl FakeBackend {
    fn with_pointer(mut self, key: &[u8], pointer: ValuePointer) -> Self {
        self.pointers
            .insert(key.to_vec(), pointer.encode().unwrap().to_vec());
        self
    }

    fn read_keys(&self) -> Vec<Vec<u8>> {
        self.reads.lock().unwrap().clone()
    }
}

impl IndexBackend for FakeBackend {
    type Snapshot = ();
    type UserIterator = std::vec::IntoIter<Result<IndexEntry>>;
    type InternalIterator = std::vec::IntoIter<Result<IndexEntry>>;

    fn commit_atomic(
        &self,
        _batch: IndexAtomicBatch,
        _mode: IndexCommitMode,
    ) -> std::result::Result<(), IndexCommitError> {
        *self.commits.lock().unwrap() += 1;
        Ok(())
    }

    fn get_database_identity(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn get_user(&self, key: &[u8], _snapshot: Option<&Self::Snapshot>) -> Result<Option<Vec<u8>>> {
        self.reads.lock().unwrap().push(key.to_vec());
        Ok(self.pointers.get(key).cloned())
    }

    fn get_internal(&self, _space: InternalIndexSpace, _key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn scan_internal(
        &self,
        _space: InternalIndexSpace,
        _range: InternalKeyRange,
    ) -> Result<Self::InternalIterator> {
        Ok(Vec::new().into_iter())
    }

    fn snapshot(&self) -> Result<Self::Snapshot> {
        Ok(())
    }

    fn iter_user(&self, _snapshot: Option<&Self::Snapshot>) -> Result<Self::UserIterator> {
        Ok(Vec::new().into_iter())
    }
}

struct FixedUuidSource([u8; 16]);

impl TxUuidSource for FixedUuidSource {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        *output = self.0;
        Ok(())
    }
}

fn old_pointer(record_offset: u32, value_len: u16) -> ValuePointer {
    ValuePointer {
        format_version: 0,
        file_id: 0,
        record_offset,
        record_len: 55 + 1 + u32::from(value_len),
        value_len,
    }
}

fn old_pointer_for_key(key: &[u8], record_offset: u32, value_len: u16) -> ValuePointer {
    ValuePointer {
        format_version: 0,
        file_id: 0,
        record_offset,
        record_len: 55 + u32::try_from(key.len()).unwrap() + u32::from(value_len),
        value_len,
    }
}

fn descriptor_from_batch(batch: &IndexAtomicBatch) -> TransactionDescriptor {
    let mut meta: Option<(&[u8], &[u8])> = None;
    let mut mutations = Vec::new();
    for operation in batch.operations() {
        if let IndexMutation::PutInternal {
            space: InternalIndexSpace::Transaction,
            key,
            value,
        } = operation
        {
            match key.get(10) {
                Some(0) => meta = Some((key, value)),
                Some(1) => mutations.push((key.as_slice(), value.as_slice())),
                _ => panic!("unexpected transaction descriptor key"),
            }
        }
    }
    let (meta_key, meta_value) = meta.expect("missing TxMeta");
    decode_descriptor(meta_key, meta_value, &mutations).unwrap()
}

fn user_mutations(batch: &IndexAtomicBatch) -> Vec<&IndexMutation> {
    batch
        .operations()
        .iter()
        .filter(|mutation| {
            matches!(
                mutation,
                IndexMutation::PutUser { .. } | IndexMutation::DeleteUser { .. }
            )
        })
        .collect()
}

fn assert_head(batch: &IndexAtomicBatch, expected: u64) {
    let head = batch
        .operations()
        .iter()
        .find_map(|mutation| match mutation {
            IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key,
                value,
            } if key == HEAD_SEQ_KEY => Some(value),
            _ => None,
        })
        .expect("missing head_seq");
    assert_eq!(decode_head_seq(head).unwrap(), expected);
}

fn assert_put_user(batch: &IndexAtomicBatch, key: &[u8], expected: ValuePointer) {
    let encoded = batch
        .operations()
        .iter()
        .find_map(|mutation| match mutation {
            IndexMutation::PutUser {
                user_key,
                encoded_pointer,
            } if user_key == key => Some(encoded_pointer),
            _ => None,
        })
        .expect("missing final PutUser mutation");
    assert_eq!(ValuePointer::decode(encoded).unwrap(), expected);
}

fn assert_delete_user(batch: &IndexAtomicBatch, key: &[u8]) {
    assert!(batch.operations().iter().any(|mutation| {
        matches!(mutation, IndexMutation::DeleteUser { user_key } if user_key == key)
    }));
}

#[test]
fn single_put_and_delete_plan_complete_envelopes_and_atomic_batches() {
    let backend = FakeBackend::default();
    let mut uuid = FixedUuidSource([0x11; 16]);
    let write = preflight_put(b"alpha", b"", false).unwrap();
    let put = prepare_commit(
        &write,
        [0x77; 16],
        4,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap();

    assert_eq!(put.commit_seq, 5);
    assert_eq!(put.tx_uuid.0[6] >> 4, 4, "UUID version must be v4");
    assert_eq!(put.tx_uuid.0[8] >> 6, 2, "UUID variant must be RFC 4122");
    assert_eq!(put.envelope.value_pointers.len(), 1);
    assert_eq!(put.vlog_begin.file_id, put.envelope.vlog_begin.file_id);
    assert_eq!(put.vlog_begin.offset, put.envelope.vlog_begin.offset);
    assert_eq!(put.vlog_end.file_id, put.envelope.vlog_end.file_id);
    assert_eq!(put.vlog_end.offset, put.envelope.vlog_end.offset);
    let pointer = put.envelope.value_pointers[0].unwrap();
    let chunk = put
        .envelope
        .chunks
        .iter()
        .find(|chunk| {
            chunk.position.file_id == pointer.file_id
                && chunk.position.offset == u64::from(pointer.record_offset)
        })
        .expect("pointer must target the KV_RECORD start");
    match decode_record_at(&chunk.bytes, chunk.position, VLogGeometry::PRODUCTION).unwrap() {
        DecodedRecord::KvRecord(record) => {
            assert_eq!(record.key, b"alpha");
            assert_eq!(record.value, b"");
            assert_eq!(record.op_index, 0);
        }
        other => panic!("pointer targeted {other:?}"),
    }
    assert_eq!(pointer.record_len as usize, chunk.bytes.len());
    assert_eq!(pointer.value_len, 0);

    let descriptor = descriptor_from_batch(&put.index_batch);
    assert_eq!(descriptor.meta.prev_seq, 4);
    assert_eq!(descriptor.meta.logical_op_count, 1);
    assert_eq!(descriptor.meta.distinct_key_count, 1);
    assert_eq!(descriptor.mutations[0].before_state, ValueState::Absent);
    assert_eq!(
        descriptor.mutations[0].after_state,
        ValueState::Present(pointer)
    );
    assert_eq!(user_mutations(&put.index_batch).len(), 1);
    assert_put_user(&put.index_batch, b"alpha", pointer);
    assert_head(&put.index_batch, 5);
    put.index_batch
        .validate_for_commit(IndexCommitMode::Buffer)
        .unwrap();
    assert_eq!(
        *backend.commits.lock().unwrap(),
        0,
        "planning must not commit"
    );

    let backend = FakeBackend::default();
    let mut uuid = FixedUuidSource([0x22; 16]);
    let write = preflight_delete(b"missing", false).unwrap();
    let delete = prepare_commit(
        &write,
        [0x77; 16],
        5,
        put.envelope.vlog_end,
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap();
    let scanned = scan_prepared_envelope(
        &delete.envelope.chunks,
        VLogGeometry::PRODUCTION,
        [0x77; 16],
        delete.envelope.vlog_begin,
        delete.envelope.vlog_end,
        Some(delete.envelope.envelope_crc32c),
    )
    .unwrap();
    assert_eq!(scanned.kv_record_count, 0);
    assert_eq!(scanned.delete_record_count, 1);
    let descriptor = descriptor_from_batch(&delete.index_batch);
    assert_eq!(descriptor.mutations[0].before_state, ValueState::Absent);
    assert_eq!(descriptor.mutations[0].after_state, ValueState::Absent);
    assert!(matches!(
        user_mutations(&delete.index_batch)[0],
        IndexMutation::DeleteUser { .. }
    ));
    assert_delete_user(&delete.index_batch, b"missing");
}

#[test]
fn deleting_an_existing_key_plans_present_to_absent() {
    let before = old_pointer_for_key(b"existing", 256, 7);
    let backend = FakeBackend::default().with_pointer(b"existing", before);
    let write = preflight_delete(b"existing", false).unwrap();
    let mut uuid = FixedUuidSource([0x2a; 16]);
    let planned = prepare_commit(
        &write,
        [0x77; 16],
        6,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap();

    assert_eq!(backend.read_keys(), vec![b"existing"]);
    assert_eq!(planned.envelope.value_pointers, vec![None]);
    let descriptor = descriptor_from_batch(&planned.index_batch);
    assert_eq!(descriptor.mutations.len(), 1);
    assert_eq!(descriptor.mutations[0].user_key, b"existing");
    assert_eq!(
        descriptor.mutations[0].before_state,
        ValueState::Present(before)
    );
    assert_eq!(descriptor.mutations[0].after_state, ValueState::Absent);
    assert_eq!(user_mutations(&planned.index_batch).len(), 1);
    assert_delete_user(&planned.index_batch, b"existing");
    assert_head(&planned.index_batch, 7);

    let scanned = scan_prepared_envelope(
        &planned.envelope.chunks,
        VLogGeometry::PRODUCTION,
        [0x77; 16],
        planned.envelope.vlog_begin,
        planned.envelope.vlog_end,
        Some(planned.envelope.envelope_crc32c),
    )
    .unwrap();
    assert_eq!(scanned.logical_op_count, 1);
    assert_eq!(scanned.kv_record_count, 0);
    assert_eq!(scanned.delete_record_count, 1);
}

#[test]
fn repeated_keys_keep_every_vlog_operation_but_publish_only_final_states() {
    let backend = FakeBackend::default()
        .with_pointer(b"a", old_pointer(64, 3))
        .with_pointer(b"c", old_pointer(128, 4));
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"a1").unwrap();
    batch.put(b"a", b"a2").unwrap();
    batch.put(b"b", b"b1").unwrap();
    batch.delete(b"b").unwrap();
    batch.delete(b"c").unwrap();
    batch.put(b"c", b"c1").unwrap();
    batch.delete(b"d").unwrap();
    batch.delete(b"d").unwrap();

    let write = preflight_batch(&batch, false).unwrap();
    assert_eq!(write.logical_op_count(), 8);
    assert_eq!(write.distinct_key_count(), 4);
    let mut uuid = FixedUuidSource([0x33; 16]);
    let planned = prepare_commit(
        &write,
        [0x88; 16],
        9,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut uuid,
    )
    .unwrap();

    assert_eq!(backend.read_keys(), vec![b"a", b"b", b"c", b"d"]);
    let scanned = scan_prepared_envelope(
        &planned.envelope.chunks,
        VLogGeometry::PRODUCTION,
        [0x88; 16],
        planned.envelope.vlog_begin,
        planned.envelope.vlog_end,
        Some(planned.envelope.envelope_crc32c),
    )
    .unwrap();
    assert_eq!(scanned.logical_op_count, 8);
    assert_eq!(scanned.distinct_key_count, 4);
    assert_eq!(scanned.kv_record_count, 4);
    assert_eq!(scanned.delete_record_count, 4);
    assert_eq!(planned.envelope.value_pointers.len(), 8);

    let logical_records = planned
        .envelope
        .chunks
        .iter()
        .filter(|chunk| chunk.bytes.get(0..4) == Some(b"RKVR".as_slice()))
        .filter_map(|chunk| {
            match decode_record_at(&chunk.bytes, chunk.position, VLogGeometry::PRODUCTION).unwrap()
            {
                DecodedRecord::KvRecord(record) => Some((
                    record.op_index,
                    b"put".as_slice(),
                    record.key.to_vec(),
                    record.value.to_vec(),
                )),
                DecodedRecord::DeleteRecord(record) => Some((
                    record.op_index,
                    b"delete".as_slice(),
                    record.key.to_vec(),
                    Vec::new(),
                )),
                DecodedRecord::TxBegin(_)
                | DecodedRecord::TxPreparedEnd(_)
                | DecodedRecord::PageEnd => None,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        logical_records,
        vec![
            (0, b"put".as_slice(), b"a".to_vec(), b"a1".to_vec()),
            (1, b"put".as_slice(), b"a".to_vec(), b"a2".to_vec()),
            (2, b"put".as_slice(), b"b".to_vec(), b"b1".to_vec()),
            (3, b"delete".as_slice(), b"b".to_vec(), Vec::new()),
            (4, b"delete".as_slice(), b"c".to_vec(), Vec::new()),
            (5, b"put".as_slice(), b"c".to_vec(), b"c1".to_vec()),
            (6, b"delete".as_slice(), b"d".to_vec(), Vec::new()),
            (7, b"delete".as_slice(), b"d".to_vec(), Vec::new()),
        ]
    );

    let descriptor = descriptor_from_batch(&planned.index_batch);
    assert_eq!(
        descriptor
            .mutations
            .iter()
            .map(|mutation| mutation.user_key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b", b"c", b"d"]
    );
    assert_eq!(
        descriptor.mutations[0].before_state,
        ValueState::Present(old_pointer(64, 3))
    );
    assert_eq!(
        descriptor.mutations[0].after_state,
        ValueState::Present(planned.envelope.value_pointers[1].unwrap())
    );
    assert_eq!(descriptor.mutations[1].before_state, ValueState::Absent);
    assert_eq!(descriptor.mutations[1].after_state, ValueState::Absent);
    assert_eq!(
        descriptor.mutations[2].before_state,
        ValueState::Present(old_pointer(128, 4))
    );
    assert_eq!(
        descriptor.mutations[2].after_state,
        ValueState::Present(planned.envelope.value_pointers[5].unwrap())
    );
    assert_eq!(descriptor.mutations[3].before_state, ValueState::Absent);
    assert_eq!(descriptor.mutations[3].after_state, ValueState::Absent);
    assert_eq!(user_mutations(&planned.index_batch).len(), 4);
    assert_put_user(
        &planned.index_batch,
        b"a",
        planned.envelope.value_pointers[1].unwrap(),
    );
    assert_delete_user(&planned.index_batch, b"b");
    assert_put_user(
        &planned.index_batch,
        b"c",
        planned.envelope.value_pointers[5].unwrap(),
    );
    assert_delete_user(&planned.index_batch, b"d");
    assert_eq!(
        planned.index_batch.len(),
        10,
        "4 users + meta + 4 mutations + head"
    );
    assert_head(&planned.index_batch, 10);
}

#[test]
fn sync_flag_does_not_change_transaction_content_planning() {
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"1").unwrap();
    batch.delete(b"b").unwrap();
    let backend = FakeBackend::default();

    let false_write = preflight_batch(&batch, false).unwrap();
    let mut false_uuid = FixedUuidSource([0x44; 16]);
    let false_plan = prepare_commit(
        &false_write,
        [0x99; 16],
        2,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut false_uuid,
    )
    .unwrap();

    let true_write = preflight_batch(&batch, true).unwrap();
    let mut true_uuid = FixedUuidSource([0x44; 16]);
    let true_plan = prepare_commit(
        &true_write,
        [0x99; 16],
        2,
        VLogPosition {
            file_id: 0,
            offset: 0,
        },
        VLogGeometry::PRODUCTION,
        &backend,
        &mut true_uuid,
    )
    .unwrap();

    assert!(!false_plan.sync);
    assert!(true_plan.sync);
    assert_eq!(false_plan.commit_seq, true_plan.commit_seq);
    assert_eq!(false_plan.tx_uuid, true_plan.tx_uuid);
    assert_eq!(false_plan.envelope, true_plan.envelope);
    assert_eq!(false_plan.index_batch, true_plan.index_batch);
}
