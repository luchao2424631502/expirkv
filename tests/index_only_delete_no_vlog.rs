#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

#[path = "../src/error.rs"]
mod error;
pub(crate) use error::{
    InstanceState, Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind,
    WriteOutcome,
};
#[path = "../src/stats.rs"]
mod stats;
pub(crate) use stats::{DbStats, LatchedErrorSummary, VLogPosition as PublicVLogPosition};
#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/commit/mod.rs"]
mod commit;
#[path = "../src/index/mod.rs"]
mod index;
#[path = "../src/lock.rs"]
mod lock;
#[path = "../src/runtime/mod.rs"]
mod runtime;
#[path = "../src/vlog/mod.rs"]
mod vlog;

use batch::WriteBatch;
use commit::{
    CommitCoordinator, DurableFrontier, DurableVLogEnd, TransactionDescriptor, TransactionKind,
    TxUuidSource, VLogPos, ValueState, decode_descriptor, decode_tx_mutation_key,
    encode_tx_meta_key, preflight_batch, preflight_delete, preflight_put,
};
use index::{
    DURABLE_FRONTIER_KEY, FjallBackend, FjallIndexOptions, IndexBackend, IndexCommitMode,
    IndexCompression, InternalIndexSpace, InternalKeyRange, initialization_batch,
};
use runtime::RuntimeControl;
use stats::StatsState;
use tempfile::TempDir;
use vlog::file_set::{FileCatalog, FileSet, VLogDirectory};
use vlog::format::{
    VLogGeometry, reset_vlog_format_call_counts_for_test, vlog_format_call_counts_for_test,
};
use vlog::reader::{
    ValueLogReader, reset_vlog_reader_call_counts_for_test, vlog_reader_call_counts_for_test,
};
use vlog::writer::{
    ValueLogWriter, reset_vlog_writer_call_counts_for_test, vlog_writer_call_counts_for_test,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DATABASE_UUID: [u8; 16] = [0x6d; 16];

struct FixedUuid(u8);

impl TxUuidSource for FixedUuid {
    fn fill_random_bytes(&mut self, output: &mut [u8; 16]) -> io::Result<()> {
        output.fill(self.0);
        self.0 = self.0.wrapping_add(1);
        Ok(())
    }
}

struct Harness {
    _temporary: TempDir,
    vlog_path: PathBuf,
    backend: Arc<FjallBackend>,
    coordinator: CommitCoordinator<FjallBackend, FixedUuid>,
    stats: Arc<StatsState>,
    directory: Arc<VLogDirectory>,
    catalog: Arc<FileCatalog>,
    geometry: VLogGeometry,
}

impl Harness {
    fn new(geometry: VLogGeometry) -> TestResult<Self> {
        let temporary = tempfile::tempdir()?;
        let index_path = temporary.path().join("index");
        let vlog_path = temporary.path().join("vlog");
        std::fs::create_dir(&vlog_path)?;
        let backend = Arc::new(FjallBackend::create(&index_path, fjall_options())?);
        backend
            .commit_atomic(
                initialization_batch(0, DATABASE_UUID)
                    .map_err(|error| io::Error::other(format!("initial batch: {error:?}")))?,
                IndexCommitMode::SyncAll,
            )
            .map_err(|error| io::Error::other(format!("initial commit: {error:?}")))?;

        let directory = Arc::new(VLogDirectory::open(&vlog_path)?);
        let catalog = Arc::new(FileCatalog::new());
        let writer = ValueLogWriter::empty(
            Arc::clone(&directory),
            DATABASE_UUID,
            geometry,
            Arc::clone(&catalog),
        )?;
        let stats = Arc::new(StatsState::new());
        let runtime = RuntimeControl::new(Arc::clone(&stats));
        let coordinator = CommitCoordinator::new(
            runtime,
            Arc::clone(&stats),
            Arc::clone(&backend),
            writer,
            FixedUuid(0x31),
            0,
            empty_frontier(),
            0,
            None,
        )?;
        Ok(Self {
            _temporary: temporary,
            vlog_path,
            backend,
            coordinator,
            stats,
            directory,
            catalog,
            geometry,
        })
    }

    fn put(&self, key: &[u8], value: &[u8], sync: bool) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_put(key, value, sync)?)?;
        Ok(())
    }

    fn delete(&self, key: &[u8], sync: bool) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_delete(key, sync)?)?;
        Ok(())
    }

    fn write(&self, batch: &WriteBatch, sync: bool) -> TestResult {
        self.coordinator
            .commit_nonempty(&preflight_batch(batch, sync)?)?;
        Ok(())
    }

    fn barrier(&self) -> TestResult {
        self.coordinator.commit_empty_batch(true)?;
        Ok(())
    }

    fn descriptor(&self, seq: u64) -> TestResult<TransactionDescriptor> {
        let meta_key = encode_tx_meta_key(seq)?;
        let meta_value = self
            .backend
            .get_internal(InternalIndexSpace::Transaction, &meta_key)?
            .unwrap_or_else(|| panic!("missing descriptor metadata for seq {seq}"));
        let mut owned_mutations = Vec::new();
        for entry in self
            .backend
            .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())?
        {
            let entry = entry?;
            if decode_tx_mutation_key(&entry.key).is_ok_and(|(entry_seq, _)| entry_seq == seq) {
                owned_mutations.push((entry.key, entry.value));
            }
        }
        let mutation_refs = owned_mutations
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        Ok(decode_descriptor(&meta_key, &meta_value, &mutation_refs)?)
    }

    fn frontier(&self) -> TestResult<DurableFrontier> {
        let encoded = self
            .backend
            .get_internal(InternalIndexSpace::System, DURABLE_FRONTIER_KEY)?
            .expect("durable frontier");
        Ok(DurableFrontier::decode(&encoded)?)
    }

    fn transaction_entry_count(&self) -> TestResult<usize> {
        Ok(self
            .backend
            .scan_internal(InternalIndexSpace::Transaction, InternalKeyRange::all())?
            .collect::<Result<Vec<_>>>()?
            .len())
    }

    fn vlog_inventory(&self) -> TestResult<Vec<(String, u64)>> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.vlog_path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('D') && name.ends_with(".data") {
                files.push((name, entry.metadata()?.len()));
            }
        }
        files.sort_unstable();
        Ok(files)
    }

    fn reader(&self, capacity: usize) -> TestResult<(Arc<FileSet>, ValueLogReader)> {
        let files = Arc::new(FileSet::new(
            Arc::clone(&self.directory),
            DATABASE_UUID,
            self.geometry,
            Arc::clone(&self.catalog),
            capacity,
        )?);
        let reader = ValueLogReader::new(Arc::clone(&files), self.geometry)?;
        Ok((files, reader))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ProbeSnapshot {
    layout_plans: u64,
    append: u64,
    file_create: u64,
    file_roll: u64,
    value_dereference: u64,
    file_sync: u64,
    directory_sync: u64,
}

fn reset_probes() {
    reset_vlog_format_call_counts_for_test();
    reset_vlog_writer_call_counts_for_test();
    reset_vlog_reader_call_counts_for_test();
}

fn probe_snapshot() -> ProbeSnapshot {
    let format = vlog_format_call_counts_for_test();
    let writer = vlog_writer_call_counts_for_test();
    let reader = vlog_reader_call_counts_for_test();
    ProbeSnapshot {
        layout_plans: format.commit_layout_plans,
        append: writer.append,
        file_create: writer.file_create,
        file_roll: writer.file_roll,
        value_dereference: reader.value_dereference,
        file_sync: writer.file_sync,
        directory_sync: writer.directory_sync,
    }
}

fn assert_zero_vlog_calls(action: impl FnOnce() -> TestResult) -> TestResult {
    reset_probes();
    action()?;
    let measured = probe_snapshot();
    assert_eq!(measured, ProbeSnapshot::default());
    Ok(())
}

fn assert_index_only_descriptor(
    descriptor: &TransactionDescriptor,
    logical_ops: u64,
    expected_keys: &[&[u8]],
) {
    assert_eq!(
        descriptor.meta.transaction_kind,
        TransactionKind::IndexOnlyDelete
    );
    assert_eq!(descriptor.meta.logical_op_count, logical_ops);
    assert_eq!(
        descriptor.meta.distinct_key_count,
        expected_keys.len() as u64
    );
    assert_eq!(
        descriptor.meta.vlog_begin,
        VLogPos {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(
        descriptor.meta.vlog_end,
        VLogPos {
            file_id: 0,
            offset: 0
        }
    );
    assert_eq!(descriptor.meta.envelope_crc32c, 0);
    assert_eq!(
        descriptor
            .mutations
            .iter()
            .map(|mutation| mutation.user_key.as_slice())
            .collect::<Vec<_>>(),
        expected_keys
    );
    assert!(
        descriptor
            .mutations
            .iter()
            .all(|mutation| mutation.after_state == ValueState::Absent)
    );
}

#[test]
fn fresh_single_and_repeated_batch_deletes_never_touch_vlog_and_commit_exact_metadata() -> TestResult
{
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;

    assert_zero_vlog_calls(|| harness.delete(b"missing", false))?;
    let first = harness.descriptor(1)?;
    assert_index_only_descriptor(&first, 1, &[b"missing"]);
    assert_eq!(first.mutations[0].before_state, ValueState::Absent);

    let mut repeated = WriteBatch::new();
    repeated.delete(b"alpha")?;
    repeated.delete(b"beta")?;
    repeated.delete(b"alpha")?;
    assert_zero_vlog_calls(|| harness.write(&repeated, false))?;
    let second = harness.descriptor(2)?;
    assert_index_only_descriptor(&second, 3, &[b"alpha", b"beta"]);

    assert_zero_vlog_calls(|| harness.delete(b"alpha", true))?;
    let third = harness.descriptor(3)?;
    assert_index_only_descriptor(&third, 1, &[b"alpha"]);

    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 3);
    assert_eq!(state.durable_seq, 3);
    assert_eq!(state.head_vlog_seq, 0);
    assert_eq!(state.durable_vlog_seq, 0);
    assert_eq!(state.head_vlog_end, None);
    assert_eq!(state.durable_vlog_end, None);
    assert_eq!(
        harness.frontier()?,
        DurableFrontier {
            durable_seq: 3,
            durable_vlog_seq: 0,
            durable_vlog_end: DurableVLogEnd::Empty,
        }
    );
    assert_eq!(harness.transaction_entry_count()?, 7);
    assert!(harness.vlog_inventory()?.is_empty());
    let stats = harness.stats.snapshot();
    assert_eq!(stats.head_seq, 3);
    assert_eq!(stats.durable_seq, 3);
    assert_eq!(stats.durability_lag, 0);
    assert!(stats.durable_vlog_end.is_none());
    assert_eq!(stats.active_vlog_file_id, None);
    assert_eq!(stats.vlog_file_count, 0);
    assert_eq!(stats.vlog_logical_bytes, 0);
    Ok(())
}

#[test]
fn pure_delete_batch_deletes_one_present_key_without_touching_vlog() -> TestResult {
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;
    harness.put(b"present", b"before-delete", true)?;
    let seed_descriptor = harness.descriptor(1)?;
    let present_state = seed_descriptor.mutations[0].after_state.clone();
    assert!(matches!(present_state, ValueState::Present(_)));
    let inventory_before = harness.vlog_inventory()?;
    let state_before = harness.coordinator.state_snapshot();

    let mut deletes = WriteBatch::new();
    deletes.delete(b"present")?;
    assert_zero_vlog_calls(|| harness.write(&deletes, true))?;

    let descriptor = harness.descriptor(2)?;
    assert_index_only_descriptor(&descriptor, 1, &[b"present"]);
    assert_eq!(descriptor.mutations[0].before_state, present_state);
    assert_eq!(descriptor.mutations[0].after_state, ValueState::Absent);
    assert!(harness.backend.get_user(b"present", None)?.is_none());
    assert_eq!(harness.vlog_inventory()?, inventory_before);

    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 2);
    assert_eq!(state.durable_seq, 2);
    assert_eq!(state.head_vlog_seq, 1);
    assert_eq!(state.durable_vlog_seq, 1);
    assert_eq!(state.head_vlog_end, state_before.head_vlog_end);
    assert_eq!(state.durable_vlog_end, state_before.durable_vlog_end);
    Ok(())
}

#[test]
fn pure_delete_batch_deletes_multiple_present_keys_without_touching_vlog() -> TestResult {
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;
    let mut seed = WriteBatch::new();
    seed.put(b"present-a", b"value-a")?;
    seed.put(b"present-b", b"value-b")?;
    seed.put(b"present-c", b"value-c")?;
    seed.put(b"survivor", b"keep-value")?;
    harness.write(&seed, true)?;

    let seed_descriptor = harness.descriptor(1)?;
    let before_states = seed_descriptor
        .mutations
        .iter()
        .map(|mutation| (mutation.user_key.clone(), mutation.after_state.clone()))
        .collect::<BTreeMap<_, _>>();
    for key in [b"present-a".as_slice(), b"present-b", b"present-c"] {
        assert!(matches!(
            before_states.get(key),
            Some(ValueState::Present(_))
        ));
    }
    let inventory_before = harness.vlog_inventory()?;
    let state_before = harness.coordinator.state_snapshot();
    let stats_before = harness.stats.snapshot();

    let mut deletes = WriteBatch::new();
    deletes.delete(b"present-a")?;
    deletes.delete(b"missing")?;
    deletes.delete(b"present-b")?;
    deletes.delete(b"present-a")?;
    deletes.delete(b"present-c")?;
    assert_zero_vlog_calls(|| harness.write(&deletes, true))?;

    let descriptor = harness.descriptor(2)?;
    assert_index_only_descriptor(
        &descriptor,
        5,
        &[b"present-a", b"missing", b"present-b", b"present-c"],
    );
    for mutation in &descriptor.mutations {
        let expected_before = before_states
            .get(mutation.user_key.as_slice())
            .cloned()
            .unwrap_or(ValueState::Absent);
        assert_eq!(mutation.before_state, expected_before);
        assert_eq!(mutation.after_state, ValueState::Absent);
    }
    for key in [
        b"present-a".as_slice(),
        b"present-b",
        b"present-c",
        b"missing",
    ] {
        assert!(harness.backend.get_user(key, None)?.is_none());
    }
    assert_eq!(harness.vlog_inventory()?, inventory_before);

    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 2);
    assert_eq!(state.durable_seq, 2);
    assert_eq!(state.head_vlog_seq, 1);
    assert_eq!(state.durable_vlog_seq, 1);
    assert_eq!(state.head_vlog_end, state_before.head_vlog_end);
    assert_eq!(state.durable_vlog_end, state_before.durable_vlog_end);
    let stats = harness.stats.snapshot();
    assert_eq!(stats.active_vlog_file_id, stats_before.active_vlog_file_id);
    assert_eq!(stats.vlog_file_count, stats_before.vlog_file_count);
    assert_eq!(stats.vlog_logical_bytes, stats_before.vlog_logical_bytes);

    reset_probes();
    let (_files, reader) = harness.reader(2)?;
    let survivor_pointer = harness
        .backend
        .get_user(b"survivor", None)?
        .expect("survivor pointer");
    assert_eq!(
        reader.read_value(&survivor_pointer, b"survivor")?,
        b"keep-value"
    );
    assert_eq!(
        probe_snapshot(),
        ProbeSnapshot {
            value_dereference: 1,
            ..ProbeSnapshot::default()
        }
    );
    Ok(())
}

#[test]
fn pure_delete_empty_sync_barrier_advances_only_the_logical_frontier_without_vlog_calls()
-> TestResult {
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;
    assert_zero_vlog_calls(|| harness.delete(b"one", false))?;
    assert_eq!(harness.coordinator.state_snapshot().durable_seq, 0);

    assert_zero_vlog_calls(|| harness.barrier())?;
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 1);
    assert_eq!(state.durable_seq, 1);
    assert_eq!(state.head_vlog_seq, 0);
    assert_eq!(state.durable_vlog_seq, 0);
    assert_eq!(state.head_vlog_end, None);
    assert_eq!(state.durable_vlog_end, None);
    assert_eq!(
        harness.frontier()?,
        DurableFrontier {
            durable_seq: 1,
            durable_vlog_seq: 0,
            durable_vlog_end: DurableVLogEnd::Empty,
        }
    );
    assert!(harness.vlog_inventory()?.is_empty());

    assert_zero_vlog_calls(|| harness.barrier())?;
    assert_eq!(harness.transaction_entry_count()?, 2);
    Ok(())
}

#[test]
fn deletes_after_a_stable_put_preserve_every_vlog_byte_cursor_and_public_vlog_stat() -> TestResult {
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;
    let mut seed = WriteBatch::new();
    seed.put(b"target", b"old-value")?;
    seed.put(b"survivor", b"keep-value")?;
    harness.write(&seed, true)?;

    let inventory_before = harness.vlog_inventory()?;
    let state_before = harness.coordinator.state_snapshot();
    let stats_before = harness.stats.snapshot();

    assert_zero_vlog_calls(|| harness.delete(b"target", false))?;
    let mut deletes = WriteBatch::new();
    deletes.delete(b"target")?;
    deletes.delete(b"missing")?;
    deletes.delete(b"target")?;
    assert_zero_vlog_calls(|| harness.write(&deletes, false))?;
    assert_zero_vlog_calls(|| harness.delete(b"another-missing", true))?;

    assert_eq!(harness.vlog_inventory()?, inventory_before);
    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 4);
    assert_eq!(state.durable_seq, 4);
    assert_eq!(state.head_vlog_seq, 1);
    assert_eq!(state.durable_vlog_seq, 1);
    assert_eq!(state.head_vlog_end, state_before.head_vlog_end);
    assert_eq!(state.durable_vlog_end, state_before.durable_vlog_end);
    let stats = harness.stats.snapshot();
    assert_eq!(stats.active_vlog_file_id, stats_before.active_vlog_file_id);
    assert_eq!(stats.vlog_file_count, stats_before.vlog_file_count);
    assert_eq!(stats.vlog_logical_bytes, stats_before.vlog_logical_bytes);
    assert_eq!(
        stats
            .durable_vlog_end
            .as_ref()
            .map(|end| (end.file_id, end.offset)),
        stats_before
            .durable_vlog_end
            .as_ref()
            .map(|end| (end.file_id, end.offset))
    );
    assert!(harness.backend.get_user(b"target", None)?.is_none());
    assert!(harness.backend.get_user(b"missing", None)?.is_none());

    let third = harness.descriptor(3)?;
    assert_index_only_descriptor(&third, 3, &[b"target", b"missing"]);
    reset_probes();
    let (_files, reader) = harness.reader(2)?;
    let survivor_pointer = harness
        .backend
        .get_user(b"survivor", None)?
        .expect("survivor pointer");
    assert_eq!(
        reader.read_value(&survivor_pointer, b"survivor")?,
        b"keep-value"
    );
    let read_calls = probe_snapshot();
    assert_eq!(read_calls.value_dereference, 1);
    assert_eq!(
        read_calls,
        ProbeSnapshot {
            value_dereference: 1,
            ..ProbeSnapshot::default()
        }
    );
    Ok(())
}

#[test]
fn sync_delete_flushes_the_dirty_put_prefix_but_appends_no_delete_bytes() -> TestResult {
    let harness = Harness::new(VLogGeometry::PRODUCTION)?;
    harness.put(b"a", b"a1", false)?;
    harness.put(b"b", b"b1", false)?;
    harness.put(b"c", b"c1", false)?;
    let inventory_before = harness.vlog_inventory()?;

    reset_probes();
    harness.delete(b"b", true)?;
    let calls = probe_snapshot();
    assert_eq!(calls.layout_plans, 0);
    assert_eq!(calls.append, 0);
    assert_eq!(calls.file_create, 0);
    assert_eq!(calls.file_roll, 0);
    assert_eq!(calls.value_dereference, 0);
    assert!(calls.file_sync >= 1);
    assert!(calls.directory_sync >= 1);
    assert_eq!(harness.vlog_inventory()?, inventory_before);

    let state = harness.coordinator.state_snapshot();
    assert_eq!(state.head_seq, 4);
    assert_eq!(state.durable_seq, 4);
    assert_eq!(state.head_vlog_seq, 3);
    assert_eq!(state.durable_vlog_seq, 3);
    assert_eq!(state.head_vlog_end, state.durable_vlog_end);
    let frontier = harness.frontier()?;
    assert_eq!(frontier.durable_seq, 4);
    assert_eq!(frontier.durable_vlog_seq, 3);
    assert!(matches!(
        frontier.durable_vlog_end,
        DurableVLogEnd::Position(_)
    ));
    assert!(harness.backend.get_user(b"b", None)?.is_none());
    Ok(())
}

#[test]
fn put_positive_control_exercises_every_probe_including_file_roll_and_reader() -> TestResult {
    let geometry = VLogGeometry::test_only(256, 512, 5)?;
    let harness = Harness::new(geometry)?;
    let mut batch = WriteBatch::new();
    batch.put(b"roll-a", b"1")?;
    batch.put(b"roll-b", b"2")?;
    batch.put(b"roll-c", b"3")?;
    batch.put(b"roll-d", b"4")?;
    batch.put(b"roll-e", b"5")?;
    batch.put(b"roll-f", b"6")?;
    batch.put(b"roll-g", b"7")?;

    reset_probes();
    harness.write(&batch, true)?;
    let write_calls = probe_snapshot();
    assert!(write_calls.layout_plans >= 1);
    assert_eq!(write_calls.append, 1);
    assert_eq!(write_calls.file_create, 1);
    assert!(write_calls.file_roll >= 1);
    assert_eq!(write_calls.value_dereference, 0);
    assert!(write_calls.file_sync >= 2);
    assert_eq!(write_calls.directory_sync, 1);
    assert!(harness.vlog_inventory()?.len() >= 2);

    reset_probes();
    let (_files, reader) = harness.reader(1)?;
    let pointer = harness
        .backend
        .get_user(b"roll-g", None)?
        .expect("roll-g pointer");
    assert_eq!(reader.read_value(&pointer, b"roll-g")?, b"7");
    assert_eq!(
        probe_snapshot(),
        ProbeSnapshot {
            value_dereference: 1,
            ..ProbeSnapshot::default()
        }
    );
    Ok(())
}

fn empty_frontier() -> DurableFrontier {
    DurableFrontier {
        durable_seq: 0,
        durable_vlog_seq: 0,
        durable_vlog_end: DurableVLogEnd::Empty,
    }
}

fn fjall_options() -> FjallIndexOptions {
    FjallIndexOptions {
        write_buffer_size: 4 * 1024 * 1024,
        max_open_files: 1000,
        block_cache_size: 8 * 1024 * 1024,
        block_size: 4 * 1024,
        block_restart_interval: 16,
        max_file_size: 2 * 1024 * 1024,
        compression: IndexCompression::None,
    }
}
