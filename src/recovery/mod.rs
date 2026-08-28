//! Open-time compound recovery orchestration.
#![allow(dead_code)] // Stage 12 analysis is wired into Open by stage 13.

use crate::commit::{
    DurableFrontier, DurableVLogEnd, RECOVERY_STATE_KEY, RecoveryPhase, RecoveryState,
    TransactionDescriptor, VLogPos, ValueState, decode_descriptor, decode_head_seq,
    decode_tx_meta_key, decode_tx_mutation_key, encode_tx_meta_key,
};
use crate::db::ManagedInventory;
use crate::format::FormatMetadataV0;
use crate::index::{
    DURABLE_FRONTIER_KEY, DatabaseIdentityV0, HEAD_SEQ_KEY, IndexApplyState, IndexAtomicBatch,
    IndexBackend, IndexCommitError, IndexCommitMode, IndexEntry, IndexMutation, InternalIndexError,
    InternalIndexSpace, InternalKeyRange,
};
use crate::lock::RootLock;
use crate::vlog::file_set::VLogDirectory;
use crate::vlog::format::{VLogGeometry, VLogPosition};
use crate::vlog::reader::{EnvelopeValueState, RecoveryEnvelope, ValueLogReader};
use crate::vlog::writer::{ValueLogRecovery, ValueLogWriter};
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

mod topology;
mod undo;

pub(crate) use topology::PhysicalTail;
use topology::RecoveryTopology;
#[cfg(test)]
pub(crate) fn fail_next_inventory_inspect_for_test() {
    topology::fail_next_inventory_inspect_for_test();
}
use undo::undo_transactions;

const VLOG_DIRECTORY_NAME: &str = "vlog";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryPlan {
    pub(crate) database_identity: Vec<u8>,
    pub(crate) durable_frontier: DurableFrontier,
    pub(crate) head_seq: u64,
    pub(crate) accepted_seq: u64,
    pub(crate) published_end: DurableVLogEnd,
    pub(crate) accepted_end: DurableVLogEnd,
    pub(crate) physical_tail: PhysicalTail,
    pub(crate) descriptors: Vec<TransactionDescriptor>,
    pub(crate) recovery_state: Option<RecoveryState>,
    pub(crate) needs_undo: bool,
    pub(crate) needs_promote: bool,
    pub(crate) needs_trim: bool,
}

pub(crate) struct RecoveredState {
    pub(crate) head_seq: u64,
    pub(crate) durable_frontier: DurableFrontier,
    pub(crate) writer: ValueLogWriter,
}

pub(crate) fn execute_recovery<B: IndexBackend>(
    backend: &B,
    plan: RecoveryPlan,
    root: &RootLock,
    format: &FormatMetadataV0,
    reader: &ValueLogReader,
    vlog: ValueLogRecovery,
) -> Result<RecoveredState> {
    execute_recovery_with_policy(
        backend,
        plan,
        root,
        format,
        reader,
        vlog,
        RecoveryGeometry::Production,
    )
}

#[cfg(test)]
pub(crate) fn execute_recovery_with_test_geometry<B: IndexBackend>(
    backend: &B,
    plan: RecoveryPlan,
    root: &RootLock,
    format: &FormatMetadataV0,
    reader: &ValueLogReader,
    vlog: ValueLogRecovery,
) -> Result<RecoveredState> {
    let geometry = vlog.geometry();
    execute_recovery_with_policy(
        backend,
        plan,
        root,
        format,
        reader,
        vlog,
        RecoveryGeometry::Test(geometry),
    )
}

fn execute_recovery_with_policy<B: IndexBackend>(
    backend: &B,
    plan: RecoveryPlan,
    root: &RootLock,
    format: &FormatMetadataV0,
    reader: &ValueLogReader,
    vlog: ValueLogRecovery,
    geometry_policy: RecoveryGeometry,
) -> Result<RecoveredState> {
    validate_recovery_bindings(backend, &plan, root, format, reader, &vlog, geometry_policy)?;

    let mut current_head = plan.head_seq;
    let mut current_frontier = plan.durable_frontier;
    let mut recovery_state = plan.recovery_state;

    if recovery_state.is_none() && (plan.needs_undo || plan.needs_trim) {
        let state = RecoveryState {
            phase: RecoveryPhase::Undo,
            original_head: plan.head_seq,
            target_seq: plan.accepted_seq,
            target_vlog_end: plan.accepted_end,
            next_undo_seq: plan.head_seq,
            trim_required: plan.needs_trim,
        };
        commit_recovery_batch(backend, recovery_state_batch(state)?)?;
        recovery_state = Some(state);
    }

    if let Some(state) = recovery_state
        && state.phase == RecoveryPhase::Undo
    {
        let undone = undo_transactions(backend, &plan, state)?;
        let next_phase = if undone.trim_required {
            RecoveryPhase::Trim
        } else {
            RecoveryPhase::Finalize
        };
        let next_state = RecoveryState {
            phase: next_phase,
            next_undo_seq: undone.target_seq,
            ..undone
        };
        vlog.sync_accepted_range(
            durable_end_position(current_frontier.durable_vlog_end),
            durable_end_position(plan.accepted_end),
        )?;
        validate_accepted_boundary(reader, &plan)?;
        let target_frontier = DurableFrontier {
            durable_seq: plan.accepted_seq,
            durable_vlog_end: plan.accepted_end,
        };
        commit_recovery_batch(
            backend,
            target_frontier_batch(plan.accepted_seq, target_frontier, Some(next_state))?,
        )?;
        current_head = plan.accepted_seq;
        current_frontier = target_frontier;
        recovery_state = Some(next_state);
    } else if recovery_state.is_none() && plan.needs_promote {
        vlog.sync_accepted_range(
            durable_end_position(current_frontier.durable_vlog_end),
            durable_end_position(plan.accepted_end),
        )?;
        validate_accepted_boundary(reader, &plan)?;
        let target_frontier = DurableFrontier {
            durable_seq: plan.accepted_seq,
            durable_vlog_end: plan.accepted_end,
        };
        commit_recovery_batch(
            backend,
            target_frontier_batch(plan.accepted_seq, target_frontier, None)?,
        )?;
        current_head = plan.accepted_seq;
        current_frontier = target_frontier;
    }

    if recovery_state.is_some_and(|state| state.phase == RecoveryPhase::Trim) {
        let before_trim = ManagedInventory::inspect(root, format).map_err(recovery_context)?;
        let file_ids = owned_inventory_file_ids(&before_trim)?;
        vlog.trim(durable_end_position(plan.accepted_end), &file_ids)?;
        let after_trim = ManagedInventory::inspect(root, format).map_err(recovery_context)?;
        let topology = analyze_topology(&after_trim, plan.accepted_end, geometry_policy)?;
        if !topology.physical_tail_matches(plan.accepted_end) {
            return Err(recovery_corruption());
        }
        commit_recovery_batch(backend, delete_recovery_state_batch()?)?;
        recovery_state = None;
    } else if recovery_state.is_some_and(|state| state.phase == RecoveryPhase::Finalize) {
        commit_recovery_batch(backend, delete_recovery_state_batch()?)?;
        recovery_state = None;
    }

    if recovery_state.is_some() {
        return Err(recovery_corruption());
    }
    verify_final_state(
        backend,
        &plan,
        root,
        format,
        current_head,
        current_frontier,
        geometry_policy,
    )?;
    let writer = vlog.into_writer(durable_end_position(current_frontier.durable_vlog_end))?;
    if writer.position() != to_vlog_position(append_position(current_frontier.durable_vlog_end)) {
        return Err(recovery_corruption());
    }
    Ok(RecoveredState {
        head_seq: current_head,
        durable_frontier: current_frontier,
        writer,
    })
}

fn validate_recovery_bindings<B: IndexBackend>(
    backend: &B,
    plan: &RecoveryPlan,
    root: &RootLock,
    format: &FormatMetadataV0,
    reader: &ValueLogReader,
    vlog: &ValueLogRecovery,
    geometry_policy: RecoveryGeometry,
) -> Result<()> {
    if vlog.geometry() != geometry_policy.geometry()
        || reader.geometry() != geometry_policy.geometry()
        || reader.files().database_uuid() != format.database_uuid
        || vlog.database_uuid() != format.database_uuid
        || !vlog.shares_file_set(reader.files())
    {
        return Err(recovery_corruption());
    }

    let current_identity = backend
        .get_database_identity()
        .map_err(recovery_context)?
        .ok_or_else(recovery_corruption)?;
    if current_identity != plan.database_identity {
        return Err(recovery_corruption());
    }
    DatabaseIdentityV0::decode(&current_identity)
        .map_err(recovery_context)?
        .validate_against(format.format_version, format.database_uuid)
        .map_err(recovery_context)?;

    let expected_directory = VLogDirectory::open(&root.canonical_path().join(VLOG_DIRECTORY_NAME))
        .map_err(recovery_context)?;
    if expected_directory.writer_identity() != vlog.directory_identity() {
        return Err(recovery_corruption());
    }
    Ok(())
}

fn validate_accepted_boundary(reader: &ValueLogReader, plan: &RecoveryPlan) -> Result<()> {
    if plan.accepted_seq == plan.durable_frontier.durable_seq {
        return validate_stable_boundary(
            reader,
            DurableFrontier {
                durable_seq: plan.accepted_seq,
                durable_vlog_end: plan.accepted_end,
            },
        );
    }
    let relative = plan
        .accepted_seq
        .checked_sub(plan.durable_frontier.durable_seq)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(recovery_corruption)?;
    let index = usize::try_from(relative).map_err(|_| recovery_corruption())?;
    let descriptor = plan
        .descriptors
        .get(index)
        .ok_or_else(recovery_corruption)?;
    if descriptor.meta.commit_seq != plan.accepted_seq
        || DurableVLogEnd::Position(descriptor.meta.vlog_end) != plan.accepted_end
    {
        return Err(recovery_corruption());
    }
    read_and_validate_envelope(reader, descriptor)
}

fn recovery_state_batch(state: RecoveryState) -> Result<IndexAtomicBatch> {
    let mut batch = IndexAtomicBatch::try_with_capacity(1).map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(RECOVERY_STATE_KEY)?,
            value: try_copy_recovery_bytes(&state.encode().map_err(recovery_context)?)?,
        })
        .map_err(index_batch_context)?;
    Ok(batch)
}

fn target_frontier_batch(
    head_seq: u64,
    frontier: DurableFrontier,
    state: Option<RecoveryState>,
) -> Result<IndexAtomicBatch> {
    if head_seq != frontier.durable_seq {
        return Err(recovery_corruption());
    }
    let capacity = if state.is_some() { 3 } else { 2 };
    let mut batch = IndexAtomicBatch::try_with_capacity(capacity).map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(HEAD_SEQ_KEY)?,
            value: try_copy_recovery_bytes(&head_seq.to_le_bytes())?,
        })
        .map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::PutInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(DURABLE_FRONTIER_KEY)?,
            value: try_copy_recovery_bytes(&frontier.encode().map_err(recovery_context)?)?,
        })
        .map_err(index_batch_context)?;
    if let Some(state) = state {
        batch
            .try_push(IndexMutation::PutInternal {
                space: InternalIndexSpace::System,
                key: try_copy_recovery_bytes(RECOVERY_STATE_KEY)?,
                value: try_copy_recovery_bytes(&state.encode().map_err(recovery_context)?)?,
            })
            .map_err(index_batch_context)?;
    }
    Ok(batch)
}

fn delete_recovery_state_batch() -> Result<IndexAtomicBatch> {
    let mut batch = IndexAtomicBatch::try_with_capacity(1).map_err(index_batch_context)?;
    batch
        .try_push(IndexMutation::DeleteInternal {
            space: InternalIndexSpace::System,
            key: try_copy_recovery_bytes(RECOVERY_STATE_KEY)?,
        })
        .map_err(index_batch_context)?;
    Ok(batch)
}

fn commit_recovery_batch<B: IndexBackend>(backend: &B, batch: IndexAtomicBatch) -> Result<()> {
    backend
        .commit_atomic(batch, IndexCommitMode::SyncAll)
        .map_err(recovery_commit_error)
}

fn recovery_commit_error(error: IndexCommitError) -> StorageError {
    let retry_advice = match error.apply_state {
        IndexApplyState::Unknown => RetryAdvice::ReopenAndVerify,
        IndexApplyState::NotApplied => recovery_retry_advice(error.source.kind),
    };
    let mut mapped = StorageError::codec_error(
        error.source.kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        retry_advice,
    );
    mapped.os_code = error.source.os_code;
    mapped
}

fn index_batch_context(error: InternalIndexError) -> StorageError {
    let mut mapped = StorageError::codec_error(
        error.kind,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        recovery_retry_advice(error.kind),
    );
    mapped.os_code = error.os_code;
    mapped
}

fn recovery_retry_advice(kind: StorageErrorKind) -> RetryAdvice {
    match kind {
        StorageErrorKind::InvalidArgument | StorageErrorKind::IncompatibleFormat => {
            RetryAdvice::DoNotRetry
        }
        StorageErrorKind::Busy | StorageErrorKind::ResourceExhausted => {
            RetryAdvice::RetrySameInstance
        }
        StorageErrorKind::Corruption
        | StorageErrorKind::InvalidLayout
        | StorageErrorKind::Unrecoverable
        | StorageErrorKind::NotFound => RetryAdvice::RestoreOrRepair,
        StorageErrorKind::Unsupported => RetryAdvice::DoNotRetry,
        StorageErrorKind::CapacityExceeded
        | StorageErrorKind::Io
        | StorageErrorKind::StorageWriteStopped
        | StorageErrorKind::StoragePoisoned => RetryAdvice::FixEnvironmentAndReopen,
    }
}

fn verify_final_state<B: IndexBackend>(
    backend: &B,
    plan: &RecoveryPlan,
    root: &RootLock,
    format: &FormatMetadataV0,
    expected_head: u64,
    expected_frontier: DurableFrontier,
    geometry_policy: RecoveryGeometry,
) -> Result<()> {
    let identity = backend
        .get_database_identity()
        .map_err(recovery_context)?
        .ok_or_else(recovery_corruption)?;
    if identity != plan.database_identity {
        return Err(recovery_corruption());
    }
    DatabaseIdentityV0::decode(&identity)
        .map_err(recovery_context)?
        .validate_against(format.format_version, format.database_uuid)
        .map_err(recovery_context)?;

    let actual_head =
        decode_head_seq(&required_internal(backend, HEAD_SEQ_KEY)?).map_err(recovery_context)?;
    let actual_frontier =
        DurableFrontier::decode(&required_internal(backend, DURABLE_FRONTIER_KEY)?)
            .map_err(recovery_context)?;
    actual_frontier
        .validate_against_head(actual_head)
        .map_err(recovery_context)?;
    if actual_head != expected_head
        || actual_frontier != expected_frontier
        || backend
            .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)
            .map_err(recovery_context)?
            .is_some()
    {
        return Err(recovery_corruption());
    }

    let inventory = ManagedInventory::inspect(root, format).map_err(recovery_context)?;
    let topology = analyze_topology(
        &inventory,
        actual_frontier.durable_vlog_end,
        geometry_policy,
    )?;
    if !topology.physical_tail_matches(actual_frontier.durable_vlog_end) {
        return Err(recovery_corruption());
    }
    Ok(())
}

fn analyze_topology(
    inventory: &ManagedInventory,
    stable_end: DurableVLogEnd,
    geometry_policy: RecoveryGeometry,
) -> Result<RecoveryTopology> {
    match geometry_policy {
        RecoveryGeometry::Production => RecoveryTopology::analyze(inventory, stable_end),
        #[cfg(test)]
        RecoveryGeometry::Test(geometry) => {
            RecoveryTopology::analyze_with_test_geometry(inventory, stable_end, geometry)
        }
    }
}

fn owned_inventory_file_ids(inventory: &ManagedInventory) -> Result<Vec<u32>> {
    let mut file_ids = Vec::new();
    file_ids
        .try_reserve_exact(inventory.vlog_files.len())
        .map_err(|_| recovery_resource())?;
    file_ids.extend(inventory.vlog_files.iter().map(|entry| entry.file_id));
    Ok(file_ids)
}

fn durable_end_position(end: DurableVLogEnd) -> Option<VLogPosition> {
    match end {
        DurableVLogEnd::Empty => None,
        DurableVLogEnd::Position(position) => Some(to_vlog_position(position)),
    }
}

fn try_copy_recovery_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| recovery_resource())?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

pub(crate) fn analyze_recovery<B: IndexBackend>(
    backend: &B,
    format: &FormatMetadataV0,
    inventory: &ManagedInventory,
    reader: &ValueLogReader,
) -> Result<RecoveryPlan> {
    analyze_recovery_with_policy(
        backend,
        format,
        inventory,
        reader,
        RecoveryGeometry::Production,
    )
}

#[cfg(test)]
pub(crate) fn analyze_recovery_with_test_geometry<B: IndexBackend>(
    backend: &B,
    format: &FormatMetadataV0,
    inventory: &ManagedInventory,
    reader: &ValueLogReader,
) -> Result<RecoveryPlan> {
    analyze_recovery_with_policy(
        backend,
        format,
        inventory,
        reader,
        RecoveryGeometry::Test(reader.geometry()),
    )
}

#[derive(Clone, Copy)]
enum RecoveryGeometry {
    Production,
    #[cfg(test)]
    Test(VLogGeometry),
}

impl RecoveryGeometry {
    fn geometry(self) -> VLogGeometry {
        match self {
            Self::Production => VLogGeometry::PRODUCTION,
            #[cfg(test)]
            Self::Test(geometry) => geometry,
        }
    }
}

fn analyze_recovery_with_policy<B: IndexBackend>(
    backend: &B,
    format: &FormatMetadataV0,
    inventory: &ManagedInventory,
    reader: &ValueLogReader,
    geometry_policy: RecoveryGeometry,
) -> Result<RecoveryPlan> {
    let encoded_identity = backend
        .get_database_identity()
        .map_err(recovery_context)?
        .ok_or_else(recovery_corruption)?;
    DatabaseIdentityV0::decode(&encoded_identity)
        .map_err(recovery_context)?
        .validate_against(format.format_version, format.database_uuid)
        .map_err(recovery_context)?;

    let recovery_geometry = geometry_policy.geometry();
    if reader.geometry() != recovery_geometry {
        return Err(recovery_corruption());
    }

    let encoded_frontier = required_internal(backend, DURABLE_FRONTIER_KEY)?;
    let durable_frontier = DurableFrontier::decode(&encoded_frontier).map_err(recovery_context)?;
    let encoded_head = required_internal(backend, HEAD_SEQ_KEY)?;
    let head_seq = decode_head_seq(&encoded_head).map_err(recovery_context)?;
    durable_frontier
        .validate_against_head(head_seq)
        .map_err(recovery_context)?;
    let encoded_recovery_state = backend
        .get_internal(InternalIndexSpace::System, RECOVERY_STATE_KEY)
        .map_err(recovery_context)?;
    let recovery_state = encoded_recovery_state
        .as_deref()
        .map(RecoveryState::decode)
        .transpose()
        .map_err(recovery_context)?;

    let topology = match geometry_policy {
        RecoveryGeometry::Production => {
            RecoveryTopology::analyze(inventory, durable_frontier.durable_vlog_end)?
        }
        #[cfg(test)]
        RecoveryGeometry::Test(geometry) => RecoveryTopology::analyze_with_test_geometry(
            inventory,
            durable_frontier.durable_vlog_end,
            geometry,
        )?,
    };
    validate_stable_boundary(reader, durable_frontier)?;
    let descriptors = load_unstable_descriptors(
        backend,
        durable_frontier.durable_seq,
        head_seq,
        durable_frontier.durable_vlog_end,
    )?;

    match recovery_state {
        Some(state) => analyze_fixed_target(
            encoded_identity,
            durable_frontier,
            head_seq,
            descriptors,
            state,
            inventory,
            topology,
            reader,
        ),
        None => analyze_new_target(
            encoded_identity,
            durable_frontier,
            head_seq,
            descriptors,
            inventory,
            topology,
            reader,
        ),
    }
}

fn required_internal<B: IndexBackend>(backend: &B, key: &[u8]) -> Result<Vec<u8>> {
    backend
        .get_internal(InternalIndexSpace::System, key)
        .map_err(recovery_context)?
        .ok_or_else(recovery_corruption)
}

fn validate_stable_boundary(reader: &ValueLogReader, frontier: DurableFrontier) -> Result<()> {
    let DurableVLogEnd::Position(end) = frontier.durable_vlog_end else {
        if frontier.durable_seq != 0 {
            return Err(recovery_corruption());
        }
        return Ok(());
    };
    if frontier.durable_seq == 0 {
        return Err(recovery_corruption());
    }
    let envelope = reader
        .read_stable_envelope_from_end(to_vlog_position(end))
        .map_err(recovery_context)?;
    if envelope.scanned.commit_seq != frontier.durable_seq
        || envelope.scanned.vlog_end != to_vlog_position(end)
    {
        return Err(recovery_corruption());
    }
    Ok(())
}

#[derive(Default)]
struct EncodedDescriptorGroup {
    meta: Option<IndexEntry>,
    mutations: Vec<IndexEntry>,
}

fn load_unstable_descriptors<B: IndexBackend>(
    backend: &B,
    durable_seq: u64,
    head_seq: u64,
    durable_end: DurableVLogEnd,
) -> Result<Vec<TransactionDescriptor>> {
    let count_u64 = head_seq
        .checked_sub(durable_seq)
        .ok_or_else(recovery_corruption)?;
    let count = usize::try_from(count_u64).map_err(|_| recovery_resource())?;
    let mut groups = Vec::new();
    groups
        .try_reserve_exact(count)
        .map_err(|_| recovery_resource())?;
    groups.resize_with(count, EncodedDescriptorGroup::default);

    let Some(descriptor_range) = unstable_descriptor_range(durable_seq, head_seq)? else {
        return Ok(Vec::new());
    };
    let entries = backend
        .scan_internal(InternalIndexSpace::Transaction, descriptor_range)
        .map_err(recovery_context)?;
    for entry in entries {
        let entry = entry.map_err(recovery_context)?;
        let (commit_seq, is_meta) = decode_descriptor_key(&entry.key)?;
        if commit_seq > head_seq {
            return Err(recovery_corruption());
        }
        if commit_seq <= durable_seq {
            continue;
        }
        let relative = commit_seq
            .checked_sub(durable_seq)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(recovery_corruption)?;
        let index = usize::try_from(relative).map_err(|_| recovery_corruption())?;
        let group = groups.get_mut(index).ok_or_else(recovery_corruption)?;
        if is_meta {
            if group.meta.replace(entry).is_some() {
                return Err(recovery_corruption());
            }
        } else {
            group
                .mutations
                .try_reserve(1)
                .map_err(|_| recovery_resource())?;
            group.mutations.push(entry);
        }
    }

    let mut descriptors = Vec::new();
    descriptors
        .try_reserve_exact(count)
        .map_err(|_| recovery_resource())?;
    for (index, group) in groups.into_iter().enumerate() {
        let expected_seq = durable_seq
            .checked_add(u64::try_from(index).map_err(|_| recovery_corruption())?)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(recovery_corruption)?;
        let meta = group.meta.ok_or_else(recovery_corruption)?;
        let mut mutation_refs = Vec::new();
        mutation_refs
            .try_reserve_exact(group.mutations.len())
            .map_err(|_| recovery_resource())?;
        mutation_refs.extend(
            group
                .mutations
                .iter()
                .map(|entry| (entry.key.as_slice(), entry.value.as_slice())),
        );
        let descriptor =
            decode_descriptor(&meta.key, &meta.value, &mutation_refs).map_err(recovery_context)?;
        if descriptor.meta.commit_seq != expected_seq {
            return Err(recovery_corruption());
        }
        descriptors.push(descriptor);
    }
    validate_descriptor_chain(durable_end, &descriptors)?;
    Ok(descriptors)
}

pub(crate) fn unstable_descriptor_range(
    durable_seq: u64,
    head_seq: u64,
) -> Result<Option<InternalKeyRange>> {
    if durable_seq > head_seq {
        return Err(recovery_corruption());
    }
    if durable_seq == u64::MAX {
        return Ok(None);
    }
    let start_seq = durable_seq.checked_add(1).ok_or_else(recovery_corruption)?;
    Ok(Some(InternalKeyRange {
        // Start at the sequence prefix rather than the canonical TxMeta key so
        // a malformed first key cannot sort immediately before the lower bound.
        // Keep the upper bound open: every canonical entry after HeadSeq must
        // be observed and rejected by `load_unstable_descriptors`.
        start_inclusive: Some(owned_tx_sequence_prefix(start_seq)?),
        end_exclusive: None,
    }))
}

fn owned_tx_sequence_prefix(commit_seq: u64) -> Result<Vec<u8>> {
    let encoded = encode_tx_meta_key(commit_seq).map_err(recovery_context)?;
    let prefix = encoded.get(..10).ok_or_else(recovery_corruption)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(prefix.len())
        .map_err(|_| recovery_resource())?;
    owned.extend_from_slice(prefix);
    Ok(owned)
}

fn decode_descriptor_key(key: &[u8]) -> Result<(u64, bool)> {
    match key.len() {
        11 => decode_tx_meta_key(key)
            .map(|commit_seq| (commit_seq, true))
            .map_err(recovery_context),
        19 => decode_tx_mutation_key(key)
            .map(|(commit_seq, _)| (commit_seq, false))
            .map_err(recovery_context),
        _ => Err(recovery_corruption()),
    }
}

fn validate_descriptor_chain(
    durable_end: DurableVLogEnd,
    descriptors: &[TransactionDescriptor],
) -> Result<()> {
    let mut expected_begin = append_position(durable_end);
    for descriptor in descriptors {
        if descriptor.meta.vlog_begin != expected_begin {
            return Err(recovery_corruption());
        }
        expected_begin = descriptor.meta.vlog_end;
    }
    Ok(())
}

fn analyze_new_target(
    database_identity: Vec<u8>,
    durable_frontier: DurableFrontier,
    head_seq: u64,
    descriptors: Vec<TransactionDescriptor>,
    inventory: &ManagedInventory,
    topology: RecoveryTopology,
    reader: &ValueLogReader,
) -> Result<RecoveryPlan> {
    let mut accepted_seq = durable_frontier.durable_seq;
    for descriptor in &descriptors {
        match read_and_validate_envelope(reader, descriptor) {
            Ok(()) => accepted_seq = descriptor.meta.commit_seq,
            Err(error) if is_rejectable_envelope_error(error.kind) => break,
            Err(error) => return Err(error),
        }
    }
    build_plan(
        database_identity,
        durable_frontier,
        head_seq,
        accepted_seq,
        descriptors,
        None,
        inventory,
        topology,
    )
}

fn analyze_fixed_target(
    database_identity: Vec<u8>,
    durable_frontier: DurableFrontier,
    head_seq: u64,
    descriptors: Vec<TransactionDescriptor>,
    state: RecoveryState,
    inventory: &ManagedInventory,
    topology: RecoveryTopology,
    reader: &ValueLogReader,
) -> Result<RecoveryPlan> {
    validate_recovery_state_against_current(state, durable_frontier, head_seq)?;
    if state.phase == RecoveryPhase::Undo && state.target_seq > durable_frontier.durable_seq {
        let accepted_count = usize::try_from(
            state
                .target_seq
                .checked_sub(durable_frontier.durable_seq)
                .ok_or_else(recovery_corruption)?,
        )
        .map_err(|_| recovery_corruption())?;
        let accepted = descriptors
            .get(..accepted_count)
            .ok_or_else(recovery_corruption)?;
        for descriptor in accepted {
            match read_and_validate_envelope(reader, descriptor) {
                Ok(()) => {}
                Err(error) if is_rejectable_envelope_error(error.kind) => {
                    return Err(recovery_corruption());
                }
                Err(error) => return Err(error),
            }
        }
    }
    let mut plan = build_plan(
        database_identity,
        durable_frontier,
        head_seq,
        state.target_seq,
        descriptors,
        Some(state),
        inventory,
        topology,
    )?;
    match state.phase {
        RecoveryPhase::Undo => {
            // `trim_required` freezes the recovery decision made before the
            // state was persisted. A later crash may lose an unsynced rejected
            // suffix, so true -> false is a valid idempotent-Trim state. The
            // reverse transition would introduce an unexplained new suffix.
            if !state.trim_required && plan.needs_trim {
                return Err(recovery_corruption());
            }
        }
        RecoveryPhase::Trim => {
            if !state.trim_required {
                return Err(recovery_corruption());
            }
        }
        RecoveryPhase::Finalize => {
            if state.trim_required || plan.needs_trim {
                return Err(recovery_corruption());
            }
        }
    }
    plan.needs_undo = state.phase == RecoveryPhase::Undo && head_seq > state.target_seq;
    Ok(plan)
}

fn validate_recovery_state_against_current(
    state: RecoveryState,
    frontier: DurableFrontier,
    head_seq: u64,
) -> Result<()> {
    if state.target_seq < frontier.durable_seq {
        return Err(recovery_corruption());
    }
    match state.phase {
        RecoveryPhase::Undo => {
            if head_seq != state.next_undo_seq {
                return Err(recovery_corruption());
            }
        }
        RecoveryPhase::Trim | RecoveryPhase::Finalize => {
            if head_seq != state.target_seq
                || frontier.durable_seq != state.target_seq
                || frontier.durable_vlog_end != state.target_vlog_end
                || state.next_undo_seq != state.target_seq
            {
                return Err(recovery_corruption());
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_plan(
    database_identity: Vec<u8>,
    durable_frontier: DurableFrontier,
    head_seq: u64,
    accepted_seq: u64,
    descriptors: Vec<TransactionDescriptor>,
    recovery_state: Option<RecoveryState>,
    inventory: &ManagedInventory,
    topology: RecoveryTopology,
) -> Result<RecoveryPlan> {
    if accepted_seq < durable_frontier.durable_seq || accepted_seq > head_seq {
        return Err(recovery_corruption());
    }
    let published_end = descriptor_end_for_seq(
        durable_frontier,
        head_seq,
        durable_frontier.durable_seq,
        &descriptors,
    )?;
    let accepted_end = descriptor_end_for_seq(
        durable_frontier,
        accepted_seq,
        durable_frontier.durable_seq,
        &descriptors,
    )?;
    if recovery_state.is_some_and(|state| state.target_vlog_end != accepted_end)
        || !topology.contains_end(inventory, accepted_end)
    {
        return Err(recovery_corruption());
    }
    let needs_trim = topology.has_suffix_after(inventory, accepted_end);
    Ok(RecoveryPlan {
        database_identity,
        durable_frontier,
        head_seq,
        accepted_seq,
        published_end,
        accepted_end,
        physical_tail: topology.physical_tail,
        descriptors,
        recovery_state,
        needs_undo: accepted_seq < head_seq,
        needs_promote: accepted_seq > durable_frontier.durable_seq,
        needs_trim,
    })
}

fn descriptor_end_for_seq(
    frontier: DurableFrontier,
    seq: u64,
    durable_seq: u64,
    descriptors: &[TransactionDescriptor],
) -> Result<DurableVLogEnd> {
    if seq == durable_seq {
        return Ok(frontier.durable_vlog_end);
    }
    let relative = seq
        .checked_sub(durable_seq)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(recovery_corruption)?;
    let index = usize::try_from(relative).map_err(|_| recovery_corruption())?;
    descriptors
        .get(index)
        .map(|descriptor| DurableVLogEnd::Position(descriptor.meta.vlog_end))
        .ok_or_else(recovery_corruption)
}

fn read_and_validate_envelope(
    reader: &ValueLogReader,
    descriptor: &TransactionDescriptor,
) -> Result<()> {
    let envelope = reader.read_recovery_envelope(
        to_vlog_position(descriptor.meta.vlog_begin),
        to_vlog_position(descriptor.meta.vlog_end),
        Some(descriptor.meta.envelope_crc32c),
    )?;
    validate_envelope_against_descriptor(&envelope, descriptor)
}

fn validate_envelope_against_descriptor(
    envelope: &RecoveryEnvelope,
    descriptor: &TransactionDescriptor,
) -> Result<()> {
    let scanned = envelope.scanned;
    if scanned.commit_seq != descriptor.meta.commit_seq
        || scanned.tx_uuid != descriptor.meta.tx_uuid.0
        || scanned.vlog_begin != to_vlog_position(descriptor.meta.vlog_begin)
        || scanned.vlog_end != to_vlog_position(descriptor.meta.vlog_end)
        || scanned.logical_op_count != descriptor.meta.logical_op_count
        || scanned.distinct_key_count != descriptor.meta.distinct_key_count
        || scanned.envelope_crc32c != descriptor.meta.envelope_crc32c
        || envelope.final_states.len() != descriptor.mutations.len()
    {
        return Err(recovery_corruption());
    }
    for (actual, expected) in envelope.final_states.iter().zip(&descriptor.mutations) {
        if actual.user_key != expected.user_key
            || !envelope_state_matches(actual.state, expected.after_state)
        {
            return Err(recovery_corruption());
        }
    }
    Ok(())
}

fn envelope_state_matches(actual: EnvelopeValueState, expected: ValueState) -> bool {
    matches!(
        (actual, expected),
        (EnvelopeValueState::Absent, ValueState::Absent)
    ) || matches!(
        (actual, expected),
        (
            EnvelopeValueState::Present(actual),
            ValueState::Present(expected)
        ) if actual == expected
    )
}

fn append_position(end: DurableVLogEnd) -> VLogPos {
    match end {
        DurableVLogEnd::Empty => VLogPos {
            file_id: 0,
            offset: 0,
        },
        DurableVLogEnd::Position(position) => position,
    }
}

fn to_vlog_position(position: VLogPos) -> VLogPosition {
    VLogPosition {
        file_id: position.file_id,
        offset: position.offset,
    }
}

fn is_rejectable_envelope_error(kind: StorageErrorKind) -> bool {
    matches!(
        kind,
        StorageErrorKind::Corruption
            | StorageErrorKind::InvalidLayout
            | StorageErrorKind::IncompatibleFormat
    )
}

fn recovery_context(mut error: StorageError) -> StorageError {
    error.operation = Operation::Open;
    error.protocol_stage = ProtocolStage::Recovery;
    error.write_outcome = None;
    error.instance_state = None;
    error
}

fn recovery_corruption() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::Corruption,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::RestoreOrRepair,
    )
}

fn recovery_resource() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::RetrySameInstance,
    )
}
