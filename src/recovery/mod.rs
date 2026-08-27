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
    DURABLE_FRONTIER_KEY, DatabaseIdentityV0, HEAD_SEQ_KEY, IndexBackend, IndexEntry,
    InternalIndexSpace, InternalKeyRange,
};
use crate::vlog::format::{VLogGeometry, VLogPosition};
use crate::vlog::reader::{EnvelopeValueState, RecoveryEnvelope, ValueLogReader};
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

mod topology;
mod undo;

pub(crate) use topology::PhysicalTail;
use topology::RecoveryTopology;

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
