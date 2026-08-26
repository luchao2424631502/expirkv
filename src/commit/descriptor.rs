//! Transaction descriptor and system-metadata encoding and decoding.
#![allow(dead_code)] // Stage 2 codecs; production consumers are wired in later stages.

use std::collections::HashSet;

use crc32c::{crc32c, crc32c_append};

use crate::vlog::format::ValuePointer;
use crate::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

pub(crate) type CommitSeq = u64;

pub(crate) const DATABASE_IDENTITY_KEY: &[u8] = b"database_identity";
pub(crate) const HEAD_SEQ_KEY: &[u8] = b"head_seq";
pub(crate) const DURABLE_FRONTIER_KEY: &[u8] = b"durable_frontier";
pub(crate) const RECOVERY_STATE_KEY: &[u8] = b"recovery_state";

pub(crate) const TX_META_KEY_ENCODED_LEN: usize = 11;
pub(crate) const TX_MUTATION_KEY_ENCODED_LEN: usize = 19;
pub(crate) const TX_META_ENCODED_LEN: usize = 86;
pub(crate) const DATABASE_IDENTITY_ENCODED_LEN: usize = 32;
pub(crate) const HEAD_SEQ_ENCODED_LEN: usize = 8;
pub(crate) const DURABLE_FRONTIER_ENCODED_LEN: usize = 31;
pub(crate) const RECOVERY_STATE_ENCODED_LEN: usize = 49;

const TX_KEY_MAGIC: [u8; 2] = *b"TX";
const TX_META_KIND: u8 = 0;
const TX_MUTATION_KIND: u8 = 1;
const TX_META_MAGIC: [u8; 4] = *b"RKTM";
const DESCRIPTOR_CRC_MAGIC: &[u8] = b"RKDESC0";
const DATABASE_IDENTITY_MAGIC: [u8; 4] = *b"RKDI";
const DURABLE_FRONTIER_MAGIC: [u8; 4] = *b"RKDF";
const RECOVERY_STATE_MAGIC: [u8; 4] = *b"RKRS";
const FORMAT_VERSION: u16 = 0;
const DATABASE_FORMAT_VERSION: u32 = 0;
const KEYSPACE_LAYOUT_VERSION: u16 = 0;
const MAX_VLOG_FILE_ID: u32 = 999_999;
const MAX_VLOG_OFFSET: u64 = 1_u64 << 32;
const MAX_USER_KEY_LEN: usize = 60_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TxUuid(pub(crate) [u8; 16]);

impl TxUuid {
    fn is_valid(self) -> bool {
        self.0 != [0; 16]
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct VLogPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

impl VLogPos {
    pub(crate) fn encode(&self) -> Result<[u8; 12]> {
        if !self.is_valid() {
            return Err(encode_invalid());
        }
        let mut encoded = [0_u8; 12];
        encoded[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[4..12].copy_from_slice(&self.offset.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != 12 {
            return Err(decode_corruption());
        }
        let position = Self {
            file_id: read_u32_le(encoded, 0).ok_or_else(decode_corruption)?,
            offset: read_u64_le(encoded, 4).ok_or_else(decode_corruption)?,
        };
        if !position.is_valid() {
            return Err(decode_corruption());
        }
        Ok(position)
    }

    fn is_valid(self) -> bool {
        self.file_id <= MAX_VLOG_FILE_ID && self.offset <= MAX_VLOG_OFFSET
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DurableVLogEnd {
    Empty,
    Position(VLogPos),
}

// 事务 head结构
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TxMeta {
    pub(crate) commit_seq: CommitSeq,
    pub(crate) tx_uuid: TxUuid,
    pub(crate) prev_seq: CommitSeq,
    pub(crate) vlog_begin: VLogPos,
    pub(crate) vlog_end: VLogPos,
    pub(crate) logical_op_count: u64,
    pub(crate) distinct_key_count: u64,
    pub(crate) envelope_crc32c: u32,
    pub(crate) descriptor_crc32c: u32,
}

// 事务 body结构
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TxMutation {
    pub(crate) user_key: Vec<u8>,
    pub(crate) before_state: ValueState,
    pub(crate) after_state: ValueState,
}

// 值状态记录结构
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValueState {
    Absent,
    Present(ValuePointer),
}

// 一次事务描述
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransactionDescriptor {
    pub(crate) meta: TxMeta,
    pub(crate) mutations: Vec<TxMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedMutation {
    pub(crate) key: [u8; TX_MUTATION_KEY_ENCODED_LEN],
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EncodedDescriptor {
    pub(crate) meta_key: [u8; TX_META_KEY_ENCODED_LEN], // meta_key -> TxDescriptor(TxMeta+TxMutation)
    pub(crate) meta_value: [u8; TX_META_ENCODED_LEN],   // TxMeta
    pub(crate) mutations: Vec<EncodedMutation>,         // TxMutation
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DatabaseIdentity {
    pub(crate) identity_format_version: u16,
    pub(crate) database_format_version: u32,
    pub(crate) database_uuid: [u8; 16],
    pub(crate) keyspace_layout_version: u16,
}

// 事务记录 全局持久化点
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DurableFrontier {
    pub(crate) durable_seq: CommitSeq,
    pub(crate) durable_vlog_end: DurableVLogEnd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryPhase {
    Undo,
    Trim,
    Finalize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryState {
    pub(crate) phase: RecoveryPhase,
    pub(crate) original_head: CommitSeq,
    pub(crate) target_seq: CommitSeq,
    pub(crate) target_vlog_end: DurableVLogEnd,
    pub(crate) next_undo_seq: CommitSeq,
    pub(crate) trim_required: bool,
}

pub(crate) fn encode_tx_meta_key(commit_seq: CommitSeq) -> Result<[u8; TX_META_KEY_ENCODED_LEN]> {
    if commit_seq == 0 {
        return Err(encode_invalid());
    }
    let mut encoded = [0_u8; TX_META_KEY_ENCODED_LEN];
    encoded[0..2].copy_from_slice(&TX_KEY_MAGIC);
    encoded[2..10].copy_from_slice(&commit_seq.to_be_bytes());
    encoded[10] = TX_META_KIND;
    Ok(encoded)
}

pub(crate) fn decode_tx_meta_key(encoded: &[u8]) -> Result<CommitSeq> {
    if encoded.len() != TX_META_KEY_ENCODED_LEN
        || encoded.get(0..2) != Some(TX_KEY_MAGIC.as_slice())
        || encoded.get(10) != Some(&TX_META_KIND)
    {
        return Err(decode_corruption());
    }
    let commit_seq = read_u64_be(encoded, 2).ok_or_else(decode_corruption)?;
    if commit_seq == 0 {
        return Err(decode_corruption());
    }
    Ok(commit_seq)
}

pub(crate) fn encode_tx_mutation_key(
    commit_seq: CommitSeq,
    ordinal: u64,
) -> Result<[u8; TX_MUTATION_KEY_ENCODED_LEN]> {
    if commit_seq == 0 {
        return Err(encode_invalid());
    }
    let mut encoded = [0_u8; TX_MUTATION_KEY_ENCODED_LEN];
    encoded[0..2].copy_from_slice(&TX_KEY_MAGIC);
    encoded[2..10].copy_from_slice(&commit_seq.to_be_bytes());
    encoded[10] = TX_MUTATION_KIND;
    encoded[11..19].copy_from_slice(&ordinal.to_be_bytes());
    Ok(encoded)
}

pub(crate) fn decode_tx_mutation_key(encoded: &[u8]) -> Result<(CommitSeq, u64)> {
    if encoded.len() != TX_MUTATION_KEY_ENCODED_LEN
        || encoded.get(0..2) != Some(TX_KEY_MAGIC.as_slice())
        || encoded.get(10) != Some(&TX_MUTATION_KIND)
    {
        return Err(decode_corruption());
    }
    let commit_seq = read_u64_be(encoded, 2).ok_or_else(decode_corruption)?;
    let ordinal = read_u64_be(encoded, 11).ok_or_else(decode_corruption)?;
    if commit_seq == 0 {
        return Err(decode_corruption());
    }
    Ok((commit_seq, ordinal))
}

pub(crate) fn encode_tx_meta(meta: &TxMeta) -> Result<[u8; TX_META_ENCODED_LEN]> {
    validate_tx_meta(meta, true)?;

    let mut encoded = [0_u8; TX_META_ENCODED_LEN];
    encoded[0..4].copy_from_slice(&TX_META_MAGIC);
    encoded[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    encoded[6..14].copy_from_slice(&meta.commit_seq.to_le_bytes());
    encoded[14..30].copy_from_slice(&meta.tx_uuid.0);
    encoded[30..38].copy_from_slice(&meta.prev_seq.to_le_bytes());
    write_vlog_pos(&mut encoded, 38, meta.vlog_begin);
    write_vlog_pos(&mut encoded, 50, meta.vlog_end);
    encoded[62..70].copy_from_slice(&meta.logical_op_count.to_le_bytes());
    encoded[70..78].copy_from_slice(&meta.distinct_key_count.to_le_bytes());
    encoded[78..82].copy_from_slice(&meta.envelope_crc32c.to_le_bytes());
    encoded[82..86].copy_from_slice(&meta.descriptor_crc32c.to_le_bytes());
    Ok(encoded)
}

pub(crate) fn decode_tx_meta(encoded: &[u8]) -> Result<TxMeta> {
    if encoded.len() != TX_META_ENCODED_LEN || encoded.get(0..4) != Some(TX_META_MAGIC.as_slice()) {
        return Err(decode_corruption());
    }
    let format_version = read_u16_le(encoded, 4).ok_or_else(decode_corruption)?;
    if format_version != FORMAT_VERSION {
        return Err(decode_incompatible());
    }

    let meta = TxMeta {
        commit_seq: read_u64_le(encoded, 6).ok_or_else(decode_corruption)?,
        tx_uuid: TxUuid(read_array(encoded, 14).ok_or_else(decode_corruption)?),
        prev_seq: read_u64_le(encoded, 30).ok_or_else(decode_corruption)?,
        vlog_begin: read_vlog_pos(encoded, 38).ok_or_else(decode_corruption)?,
        vlog_end: read_vlog_pos(encoded, 50).ok_or_else(decode_corruption)?,
        logical_op_count: read_u64_le(encoded, 62).ok_or_else(decode_corruption)?,
        distinct_key_count: read_u64_le(encoded, 70).ok_or_else(decode_corruption)?,
        envelope_crc32c: read_u32_le(encoded, 78).ok_or_else(decode_corruption)?,
        descriptor_crc32c: read_u32_le(encoded, 82).ok_or_else(decode_corruption)?,
    };
    validate_tx_meta(&meta, false)?;
    Ok(meta)
}

pub(crate) fn encode_tx_mutation(mutation: &TxMutation) -> Result<Vec<u8>> {
    validate_user_key(&mutation.user_key, true)?;
    validate_value_state_key_len(mutation.before_state, mutation.user_key.len(), true)?;
    validate_value_state_key_len(mutation.after_state, mutation.user_key.len(), true)?;
    let key_len = u16::try_from(mutation.user_key.len()).map_err(|_| encode_capacity())?;
    let before_len = value_state_encoded_len(mutation.before_state);
    let after_len = value_state_encoded_len(mutation.after_state);
    let encoded_len = 2_usize
        .checked_add(mutation.user_key.len())
        .and_then(|len| len.checked_add(before_len))
        .and_then(|len| len.checked_add(after_len))
        .ok_or_else(encode_capacity)?;

    let mut encoded = Vec::new();
    inject_descriptor_allocation_failure(DescriptorAllocationFailureSite::MutationValue)?;
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| encode_allocation())?;
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(&mutation.user_key);
    encode_value_state(mutation.before_state, &mut encoded)?;
    encode_value_state(mutation.after_state, &mut encoded)?;
    Ok(encoded)
}

pub(crate) fn decode_tx_mutation(encoded: &[u8]) -> Result<TxMutation> {
    let user_key_len = usize::from(read_u16_le(encoded, 0).ok_or_else(decode_corruption)?);
    if user_key_len == 0 || user_key_len > MAX_USER_KEY_LEN {
        return Err(decode_corruption());
    }
    let key_end = 2_usize
        .checked_add(user_key_len)
        .ok_or_else(decode_corruption)?;
    let user_key_bytes = encoded.get(2..key_end).ok_or_else(decode_corruption)?;

    let mut user_key = Vec::new();
    user_key
        .try_reserve_exact(user_key_len)
        .map_err(|_| decode_allocation())?;
    user_key.extend_from_slice(user_key_bytes);

    let (before_state, before_len) =
        decode_value_state(encoded.get(key_end..).ok_or_else(decode_corruption)?)?;
    let after_start = key_end
        .checked_add(before_len)
        .ok_or_else(decode_corruption)?;
    let (after_state, after_len) =
        decode_value_state(encoded.get(after_start..).ok_or_else(decode_corruption)?)?;
    let consumed = after_start
        .checked_add(after_len)
        .ok_or_else(decode_corruption)?;
    if consumed != encoded.len() {
        return Err(decode_corruption());
    }
    validate_value_state_key_len(before_state, user_key.len(), false)?;
    validate_value_state_key_len(after_state, user_key.len(), false)?;

    Ok(TxMutation {
        user_key,
        before_state,
        after_state,
    })
}

pub(crate) fn encode_descriptor(descriptor: &TransactionDescriptor) -> Result<EncodedDescriptor> {
    let expected_count =
        usize::try_from(descriptor.meta.distinct_key_count).map_err(|_| encode_capacity())?;
    if expected_count != descriptor.mutations.len() {
        return Err(encode_invalid());
    }
    validate_tx_meta(&descriptor.meta, true)?;

    let mut seen_keys: HashSet<&[u8]> = HashSet::new();
    inject_descriptor_allocation_failure(DescriptorAllocationFailureSite::SeenKeys)?;
    seen_keys
        .try_reserve(expected_count)
        .map_err(|_| encode_allocation())?;
    let mut mutations = Vec::new();
    inject_descriptor_allocation_failure(DescriptorAllocationFailureSite::EncodedMutations)?;
    mutations
        .try_reserve_exact(expected_count)
        .map_err(|_| encode_allocation())?;

    for (index, mutation) in descriptor.mutations.iter().enumerate() {
        if !seen_keys.insert(mutation.user_key.as_slice()) {
            return Err(encode_invalid());
        }
        let ordinal = u64::try_from(index).map_err(|_| encode_capacity())?;
        let key = encode_tx_mutation_key(descriptor.meta.commit_seq, ordinal)?;
        let value = encode_tx_mutation(mutation)?;
        mutations.push(EncodedMutation { key, value });
    }

    let mut meta = descriptor.meta.clone();
    meta.descriptor_crc32c = 0;
    let meta_without_crc = encode_tx_meta(&meta)?;
    let descriptor_crc32c = descriptor_crc(&meta_without_crc, &mutations, true)?;
    meta.descriptor_crc32c = descriptor_crc32c;

    Ok(EncodedDescriptor {
        meta_key: encode_tx_meta_key(meta.commit_seq)?,
        meta_value: encode_tx_meta(&meta)?,
        mutations,
    })
}

pub(crate) fn decode_descriptor(
    meta_key: &[u8],
    meta_value: &[u8],
    mutation_entries: &[(&[u8], &[u8])],
) -> Result<TransactionDescriptor> {
    let key_commit_seq = decode_tx_meta_key(meta_key)?;
    let meta = decode_tx_meta(meta_value)?;
    if key_commit_seq != meta.commit_seq {
        return Err(decode_corruption());
    }

    let expected_count =
        usize::try_from(meta.distinct_key_count).map_err(|_| decode_corruption())?;
    if mutation_entries.len() != expected_count {
        return Err(decode_corruption());
    }

    let mut seen_keys: HashSet<Vec<u8>> = HashSet::new();
    seen_keys
        .try_reserve(expected_count)
        .map_err(|_| decode_allocation())?;
    let mut mutations = Vec::new();
    mutations
        .try_reserve_exact(expected_count)
        .map_err(|_| decode_allocation())?;

    let mut crc = crc32c(DESCRIPTOR_CRC_MAGIC);
    crc = crc32c_append(crc, meta_value.get(0..82).ok_or_else(decode_corruption)?);

    for (expected_ordinal, (key, value)) in mutation_entries.iter().enumerate() {
        let (commit_seq, ordinal) = decode_tx_mutation_key(key)?;
        let expected_ordinal = u64::try_from(expected_ordinal).map_err(|_| decode_corruption())?;
        if commit_seq != meta.commit_seq || ordinal != expected_ordinal {
            return Err(decode_corruption());
        }

        let mutation = decode_tx_mutation(value)?;
        let duplicate_key = try_clone_bytes(&mutation.user_key, false)?;
        if !seen_keys.insert(duplicate_key) {
            return Err(decode_corruption());
        }

        let value_len = u32::try_from(value.len()).map_err(|_| decode_corruption())?;
        crc = crc32c_append(crc, key);
        crc = crc32c_append(crc, &value_len.to_le_bytes());
        crc = crc32c_append(crc, value);
        mutations.push(mutation);
    }

    if crc != meta.descriptor_crc32c {
        return Err(decode_corruption());
    }
    Ok(TransactionDescriptor { meta, mutations })
}

impl DatabaseIdentity {
    pub(crate) fn encode(&self) -> Result<[u8; DATABASE_IDENTITY_ENCODED_LEN]> {
        if self.identity_format_version != FORMAT_VERSION
            || self.database_format_version != DATABASE_FORMAT_VERSION
            || self.database_uuid == [0; 16]
            || self.keyspace_layout_version != KEYSPACE_LAYOUT_VERSION
        {
            return Err(encode_invalid());
        }

        let mut encoded = [0_u8; DATABASE_IDENTITY_ENCODED_LEN];
        encoded[0..4].copy_from_slice(&DATABASE_IDENTITY_MAGIC);
        encoded[4..6].copy_from_slice(&self.identity_format_version.to_le_bytes());
        encoded[6..10].copy_from_slice(&self.database_format_version.to_le_bytes());
        encoded[10..26].copy_from_slice(&self.database_uuid);
        encoded[26..28].copy_from_slice(&self.keyspace_layout_version.to_le_bytes());
        let checksum = crc32c(&encoded[0..28]);
        encoded[28..32].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != DATABASE_IDENTITY_ENCODED_LEN
            || encoded.get(0..4) != Some(DATABASE_IDENTITY_MAGIC.as_slice())
        {
            return Err(decode_corruption());
        }
        verify_crc(encoded, 28, 28)?;

        let identity_format_version = read_u16_le(encoded, 4).ok_or_else(decode_corruption)?;
        let keyspace_layout_version = read_u16_le(encoded, 26).ok_or_else(decode_corruption)?;
        if identity_format_version != FORMAT_VERSION
            || keyspace_layout_version != KEYSPACE_LAYOUT_VERSION
        {
            return Err(decode_incompatible());
        }

        let identity = Self {
            identity_format_version,
            database_format_version: read_u32_le(encoded, 6).ok_or_else(decode_corruption)?,
            database_uuid: read_array(encoded, 10).ok_or_else(decode_corruption)?,
            keyspace_layout_version,
        };
        if identity.database_uuid == [0; 16] {
            return Err(decode_corruption());
        }
        Ok(identity)
    }

    pub(crate) fn validate_against(
        &self,
        database_format_version: u32,
        database_uuid: [u8; 16],
    ) -> Result<()> {
        if self.database_format_version != database_format_version
            || self.database_uuid != database_uuid
        {
            return Err(decode_invalid_layout());
        }
        Ok(())
    }
}

pub(crate) fn encode_head_seq(head_seq: CommitSeq) -> [u8; HEAD_SEQ_ENCODED_LEN] {
    head_seq.to_le_bytes()
}

pub(crate) fn decode_head_seq(encoded: &[u8]) -> Result<CommitSeq> {
    if encoded.len() != HEAD_SEQ_ENCODED_LEN {
        return Err(decode_corruption());
    }
    read_u64_le(encoded, 0).ok_or_else(decode_corruption)
}

pub(crate) fn next_commit_seq(head_seq: CommitSeq) -> Result<CommitSeq> {
    head_seq.checked_add(1).ok_or_else(encode_capacity)
}

impl DurableFrontier {
    pub(crate) fn encode(&self) -> Result<[u8; DURABLE_FRONTIER_ENCODED_LEN]> {
        self.validate().map_err(|_| encode_invalid())?;
        let mut encoded = [0_u8; DURABLE_FRONTIER_ENCODED_LEN];
        encoded[0..4].copy_from_slice(&DURABLE_FRONTIER_MAGIC);
        encoded[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        encoded[6..14].copy_from_slice(&self.durable_seq.to_le_bytes());
        write_durable_end(&mut encoded, 14, self.durable_vlog_end);
        let checksum = crc32c(&encoded[0..27]);
        encoded[27..31].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != DURABLE_FRONTIER_ENCODED_LEN
            || encoded.get(0..4) != Some(DURABLE_FRONTIER_MAGIC.as_slice())
        {
            return Err(decode_corruption());
        }
        verify_crc(encoded, 27, 27)?;
        let format_version = read_u16_le(encoded, 4).ok_or_else(decode_corruption)?;
        if format_version != FORMAT_VERSION {
            return Err(decode_incompatible());
        }

        let frontier = Self {
            durable_seq: read_u64_le(encoded, 6).ok_or_else(decode_corruption)?,
            durable_vlog_end: read_durable_end(encoded, 14)?,
        };
        frontier.validate()?;
        Ok(frontier)
    }

    pub(crate) fn validate_against_head(&self, head_seq: CommitSeq) -> Result<()> {
        self.validate()?;
        if self.durable_seq > head_seq {
            return Err(decode_corruption());
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        match (self.durable_seq, self.durable_vlog_end) {
            (0, DurableVLogEnd::Empty) => Ok(()),
            (0, DurableVLogEnd::Position(_)) | (_, DurableVLogEnd::Empty) => {
                Err(decode_corruption())
            }
            (_, DurableVLogEnd::Position(position)) if !position.is_valid() => {
                Err(decode_corruption())
            }
            (_, DurableVLogEnd::Position(_)) => Ok(()),
        }
    }
}

impl RecoveryState {
    pub(crate) fn encode(&self) -> Result<[u8; RECOVERY_STATE_ENCODED_LEN]> {
        self.validate().map_err(|_| encode_invalid())?;
        let mut encoded = [0_u8; RECOVERY_STATE_ENCODED_LEN];
        encoded[0..4].copy_from_slice(&RECOVERY_STATE_MAGIC);
        encoded[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        encoded[6] = match self.phase {
            RecoveryPhase::Undo => 1,
            RecoveryPhase::Trim => 2,
            RecoveryPhase::Finalize => 3,
        };
        encoded[7..15].copy_from_slice(&self.original_head.to_le_bytes());
        encoded[15..23].copy_from_slice(&self.target_seq.to_le_bytes());
        write_durable_end(&mut encoded, 23, self.target_vlog_end);
        encoded[36..44].copy_from_slice(&self.next_undo_seq.to_le_bytes());
        encoded[44] = u8::from(self.trim_required);
        let checksum = crc32c(&encoded[0..45]);
        encoded[45..49].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != RECOVERY_STATE_ENCODED_LEN
            || encoded.get(0..4) != Some(RECOVERY_STATE_MAGIC.as_slice())
        {
            return Err(decode_corruption());
        }
        verify_crc(encoded, 45, 45)?;
        let format_version = read_u16_le(encoded, 4).ok_or_else(decode_corruption)?;
        if format_version != FORMAT_VERSION {
            return Err(decode_incompatible());
        }

        let phase = match encoded.get(6) {
            Some(1) => RecoveryPhase::Undo,
            Some(2) => RecoveryPhase::Trim,
            Some(3) => RecoveryPhase::Finalize,
            _ => return Err(decode_corruption()),
        };
        let trim_required = match encoded.get(44) {
            Some(0) => false,
            Some(1) => true,
            _ => return Err(decode_corruption()),
        };
        let state = Self {
            phase,
            original_head: read_u64_le(encoded, 7).ok_or_else(decode_corruption)?,
            target_seq: read_u64_le(encoded, 15).ok_or_else(decode_corruption)?,
            target_vlog_end: read_durable_end(encoded, 23)?,
            next_undo_seq: read_u64_le(encoded, 36).ok_or_else(decode_corruption)?,
            trim_required,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<()> {
        if self.target_seq > self.next_undo_seq || self.next_undo_seq > self.original_head {
            return Err(decode_corruption());
        }
        match (self.target_seq, self.target_vlog_end) {
            (0, DurableVLogEnd::Empty) => {}
            (0, DurableVLogEnd::Position(_)) | (_, DurableVLogEnd::Empty) => {
                return Err(decode_corruption());
            }
            (_, DurableVLogEnd::Position(position)) if !position.is_valid() => {
                return Err(decode_corruption());
            }
            (_, DurableVLogEnd::Position(_)) => {}
        }

        match self.phase {
            RecoveryPhase::Undo => Ok(()),
            RecoveryPhase::Trim if self.next_undo_seq == self.target_seq && self.trim_required => {
                Ok(())
            }
            RecoveryPhase::Finalize
                if self.next_undo_seq == self.target_seq && !self.trim_required =>
            {
                Ok(())
            }
            RecoveryPhase::Trim | RecoveryPhase::Finalize => Err(decode_corruption()),
        }
    }
}

fn validate_tx_meta(meta: &TxMeta, encoding: bool) -> Result<()> {
    let invalid = || {
        if encoding {
            encode_invalid()
        } else {
            decode_corruption()
        }
    };
    if meta.commit_seq == 0
        || meta.prev_seq != meta.commit_seq.checked_sub(1).ok_or_else(invalid)?
        || !meta.tx_uuid.is_valid()
        || !meta.vlog_begin.is_valid()
        || !meta.vlog_end.is_valid()
        || meta.vlog_begin >= meta.vlog_end
        || meta.logical_op_count == 0
        || meta.distinct_key_count == 0
        || meta.distinct_key_count > meta.logical_op_count
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_user_key(user_key: &[u8], encoding: bool) -> Result<()> {
    if user_key.is_empty() || user_key.len() > MAX_USER_KEY_LEN {
        return Err(if encoding {
            encode_invalid()
        } else {
            decode_corruption()
        });
    }
    Ok(())
}

fn value_state_encoded_len(state: ValueState) -> usize {
    match state {
        ValueState::Absent => 1,
        ValueState::Present(_) => 17,
    }
}

fn validate_value_state_key_len(
    state: ValueState,
    user_key_len: usize,
    encoding: bool,
) -> Result<()> {
    let ValueState::Present(pointer) = state else {
        return Ok(());
    };
    let layout = pointer.layout().map_err(|error| {
        if encoding {
            encode_invalid()
        } else {
            metadata_error_from_pointer(error.kind)
        }
    })?;
    if usize::from(layout.key_len) != user_key_len {
        return Err(if encoding {
            encode_invalid()
        } else {
            decode_corruption()
        });
    }
    Ok(())
}

fn encode_value_state(state: ValueState, output: &mut Vec<u8>) -> Result<()> {
    match state {
        ValueState::Absent => output.push(0),
        ValueState::Present(pointer) => {
            let encoded = pointer.encode()?;
            output.push(1);
            output.extend_from_slice(&encoded);
        }
    }
    Ok(())
}

fn decode_value_state(encoded: &[u8]) -> Result<(ValueState, usize)> {
    match encoded.first() {
        Some(0) => Ok((ValueState::Absent, 1)),
        Some(1) => {
            let pointer_end = 1_usize.checked_add(16).ok_or_else(decode_corruption)?;
            let pointer_bytes = encoded.get(1..pointer_end).ok_or_else(decode_corruption)?;
            let pointer = ValuePointer::decode(pointer_bytes)
                .map_err(|error| metadata_error_from_pointer(error.kind))?;
            Ok((ValueState::Present(pointer), pointer_end))
        }
        _ => Err(decode_corruption()),
    }
}

fn descriptor_crc(
    meta_value: &[u8; TX_META_ENCODED_LEN],
    mutations: &[EncodedMutation],
    encoding: bool,
) -> Result<u32> {
    let mut crc = crc32c(DESCRIPTOR_CRC_MAGIC);
    crc = crc32c_append(crc, &meta_value[0..82]);
    for mutation in mutations {
        let value_len = u32::try_from(mutation.value.len()).map_err(|_| {
            if encoding {
                encode_capacity()
            } else {
                decode_corruption()
            }
        })?;
        crc = crc32c_append(crc, &mutation.key);
        crc = crc32c_append(crc, &value_len.to_le_bytes());
        crc = crc32c_append(crc, &mutation.value);
    }
    Ok(crc)
}

fn write_vlog_pos(output: &mut [u8], offset: usize, position: VLogPos) {
    output[offset..offset + 4].copy_from_slice(&position.file_id.to_le_bytes());
    output[offset + 4..offset + 12].copy_from_slice(&position.offset.to_le_bytes());
}

fn read_vlog_pos(input: &[u8], offset: usize) -> Option<VLogPos> {
    Some(VLogPos {
        file_id: read_u32_le(input, offset)?,
        offset: read_u64_le(input, offset.checked_add(4)?)?,
    })
}

fn write_durable_end(output: &mut [u8], offset: usize, end: DurableVLogEnd) {
    match end {
        DurableVLogEnd::Empty => {
            output[offset] = 0;
        }
        DurableVLogEnd::Position(position) => {
            output[offset] = 1;
            write_vlog_pos(output, offset + 1, position);
        }
    }
}

fn read_durable_end(input: &[u8], offset: usize) -> Result<DurableVLogEnd> {
    let tag = *input.get(offset).ok_or_else(decode_corruption)?;
    let position_offset = offset.checked_add(1).ok_or_else(decode_corruption)?;
    let position = read_vlog_pos(input, position_offset).ok_or_else(decode_corruption)?;
    match tag {
        0 if position.file_id == 0 && position.offset == 0 => Ok(DurableVLogEnd::Empty),
        0 => Err(decode_corruption()),
        1 if position.is_valid() => Ok(DurableVLogEnd::Position(position)),
        1 => Err(decode_corruption()),
        _ => Err(decode_corruption()),
    }
}

fn verify_crc(input: &[u8], covered_len: usize, crc_offset: usize) -> Result<()> {
    let covered = input.get(0..covered_len).ok_or_else(decode_corruption)?;
    let expected = read_u32_le(input, crc_offset).ok_or_else(decode_corruption)?;
    if crc32c(covered) != expected {
        return Err(decode_corruption());
    }
    Ok(())
}

fn try_clone_bytes(bytes: &[u8], encoding: bool) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned.try_reserve_exact(bytes.len()).map_err(|_| {
        if encoding {
            encode_allocation()
        } else {
            decode_allocation()
        }
    })?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

fn read_array<const N: usize>(input: &[u8], offset: usize) -> Option<[u8; N]> {
    let end = offset.checked_add(N)?;
    input.get(offset..end)?.try_into().ok()
}

fn read_u16_le(input: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(read_array(input, offset)?))
}

fn read_u32_le(input: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(read_array(input, offset)?))
}

fn read_u64_le(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(read_array(input, offset)?))
}

fn read_u64_be(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_be_bytes(read_array(input, offset)?))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescriptorAllocationFailureSite {
    SeenKeys,
    EncodedMutations,
    MutationValue,
}

#[cfg(test)]
mod descriptor_allocation_failure {
    use std::cell::Cell;

    use super::DescriptorAllocationFailureSite;

    thread_local! {
        static NEXT_FAILURE: Cell<Option<DescriptorAllocationFailureSite>> = const { Cell::new(None) };
    }

    pub(super) fn inject(site: DescriptorAllocationFailureSite) {
        NEXT_FAILURE.with(|next| assert!(next.replace(Some(site)).is_none()));
    }

    pub(super) fn should_fail(site: DescriptorAllocationFailureSite) -> bool {
        NEXT_FAILURE.with(|next| {
            if next.get() == Some(site) {
                next.set(None);
                true
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
pub(crate) fn inject_descriptor_allocation_failure_for_test(site: DescriptorAllocationFailureSite) {
    descriptor_allocation_failure::inject(site);
}

fn inject_descriptor_allocation_failure(site: DescriptorAllocationFailureSite) -> Result<()> {
    #[cfg(test)]
    if descriptor_allocation_failure::should_fail(site) {
        return Err(encode_allocation());
    }
    let _ = site;
    Ok(())
}

fn encode_invalid() -> StorageError {
    metadata_encode_error(
        StorageErrorKind::InvalidArgument,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn encode_capacity() -> StorageError {
    metadata_encode_error(
        StorageErrorKind::CapacityExceeded,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn encode_allocation() -> StorageError {
    metadata_encode_error(
        StorageErrorKind::ResourceExhausted,
        RetryAdvice::RetrySameInstance,
    )
}

fn metadata_encode_error(kind: StorageErrorKind, retry_advice: RetryAdvice) -> StorageError {
    StorageError::codec_error(
        kind,
        Operation::WriteBatch,
        ProtocolStage::Preflight,
        Some(WriteOutcome::NotCommitted),
        retry_advice,
    )
}

fn decode_corruption() -> StorageError {
    metadata_decode_error(StorageErrorKind::Corruption, RetryAdvice::RestoreOrRepair)
}

fn decode_incompatible() -> StorageError {
    metadata_decode_error(
        StorageErrorKind::IncompatibleFormat,
        RetryAdvice::DoNotRetry,
    )
}

fn decode_invalid_layout() -> StorageError {
    metadata_decode_error(
        StorageErrorKind::InvalidLayout,
        RetryAdvice::RestoreOrRepair,
    )
}

fn decode_allocation() -> StorageError {
    metadata_decode_error(
        StorageErrorKind::ResourceExhausted,
        RetryAdvice::FixEnvironmentAndReopen,
    )
}

fn metadata_error_from_pointer(kind: StorageErrorKind) -> StorageError {
    match kind {
        StorageErrorKind::IncompatibleFormat => decode_incompatible(),
        _ => decode_corruption(),
    }
}

fn metadata_decode_error(kind: StorageErrorKind, retry_advice: RetryAdvice) -> StorageError {
    StorageError::codec_error(
        kind,
        Operation::Recovery,
        ProtocolStage::Recovery,
        None,
        retry_advice,
    )
}
