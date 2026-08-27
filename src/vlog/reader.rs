//! Value Log positional reads, envelope scanning, and record validation.
#![allow(dead_code)] // Stage 8 boundary; public reads are wired in later stages.

use std::collections::HashMap;
use std::sync::Arc;

use crate::vlog::file_set::{FileSet, read_corruption, read_exact_at};
use crate::vlog::format::{
    DecodedRecord, FILE_HEADER_ENCODED_LEN, PAGE_HEADER_ENCODED_LEN, PhysicalChunk,
    RECORD_HEADER_ENCODED_LEN, RecordHeader, ScannedEnvelope, VLogGeometry, VLogPosition,
    ValuePointer, locate_footer_from_end, scan_prepared_envelope,
};
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

pub(crate) struct ValueLogReader {
    files: Arc<FileSet>,
    geometry: VLogGeometry,
    #[cfg(test)]
    positioned_read: Option<Arc<dyn crate::vlog::file_set::PositionedRead>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvelopeValueState {
    Absent,
    Present(ValuePointer),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnvelopeFinalState {
    pub(crate) user_key: Vec<u8>,
    pub(crate) state: EnvelopeValueState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryEnvelope {
    pub(crate) scanned: ScannedEnvelope,
    pub(crate) final_states: Vec<EnvelopeFinalState>,
}

impl ValueLogReader {
    pub(crate) fn new(files: Arc<FileSet>, geometry: VLogGeometry) -> Result<Self> {
        crate::vlog::format::LayoutPlanner::empty(geometry)
            .map_err(|error| read_context(error, None, None))?;
        if files.geometry() != geometry {
            return Err(reader_configuration_error());
        }
        Ok(Self {
            files,
            geometry,
            #[cfg(test)]
            positioned_read: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_positioned_read(
        files: Arc<FileSet>,
        geometry: VLogGeometry,
        positioned_read: Arc<dyn crate::vlog::file_set::PositionedRead>,
    ) -> Result<Self> {
        let mut reader = Self::new(files, geometry)?;
        reader.positioned_read = Some(positioned_read);
        Ok(reader)
    }

    pub(crate) fn read_value(
        &self,
        encoded_pointer: &[u8],
        expected_key: &[u8],
    ) -> Result<Vec<u8>> {
        let pointer = ValuePointer::decode(encoded_pointer)
            .map_err(|error| read_context(error, None, None))?;
        self.read_pointer(pointer, expected_key)
    }

    pub(crate) fn read_pointer(
        &self,
        pointer: ValuePointer,
        expected_key: &[u8],
    ) -> Result<Vec<u8>> {
        let pointer_offset = u64::from(pointer.record_offset);
        pointer
            .layout()
            .map_err(|error| read_context(error, Some(pointer.file_id), Some(pointer_offset)))?;
        let handle = self.files.handle(pointer.file_id)?;
        let file_len = handle
            .metadata()
            .map_err(|error| read_io(pointer.file_id, pointer.record_offset, error))?
            .len();
        let pointer_layout = pointer
            .validate_file_bounds(file_len)
            .map_err(|error| read_context(error, Some(pointer.file_id), Some(pointer_offset)))?;
        let record_len = usize::try_from(pointer.record_len).map_err(|_| {
            read_corruption(pointer.file_id, Some(u64::from(pointer.record_offset)))
        })?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(record_len)
            .map_err(|_| read_resource(pointer.file_id, pointer.record_offset))?;
        encoded.resize(record_len, 0);
        #[cfg(test)]
        if let Some(positioned_read) = &self.positioned_read {
            crate::vlog::file_set::read_exact_at_with(
                positioned_read.as_ref(),
                &handle,
                &mut encoded,
                u64::from(pointer.record_offset),
                pointer.file_id,
            )?;
        } else {
            read_exact_at(
                &handle,
                &mut encoded,
                u64::from(pointer.record_offset),
                pointer.file_id,
            )?;
        }
        #[cfg(not(test))]
        read_exact_at(
            &handle,
            &mut encoded,
            u64::from(pointer.record_offset),
            pointer.file_id,
        )?;

        let start = VLogPosition {
            file_id: pointer.file_id,
            offset: u64::from(pointer.record_offset),
        };
        let record = crate::vlog::format::decode_record_at(&encoded, start, self.geometry)
            .map_err(|error| read_context(error, Some(pointer.file_id), Some(pointer_offset)))?;
        let DecodedRecord::KvRecord(record) = record else {
            return Err(read_corruption(pointer.file_id, Some(start.offset)));
        };
        if record.key != expected_key
            || record.key.len() != usize::from(pointer_layout.key_len)
            || record.value.len() != usize::from(pointer.value_len)
        {
            return Err(read_corruption(pointer.file_id, Some(start.offset)));
        }

        let mut value = Vec::new();
        value
            .try_reserve_exact(record.value.len())
            .map_err(|_| read_resource(pointer.file_id, pointer.record_offset))?;
        value.extend_from_slice(record.value);
        Ok(value)
    }

    pub(crate) fn files(&self) -> &Arc<FileSet> {
        &self.files
    }

    pub(crate) fn geometry(&self) -> VLogGeometry {
        self.geometry
    }

    pub(crate) fn read_recovery_envelope(
        &self,
        vlog_begin: VLogPosition,
        vlog_end: VLogPosition,
        expected_envelope_crc32c: Option<u32>,
    ) -> Result<RecoveryEnvelope> {
        let chunks = self.read_recovery_chunks(vlog_begin, vlog_end)?;
        let scanned = scan_prepared_envelope(
            &chunks,
            self.geometry,
            self.files.database_uuid(),
            vlog_begin,
            vlog_end,
            expected_envelope_crc32c,
        )
        .map_err(recovery_context)?;
        let final_states = collect_final_states(&chunks, self.geometry)?;
        Ok(RecoveryEnvelope {
            scanned,
            final_states,
        })
    }

    pub(crate) fn read_stable_envelope_from_end(
        &self,
        vlog_end: VLogPosition,
    ) -> Result<RecoveryEnvelope> {
        if vlog_end.file_id > self.geometry.max_file_id
            || vlog_end.offset == 0
            || vlog_end.offset > self.geometry.max_file_size
        {
            return Err(recovery_corruption());
        }
        let containing_offset = vlog_end
            .offset
            .checked_sub(1)
            .ok_or_else(recovery_corruption)?;
        let page_start = containing_offset
            .checked_div(self.geometry.page_size)
            .and_then(|page_no| page_no.checked_mul(self.geometry.page_size))
            .ok_or_else(recovery_corruption)?;
        let tail_start_offset = if page_start == 0 {
            u64::try_from(PAGE_HEADER_ENCODED_LEN + FILE_HEADER_ENCODED_LEN)
                .map_err(|_| recovery_corruption())?
        } else {
            page_start
                .checked_add(
                    u64::try_from(PAGE_HEADER_ENCODED_LEN).map_err(|_| recovery_corruption())?,
                )
                .ok_or_else(recovery_corruption)?
        };
        if tail_start_offset >= vlog_end.offset {
            return Err(recovery_corruption());
        }
        let tail_len = usize::try_from(
            vlog_end
                .offset
                .checked_sub(tail_start_offset)
                .ok_or_else(recovery_corruption)?,
        )
        .map_err(|_| recovery_corruption())?;
        let tail = self.read_recovery_bytes(vlog_end.file_id, tail_start_offset, tail_len)?;
        let located = locate_footer_from_end(
            VLogPosition {
                file_id: vlog_end.file_id,
                offset: tail_start_offset,
            },
            &tail,
            vlog_end,
            self.geometry,
        )
        .map_err(recovery_context)?;
        self.read_recovery_envelope(
            located.footer.vlog_begin,
            vlog_end,
            Some(located.footer.envelope_crc32c),
        )
    }

    fn read_recovery_chunks(
        &self,
        vlog_begin: VLogPosition,
        vlog_end: VLogPosition,
    ) -> Result<Vec<PhysicalChunk>> {
        if vlog_begin >= vlog_end
            || vlog_begin.file_id > self.geometry.max_file_id
            || vlog_end.file_id > self.geometry.max_file_id
            || vlog_begin.offset > self.geometry.max_file_size
            || vlog_end.offset > self.geometry.max_file_size
        {
            return Err(recovery_corruption());
        }

        let mut chunks = Vec::new();
        let mut cursor = recovery_next_position(vlog_begin, self.geometry)?;
        while cursor < vlog_end {
            if cursor.file_id > vlog_end.file_id {
                return Err(recovery_corruption());
            }
            let encoded_len = if cursor.offset.is_multiple_of(self.geometry.page_size) {
                PAGE_HEADER_ENCODED_LEN
            } else if cursor.offset == PAGE_HEADER_ENCODED_LEN as u64 {
                FILE_HEADER_ENCODED_LEN
            } else {
                let header_end = cursor
                    .offset
                    .checked_add(
                        u64::try_from(RECORD_HEADER_ENCODED_LEN)
                            .map_err(|_| recovery_corruption())?,
                    )
                    .ok_or_else(recovery_corruption)?;
                if cursor.file_id == vlog_end.file_id && header_end > vlog_end.offset {
                    return Err(recovery_corruption_at(cursor.file_id, cursor.offset));
                }
                let header = self.read_recovery_bytes(
                    cursor.file_id,
                    cursor.offset,
                    RECORD_HEADER_ENCODED_LEN,
                )?;
                let header = RecordHeader::decode(&header).map_err(recovery_context)?;
                let encoded_len =
                    usize::try_from(header.encoded_len).map_err(|_| recovery_corruption())?;
                if encoded_len < RECORD_HEADER_ENCODED_LEN {
                    return Err(recovery_corruption());
                }
                encoded_len
            };
            if encoded_len == 0 {
                return Err(recovery_corruption());
            }
            let encoded_len_u64 = u64::try_from(encoded_len).map_err(|_| recovery_corruption())?;
            let chunk_end_offset = cursor
                .offset
                .checked_add(encoded_len_u64)
                .ok_or_else(recovery_corruption)?;
            let page_end = cursor
                .offset
                .checked_div(self.geometry.page_size)
                .and_then(|page_no| page_no.checked_add(1))
                .and_then(|page_no| page_no.checked_mul(self.geometry.page_size))
                .ok_or_else(recovery_corruption)?;
            if chunk_end_offset > page_end || chunk_end_offset > self.geometry.max_file_size {
                return Err(recovery_corruption());
            }
            let chunk_end = VLogPosition {
                file_id: cursor.file_id,
                offset: chunk_end_offset,
            };
            if chunk_end > vlog_end {
                return Err(recovery_corruption());
            }
            let bytes = self.read_recovery_bytes(cursor.file_id, cursor.offset, encoded_len)?;
            chunks
                .try_reserve(1)
                .map_err(|_| recovery_resource(cursor.file_id, cursor.offset))?;
            chunks.push(PhysicalChunk {
                position: cursor,
                bytes,
            });
            cursor = if chunk_end == vlog_end {
                chunk_end
            } else {
                recovery_next_position(chunk_end, self.geometry)?
            };
        }
        if cursor != vlog_end {
            return Err(recovery_corruption());
        }
        Ok(chunks)
    }

    fn read_recovery_bytes(&self, file_id: u32, offset: u64, len: usize) -> Result<Vec<u8>> {
        let handle = self.files.handle(file_id).map_err(recovery_context)?;
        let file_len = handle
            .metadata()
            .map_err(|error| recovery_io(file_id, offset, error))?
            .len();
        let len_u64 = u64::try_from(len).map_err(|_| recovery_corruption())?;
        let end = offset
            .checked_add(len_u64)
            .ok_or_else(recovery_corruption)?;
        if end > file_len || end > self.geometry.max_file_size {
            return Err(recovery_corruption_at(file_id, offset));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| recovery_resource(file_id, offset))?;
        bytes.resize(len, 0);
        read_exact_at(&handle, &mut bytes, offset, file_id).map_err(recovery_context)?;
        Ok(bytes)
    }
}

fn recovery_next_position(position: VLogPosition, geometry: VLogGeometry) -> Result<VLogPosition> {
    if position.offset < geometry.max_file_size {
        return Ok(position);
    }
    if position.offset != geometry.max_file_size || position.file_id >= geometry.max_file_id {
        return Err(recovery_corruption());
    }
    Ok(VLogPosition {
        file_id: position
            .file_id
            .checked_add(1)
            .ok_or_else(recovery_corruption)?,
        offset: 0,
    })
}

fn collect_final_states(
    chunks: &[PhysicalChunk],
    geometry: VLogGeometry,
) -> Result<Vec<EnvelopeFinalState>> {
    let mut key_indexes = HashMap::<Vec<u8>, usize>::new();
    key_indexes
        .try_reserve(chunks.len())
        .map_err(|_| recovery_resource(0, 0))?;
    let mut states = Vec::<EnvelopeFinalState>::new();
    states
        .try_reserve(chunks.len())
        .map_err(|_| recovery_resource(0, 0))?;

    for chunk in chunks {
        let page_offset = chunk.position.offset % geometry.page_size;
        if page_offset == 0
            || (chunk.position.offset < geometry.page_size
                && chunk.position.offset == PAGE_HEADER_ENCODED_LEN as u64)
        {
            continue;
        }
        let decoded = crate::vlog::format::decode_record_at(&chunk.bytes, chunk.position, geometry)
            .map_err(recovery_context)?;
        let (key, state) = match decoded {
            DecodedRecord::KvRecord(record) => {
                let record_offset =
                    u32::try_from(chunk.position.offset).map_err(|_| recovery_corruption())?;
                let record_len =
                    u32::try_from(chunk.bytes.len()).map_err(|_| recovery_corruption())?;
                let value_len =
                    u16::try_from(record.value.len()).map_err(|_| recovery_corruption())?;
                (
                    record.key,
                    EnvelopeValueState::Present(ValuePointer {
                        format_version: 0,
                        file_id: chunk.position.file_id,
                        record_offset,
                        record_len,
                        value_len,
                    }),
                )
            }
            DecodedRecord::DeleteRecord(record) => (record.key, EnvelopeValueState::Absent),
            DecodedRecord::TxBegin(_)
            | DecodedRecord::TxPreparedEnd(_)
            | DecodedRecord::PageEnd => continue,
        };

        if let Some(index) = key_indexes.get(key).copied() {
            states.get_mut(index).ok_or_else(recovery_corruption)?.state = state;
            continue;
        }
        let map_key = try_copy_recovery_bytes(key)?;
        let state_key = try_copy_recovery_bytes(key)?;
        let index = states.len();
        key_indexes.insert(map_key, index);
        states.push(EnvelopeFinalState {
            user_key: state_key,
            state,
        });
    }
    Ok(states)
}

fn try_copy_recovery_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| recovery_resource(0, 0))?;
    owned.extend_from_slice(bytes);
    Ok(owned)
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

fn recovery_corruption_at(file_id: u32, offset: u64) -> StorageError {
    let mut error = recovery_corruption();
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn recovery_resource(file_id: u32, offset: u64) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::RetrySameInstance,
    );
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

fn recovery_io(file_id: u32, offset: u64, source: std::io::Error) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Open,
        ProtocolStage::Recovery,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.os_code = source.raw_os_error();
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(offset);
    error
}

#[cfg(not(test))]
impl crate::db::ValueReader for ValueLogReader {
    fn read_value(&self, encoded_pointer: &[u8], expected_key: &[u8]) -> Result<Vec<u8>> {
        ValueLogReader::read_value(self, encoded_pointer, expected_key)
    }
}

fn read_context(
    mut error: StorageError,
    file_id: Option<u32>,
    offset: Option<u64>,
) -> StorageError {
    error.operation = Operation::Get;
    error.protocol_stage = ProtocolStage::Read;
    error.write_outcome = None;
    error.instance_state = None;
    error.vlog_file_id = file_id;
    error.vlog_offset = offset;
    error
}

fn read_io(file_id: u32, offset: u32, source: std::io::Error) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::Io,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::FixEnvironmentAndReopen,
    );
    error.os_code = source.raw_os_error();
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(u64::from(offset));
    error
}

fn read_resource(file_id: u32, offset: u32) -> StorageError {
    let mut error = StorageError::codec_error(
        StorageErrorKind::ResourceExhausted,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::RetrySameInstance,
    );
    error.vlog_file_id = Some(file_id);
    error.vlog_offset = Some(u64::from(offset));
    error
}

fn reader_configuration_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidArgument,
        Operation::Get,
        ProtocolStage::Read,
        None,
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}
