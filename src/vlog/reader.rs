//! Value Log positional reads, envelope scanning, and record validation.
#![allow(dead_code)] // Stage 8 boundary; public reads are wired in later stages.

use std::sync::Arc;

use crate::vlog::file_set::{FileSet, read_corruption, read_exact_at};
use crate::vlog::format::{DecodedRecord, VLogGeometry, VLogPosition, ValuePointer};
use crate::{Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind};

pub(crate) struct ValueLogReader {
    files: Arc<FileSet>,
    geometry: VLogGeometry,
    #[cfg(test)]
    positioned_read: Option<Arc<dyn crate::vlog::file_set::PositionedRead>>,
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
