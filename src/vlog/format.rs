//! Value Log file, page, envelope, record, and pointer encoding.
#![allow(dead_code)] // Stage 2 codec; production consumers are wired in later stages.

use crate::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

pub(crate) const VALUE_POINTER_FORMAT_VERSION: u16 = 0;
pub(crate) const VALUE_POINTER_ENCODED_LEN: usize = 16;
pub(crate) const MAX_VLOG_FILE_ID: u32 = 999_999;
pub(crate) const MAX_VLOG_FILE_SIZE: u64 = 1_u64 << 32;
pub(crate) const VLOG_PAGE_SIZE: u64 = 65_536;
pub(crate) const FIRST_PAGE_RECORD_AREA_START: u64 = 64;
pub(crate) const OTHER_PAGE_RECORD_AREA_OFFSET: u64 = 16;
pub(crate) const MIN_KV_RECORD_LEN: u32 = 56;
pub(crate) const MAX_KV_RECORD_LEN: u32 = 60_055;
pub(crate) const MAX_KEY_VALUE_SIZE: u32 = 60_000;
const KV_RECORD_FIXED_LEN_WITH_CRC: u32 = 55;
const RECORD_CRC_LEN: u32 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValuePointer {
    pub(crate) format_version: u16,
    pub(crate) file_id: u32,
    pub(crate) record_offset: u32,
    pub(crate) record_len: u32,
    pub(crate) value_len: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValuePointerLayout {
    pub(crate) key_len: u16,
    pub(crate) value_relative_offset: u32,
    pub(crate) value_start: u64,
    pub(crate) value_end: u64,
    pub(crate) record_end: u64,
}

impl ValuePointer {
    pub(crate) fn encode(&self) -> Result<[u8; VALUE_POINTER_ENCODED_LEN]> {
        self.validate_fields().map_err(|_| encode_error())?;

        let mut encoded = [0_u8; VALUE_POINTER_ENCODED_LEN];
        encoded[0..2].copy_from_slice(&self.format_version.to_le_bytes());
        encoded[2..6].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[6..10].copy_from_slice(&self.record_offset.to_le_bytes());
        encoded[10..14].copy_from_slice(&self.record_len.to_le_bytes());
        encoded[14..16].copy_from_slice(&self.value_len.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != VALUE_POINTER_ENCODED_LEN {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let format_version = u16::from_le_bytes(
            encoded[0..2]
                .try_into()
                .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
        );
        if format_version != VALUE_POINTER_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }

        let pointer = Self {
            format_version,
            file_id: u32::from_le_bytes(
                encoded[2..6]
                    .try_into()
                    .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
            ),
            record_offset: u32::from_le_bytes(
                encoded[6..10]
                    .try_into()
                    .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
            ),
            record_len: u32::from_le_bytes(
                encoded[10..14]
                    .try_into()
                    .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
            ),
            value_len: u16::from_le_bytes(
                encoded[14..16]
                    .try_into()
                    .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
            ),
        };
        pointer.validate_fields()?;
        Ok(pointer)
    }

    // 得到layout 说明满足计算关系
    pub(crate) fn layout(&self) -> Result<ValuePointerLayout> {
        self.validate_fields()?;

        let payload_len = self
            .record_len
            .checked_sub(KV_RECORD_FIXED_LEN_WITH_CRC)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let key_len = payload_len
            .checked_sub(u32::from(self.value_len))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let key_len_u16 =
            u16::try_from(key_len).map_err(|_| decode_error(StorageErrorKind::Corruption))?;
        let value_relative_offset = self
            .record_len
            .checked_sub(RECORD_CRC_LEN)
            .and_then(|len| len.checked_sub(u32::from(self.value_len)))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let record_start = u64::from(self.record_offset);
        let record_end = record_start
            .checked_add(u64::from(self.record_len))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let value_start = record_start
            .checked_add(u64::from(value_relative_offset))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let value_end = record_end
            .checked_sub(u64::from(RECORD_CRC_LEN))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;

        Ok(ValuePointerLayout {
            key_len: key_len_u16,
            value_relative_offset,
            value_start,
            value_end,
            record_end,
        })
    }

    pub(crate) fn validate_file_bounds(&self, file_st_size: u64) -> Result<ValuePointerLayout> {
        let layout = self.layout()?;
        if file_st_size > MAX_VLOG_FILE_SIZE
            || layout.record_end > MAX_VLOG_FILE_SIZE
            || layout.record_end > file_st_size
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let record_start = u64::from(self.record_offset);
        let page_no = record_start / VLOG_PAGE_SIZE;
        let page_start = page_no
            .checked_mul(VLOG_PAGE_SIZE)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let record_area_start = if page_no == 0 {
            FIRST_PAGE_RECORD_AREA_START
        } else {
            page_start
                .checked_add(OTHER_PAGE_RECORD_AREA_OFFSET)
                .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
        };
        let page_end = page_start
            .checked_add(VLOG_PAGE_SIZE)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;

        if record_start < record_area_start || layout.record_end > page_end {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(layout)
    }

    // 验证ValuePointer的每个字段是否符合规范
    fn validate_fields(&self) -> Result<()> {
        if self.format_version != VALUE_POINTER_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }
        if self.file_id > MAX_VLOG_FILE_ID
            || self.record_len < MIN_KV_RECORD_LEN
            || self.record_len > MAX_KV_RECORD_LEN
            || u32::from(self.value_len) > MAX_KEY_VALUE_SIZE
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let payload_len = self
            .record_len
            .checked_sub(KV_RECORD_FIXED_LEN_WITH_CRC)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let key_len = payload_len
            .checked_sub(u32::from(self.value_len))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        let total = key_len
            .checked_add(u32::from(self.value_len))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        if key_len == 0 || total > MAX_KEY_VALUE_SIZE {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(())
    }
}

fn encode_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::InvalidArgument,
        Operation::WriteBatch,
        ProtocolStage::Preflight,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::FixRequestAndRetrySameInstance,
    )
}

fn decode_error(kind: StorageErrorKind) -> StorageError {
    let retry_advice = if kind == StorageErrorKind::IncompatibleFormat {
        RetryAdvice::DoNotRetry
    } else {
        RetryAdvice::RestoreOrRepair
    };
    StorageError::codec_error(
        kind,
        Operation::Get,
        ProtocolStage::Read,
        None,
        retry_advice,
    )
}
