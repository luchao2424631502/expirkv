//! Value Log file, page, envelope, record, and pointer encoding.
#![allow(dead_code)] // Stage 2/3 codecs are connected to file I/O in later stages.

use std::collections::HashSet;

use crate::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};
use crc32c::{crc32c, crc32c_append};

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

pub(crate) const PAGE_HEADER_ENCODED_LEN: usize = 16;
pub(crate) const FILE_HEADER_ENCODED_LEN: usize = 48;
pub(crate) const RECORD_HEADER_ENCODED_LEN: usize = 39;
pub(crate) const TX_BEGIN_ENCODED_LEN: u32 = 71;
pub(crate) const MIN_DELETE_RECORD_LEN: u32 = 54;
pub(crate) const TX_PREPARED_END_ENCODED_LEN: u32 = 111;
pub(crate) const PAGE_END_MIN_SIZE: u32 = 43;
pub(crate) const MAX_PAGE_END_LEN_FIRST_PAGE: u32 = 65_472;
pub(crate) const MAX_PAGE_END_LEN_OTHER_PAGE: u32 = 65_520;

const PAGE_HEADER_MAGIC: &[u8; 4] = b"RKVP";
const FILE_HEADER_MAGIC: &[u8; 8] = b"RKVLOG00";
const RECORD_HEADER_MAGIC: &[u8; 4] = b"RKVR";
const END_TRAILER_MAGIC: &[u8; 4] = b"RKTE";
const ENVELOPE_CRC_MAGIC: &[u8; 6] = b"RKENV0";
const VLOG_FORMAT_VERSION: u16 = 0;
const FILE_FORMAT_VERSION: u32 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct VLogPosition {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

impl VLogPosition {
    fn validate(self) -> Result<()> {
        if self.file_id > MAX_VLOG_FILE_ID || self.offset > MAX_VLOG_FILE_SIZE {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RecordType {
    TxBegin = 0x01,
    KvRecord = 0x02,
    DeleteRecord = 0x03,
    TxPreparedEnd = 0x04,
    PageEnd = 0x05,
}

impl RecordType {
    fn decode(encoded: u8) -> Result<Self> {
        match encoded {
            0x01 => Ok(Self::TxBegin),
            0x02 => Ok(Self::KvRecord),
            0x03 => Ok(Self::DeleteRecord),
            0x04 => Ok(Self::TxPreparedEnd),
            0x05 => Ok(Self::PageEnd),
            _ => Err(decode_error(StorageErrorKind::Corruption)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageHeader {
    pub(crate) file_id: u32,
    pub(crate) page_no: u32,
}

impl PageHeader {
    pub(crate) fn encode(self) -> Result<[u8; PAGE_HEADER_ENCODED_LEN]> {
        if self.file_id > MAX_VLOG_FILE_ID || self.page_no > 65_535 {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }

        let mut encoded = [0_u8; PAGE_HEADER_ENCODED_LEN];
        encoded[0..4].copy_from_slice(PAGE_HEADER_MAGIC);
        encoded[4..8].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[8..12].copy_from_slice(&self.page_no.to_le_bytes());
        let checksum = crc32c(&encoded[0..12]);
        encoded[12..16].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != PAGE_HEADER_ENCODED_LEN || encoded.get(0..4) != Some(PAGE_HEADER_MAGIC)
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let stored_crc = read_u32(encoded, 12)?;
        if crc32c(&encoded[0..12]) != stored_crc {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let header = Self {
            file_id: read_u32(encoded, 4)?,
            page_no: read_u32(encoded, 8)?,
        };
        if header.file_id > MAX_VLOG_FILE_ID || header.page_no > 65_535 {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(header)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VLogFileHeader {
    pub(crate) format_version: u32,
    pub(crate) database_uuid: [u8; 16],
    pub(crate) file_id: u32,
    pub(crate) page_size: u32,
    pub(crate) max_file_size: u64,
}

impl VLogFileHeader {
    pub(crate) fn new(database_uuid: [u8; 16], file_id: u32) -> Self {
        Self {
            format_version: FILE_FORMAT_VERSION,
            database_uuid,
            file_id,
            page_size: VLOG_PAGE_SIZE as u32,
            max_file_size: MAX_VLOG_FILE_SIZE,
        }
    }

    pub(crate) fn encode(self) -> Result<[u8; FILE_HEADER_ENCODED_LEN]> {
        self.validate().map_err(|error| {
            invalid_vlog_error(if error.kind == StorageErrorKind::IncompatibleFormat {
                StorageErrorKind::IncompatibleFormat
            } else {
                StorageErrorKind::InvalidArgument
            })
        })?;

        let mut encoded = [0_u8; FILE_HEADER_ENCODED_LEN];
        encoded[0..8].copy_from_slice(FILE_HEADER_MAGIC);
        encoded[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        encoded[12..28].copy_from_slice(&self.database_uuid);
        encoded[28..32].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[32..36].copy_from_slice(&self.page_size.to_le_bytes());
        encoded[36..44].copy_from_slice(&self.max_file_size.to_le_bytes());
        let checksum = crc32c(&encoded[0..44]);
        encoded[44..48].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() != FILE_HEADER_ENCODED_LEN || encoded.get(0..8) != Some(FILE_HEADER_MAGIC)
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let stored_crc = read_u32(encoded, 44)?;
        if crc32c(&encoded[0..44]) != stored_crc {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let format_version = read_u32(encoded, 8)?;
        if format_version != FILE_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }
        let mut database_uuid = [0_u8; 16];
        database_uuid.copy_from_slice(
            encoded
                .get(12..28)
                .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
        );
        let header = Self {
            format_version,
            database_uuid,
            file_id: read_u32(encoded, 28)?,
            page_size: read_u32(encoded, 32)?,
            max_file_size: read_u64(encoded, 36)?,
        };
        header.validate()?;
        Ok(header)
    }

    fn validate(self) -> Result<()> {
        if self.format_version != FILE_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }
        if self.database_uuid.iter().all(|byte| *byte == 0)
            || self.file_id > MAX_VLOG_FILE_ID
            || self.page_size != VLOG_PAGE_SIZE as u32
            || self.max_file_size != MAX_VLOG_FILE_SIZE
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordHeader {
    pub(crate) format_version: u16,
    pub(crate) record_type: RecordType,
    pub(crate) encoded_len: u32,
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
}

impl RecordHeader {
    fn new(record_type: RecordType, encoded_len: u32, commit_seq: u64, tx_uuid: [u8; 16]) -> Self {
        Self {
            format_version: VLOG_FORMAT_VERSION,
            record_type,
            encoded_len,
            commit_seq,
            tx_uuid,
        }
    }

    fn encode(self) -> Result<[u8; RECORD_HEADER_ENCODED_LEN]> {
        self.validate().map_err(|error| {
            invalid_vlog_error(if error.kind == StorageErrorKind::IncompatibleFormat {
                StorageErrorKind::IncompatibleFormat
            } else {
                StorageErrorKind::InvalidArgument
            })
        })?;

        let mut encoded = [0_u8; RECORD_HEADER_ENCODED_LEN];
        encoded[0..4].copy_from_slice(RECORD_HEADER_MAGIC);
        encoded[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        encoded[6] = self.record_type as u8;
        encoded[7..11].copy_from_slice(&self.encoded_len.to_le_bytes());
        encoded[11..19].copy_from_slice(&self.commit_seq.to_le_bytes());
        encoded[19..35].copy_from_slice(&self.tx_uuid);
        let checksum = crc32c(&encoded[0..35]);
        encoded[35..39].copy_from_slice(&checksum.to_le_bytes());
        Ok(encoded)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() < RECORD_HEADER_ENCODED_LEN
            || encoded.get(0..4) != Some(RECORD_HEADER_MAGIC)
        {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let stored_crc = read_u32(encoded, 35)?;
        if crc32c(&encoded[0..35]) != stored_crc {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let format_version = read_u16(encoded, 4)?;
        if format_version != VLOG_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }
        let header = Self {
            format_version,
            record_type: RecordType::decode(
                *encoded
                    .get(6)
                    .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
            )?,
            encoded_len: read_u32(encoded, 7)?,
            commit_seq: read_u64(encoded, 11)?,
            tx_uuid: read_array_16(encoded, 19)?,
        };
        header.validate()?;
        Ok(header)
    }

    fn validate(self) -> Result<()> {
        if self.format_version != VLOG_FORMAT_VERSION {
            return Err(decode_error(StorageErrorKind::IncompatibleFormat));
        }
        if self.commit_seq == 0 || self.tx_uuid.iter().all(|byte| *byte == 0) {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TxBegin {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) vlog_begin: VLogPosition,
    pub(crate) logical_op_count: u64,
    pub(crate) distinct_key_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TxPreparedEnd {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) vlog_begin: VLogPosition,
    pub(crate) vlog_end: VLogPosition,
    pub(crate) logical_op_count: u64,
    pub(crate) distinct_key_count: u64,
    pub(crate) kv_record_count: u64,
    pub(crate) delete_record_count: u64,
    pub(crate) envelope_crc32c: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KvRecordRef<'a> {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) op_index: u64,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteRecordRef<'a> {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) op_index: u64,
    pub(crate) key: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodedRecord<'a> {
    TxBegin(TxBegin),
    KvRecord(KvRecordRef<'a>),
    DeleteRecord(DeleteRecordRef<'a>),
    TxPreparedEnd(TxPreparedEnd),
    PageEnd,
}

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

pub(crate) fn encode_tx_begin(begin: TxBegin) -> Result<Vec<u8>> {
    validate_tx_begin(begin).map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let mut encoded = zeroed_record(TX_BEGIN_ENCODED_LEN)?;
    write_record_header(
        &mut encoded,
        RecordHeader::new(
            RecordType::TxBegin,
            TX_BEGIN_ENCODED_LEN,
            begin.commit_seq,
            begin.tx_uuid,
        ),
    )?;
    encoded[39..43].copy_from_slice(&begin.vlog_begin.file_id.to_le_bytes());
    encoded[43..51].copy_from_slice(&begin.vlog_begin.offset.to_le_bytes());
    encoded[51..59].copy_from_slice(&begin.logical_op_count.to_le_bytes());
    encoded[59..67].copy_from_slice(&begin.distinct_key_count.to_le_bytes());
    write_standard_record_crc(&mut encoded)?;
    Ok(encoded)
}

pub(crate) fn encode_kv_record(record: KvRecordRef<'_>) -> Result<Vec<u8>> {
    validate_identity(record.commit_seq, record.tx_uuid)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let key_len = u16::try_from(record.key.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let value_len = u16::try_from(record.value.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let payload_len = record
        .key
        .len()
        .checked_add(record.value.len())
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    if key_len == 0 || payload_len > MAX_KEY_VALUE_SIZE as usize {
        return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
    }
    let encoded_len = KV_RECORD_FIXED_LEN_WITH_CRC
        .checked_add(
            u32::try_from(payload_len)
                .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?,
        )
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let mut encoded = zeroed_record(encoded_len)?;
    write_record_header(
        &mut encoded,
        RecordHeader::new(
            RecordType::KvRecord,
            encoded_len,
            record.commit_seq,
            record.tx_uuid,
        ),
    )?;
    encoded[39..47].copy_from_slice(&record.op_index.to_le_bytes());
    encoded[47..49].copy_from_slice(&key_len.to_le_bytes());
    encoded[49..51].copy_from_slice(&value_len.to_le_bytes());
    let key_end = 51_usize
        .checked_add(record.key.len())
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let value_end = key_end
        .checked_add(record.value.len())
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    encoded[51..key_end].copy_from_slice(record.key);
    encoded[key_end..value_end].copy_from_slice(record.value);
    write_standard_record_crc(&mut encoded)?;
    Ok(encoded)
}

pub(crate) fn encode_delete_record(record: DeleteRecordRef<'_>) -> Result<Vec<u8>> {
    validate_identity(record.commit_seq, record.tx_uuid)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let key_len = u16::try_from(record.key.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    if key_len == 0 || record.key.len() > MAX_KEY_VALUE_SIZE as usize {
        return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
    }
    let encoded_len = 53_u32
        .checked_add(u32::from(key_len))
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let mut encoded = zeroed_record(encoded_len)?;
    write_record_header(
        &mut encoded,
        RecordHeader::new(
            RecordType::DeleteRecord,
            encoded_len,
            record.commit_seq,
            record.tx_uuid,
        ),
    )?;
    encoded[39..47].copy_from_slice(&record.op_index.to_le_bytes());
    encoded[47..49].copy_from_slice(&key_len.to_le_bytes());
    let key_end = 49_usize
        .checked_add(record.key.len())
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    encoded[49..key_end].copy_from_slice(record.key);
    write_standard_record_crc(&mut encoded)?;
    Ok(encoded)
}

pub(crate) fn encode_tx_prepared_end(footer: TxPreparedEnd) -> Result<Vec<u8>> {
    validate_tx_prepared_end(footer)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let mut encoded = zeroed_record(TX_PREPARED_END_ENCODED_LEN)?;
    write_record_header(
        &mut encoded,
        RecordHeader::new(
            RecordType::TxPreparedEnd,
            TX_PREPARED_END_ENCODED_LEN,
            footer.commit_seq,
            footer.tx_uuid,
        ),
    )?;
    encoded[39..43].copy_from_slice(&footer.vlog_begin.file_id.to_le_bytes());
    encoded[43..51].copy_from_slice(&footer.vlog_begin.offset.to_le_bytes());
    encoded[51..55].copy_from_slice(&footer.vlog_end.file_id.to_le_bytes());
    encoded[55..63].copy_from_slice(&footer.vlog_end.offset.to_le_bytes());
    encoded[63..71].copy_from_slice(&footer.logical_op_count.to_le_bytes());
    encoded[71..79].copy_from_slice(&footer.distinct_key_count.to_le_bytes());
    encoded[79..87].copy_from_slice(&footer.kv_record_count.to_le_bytes());
    encoded[87..95].copy_from_slice(&footer.delete_record_count.to_le_bytes());
    encoded[95..99].copy_from_slice(&footer.envelope_crc32c.to_le_bytes());
    encoded[103..107].copy_from_slice(&TX_PREPARED_END_ENCODED_LEN.to_le_bytes());
    encoded[107..111].copy_from_slice(END_TRAILER_MAGIC);
    let checksum = crc32c_append(crc32c(&encoded[0..99]), &encoded[103..111]);
    encoded[99..103].copy_from_slice(&checksum.to_le_bytes());
    Ok(encoded)
}

pub(crate) fn encode_page_end(
    commit_seq: u64,
    tx_uuid: [u8; 16],
    record_start: VLogPosition,
    geometry: VLogGeometry,
) -> Result<Vec<u8>> {
    validate_identity(commit_seq, tx_uuid)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    geometry.validate()?;
    validate_record_start(record_start, geometry)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let page_end = page_end(record_start.offset, geometry)?;
    let encoded_len_u64 = page_end
        .checked_sub(record_start.offset)
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let encoded_len = u32::try_from(encoded_len_u64)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    if encoded_len < PAGE_END_MIN_SIZE {
        return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
    }

    let mut encoded = zeroed_record(encoded_len)?;
    write_record_header(
        &mut encoded,
        RecordHeader::new(RecordType::PageEnd, encoded_len, commit_seq, tx_uuid),
    )?;
    write_standard_record_crc(&mut encoded)?;
    Ok(encoded)
}

pub(crate) fn decode_record_at<'a>(
    encoded: &'a [u8],
    record_start: VLogPosition,
    geometry: VLogGeometry,
) -> Result<DecodedRecord<'a>> {
    geometry.validate_for_decode()?;
    validate_record_bounds(encoded, record_start, geometry)?;
    let header = RecordHeader::decode(encoded)?;
    let encoded_len = usize::try_from(header.encoded_len)
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    if encoded_len != encoded.len() {
        return Err(decode_error(StorageErrorKind::Corruption));
    }

    match header.record_type {
        RecordType::TxBegin => decode_tx_begin(encoded, header).map(DecodedRecord::TxBegin),
        RecordType::KvRecord => decode_kv_record(encoded, header).map(DecodedRecord::KvRecord),
        RecordType::DeleteRecord => {
            decode_delete_record(encoded, header).map(DecodedRecord::DeleteRecord)
        }
        RecordType::TxPreparedEnd => {
            let footer = decode_tx_prepared_end(encoded, header)?;
            let physical_end = record_start
                .offset
                .checked_add(u64::from(header.encoded_len))
                .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
            if footer.vlog_end
                != (VLogPosition {
                    file_id: record_start.file_id,
                    offset: physical_end,
                })
            {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            Ok(DecodedRecord::TxPreparedEnd(footer))
        }
        RecordType::PageEnd => {
            decode_page_end(encoded, record_start, geometry)?;
            Ok(DecodedRecord::PageEnd)
        }
    }
}

fn decode_tx_begin(encoded: &[u8], header: RecordHeader) -> Result<TxBegin> {
    if header.encoded_len != TX_BEGIN_ENCODED_LEN {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    verify_standard_record_crc(encoded)?;
    let begin = TxBegin {
        commit_seq: header.commit_seq,
        tx_uuid: header.tx_uuid,
        vlog_begin: VLogPosition {
            file_id: read_u32(encoded, 39)?,
            offset: read_u64(encoded, 43)?,
        },
        logical_op_count: read_u64(encoded, 51)?,
        distinct_key_count: read_u64(encoded, 59)?,
    };
    validate_tx_begin(begin)?;
    Ok(begin)
}

fn decode_kv_record<'a>(encoded: &'a [u8], header: RecordHeader) -> Result<KvRecordRef<'a>> {
    if header.encoded_len < MIN_KV_RECORD_LEN || header.encoded_len > MAX_KV_RECORD_LEN {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let key_len = usize::from(read_u16(encoded, 47)?);
    let value_len = usize::from(read_u16(encoded, 49)?);
    let payload_len = key_len
        .checked_add(value_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if key_len == 0 || payload_len > MAX_KEY_VALUE_SIZE as usize {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let expected_len = 55_usize
        .checked_add(payload_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if expected_len != encoded.len() {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    verify_standard_record_crc(encoded)?;
    let key_end = 51_usize
        .checked_add(key_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let value_end = key_end
        .checked_add(value_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let key = encoded
        .get(51..key_end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let value = encoded
        .get(key_end..value_end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    Ok(KvRecordRef {
        commit_seq: header.commit_seq,
        tx_uuid: header.tx_uuid,
        op_index: read_u64(encoded, 39)?,
        key,
        value,
    })
}

fn decode_delete_record<'a>(
    encoded: &'a [u8],
    header: RecordHeader,
) -> Result<DeleteRecordRef<'a>> {
    if header.encoded_len < MIN_DELETE_RECORD_LEN || header.encoded_len > 60_053 {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let key_len = usize::from(read_u16(encoded, 47)?);
    if key_len == 0 || key_len > MAX_KEY_VALUE_SIZE as usize {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let expected_len = 53_usize
        .checked_add(key_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if expected_len != encoded.len() {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    verify_standard_record_crc(encoded)?;
    let key_end = 49_usize
        .checked_add(key_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let key = encoded
        .get(49..key_end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    Ok(DeleteRecordRef {
        commit_seq: header.commit_seq,
        tx_uuid: header.tx_uuid,
        op_index: read_u64(encoded, 39)?,
        key,
    })
}

fn decode_tx_prepared_end(encoded: &[u8], header: RecordHeader) -> Result<TxPreparedEnd> {
    if header.encoded_len != TX_PREPARED_END_ENCODED_LEN
        || encoded.get(107..111) != Some(END_TRAILER_MAGIC)
        || read_u32(encoded, 103)? != TX_PREPARED_END_ENCODED_LEN
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let stored_crc = read_u32(encoded, 99)?;
    let calculated_crc = crc32c_append(crc32c(&encoded[0..99]), &encoded[103..111]);
    if stored_crc != calculated_crc {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let footer = TxPreparedEnd {
        commit_seq: header.commit_seq,
        tx_uuid: header.tx_uuid,
        vlog_begin: VLogPosition {
            file_id: read_u32(encoded, 39)?,
            offset: read_u64(encoded, 43)?,
        },
        vlog_end: VLogPosition {
            file_id: read_u32(encoded, 51)?,
            offset: read_u64(encoded, 55)?,
        },
        logical_op_count: read_u64(encoded, 63)?,
        distinct_key_count: read_u64(encoded, 71)?,
        kv_record_count: read_u64(encoded, 79)?,
        delete_record_count: read_u64(encoded, 87)?,
        envelope_crc32c: read_u32(encoded, 95)?,
    };
    validate_tx_prepared_end(footer)?;
    Ok(footer)
}

fn decode_page_end(
    encoded: &[u8],
    record_start: VLogPosition,
    geometry: VLogGeometry,
) -> Result<()> {
    let encoded_len =
        u32::try_from(encoded.len()).map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    if encoded_len < PAGE_END_MIN_SIZE {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let end = record_start
        .offset
        .checked_add(u64::from(encoded_len))
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if end != page_end(record_start.offset, geometry)? {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let padding_end = encoded
        .len()
        .checked_sub(RECORD_CRC_LEN as usize)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let padding = encoded
        .get(RECORD_HEADER_ENCODED_LEN..padding_end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if padding.iter().any(|byte| *byte != 0) {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    verify_standard_record_crc(encoded)
}

fn validate_tx_begin(begin: TxBegin) -> Result<()> {
    validate_identity(begin.commit_seq, begin.tx_uuid)?;
    begin.vlog_begin.validate()?;
    if begin.logical_op_count == 0
        || begin.distinct_key_count == 0
        || begin.distinct_key_count > begin.logical_op_count
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

fn validate_tx_prepared_end(footer: TxPreparedEnd) -> Result<()> {
    validate_identity(footer.commit_seq, footer.tx_uuid)?;
    footer.vlog_begin.validate()?;
    footer.vlog_end.validate()?;
    let actual_count = footer
        .kv_record_count
        .checked_add(footer.delete_record_count)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if footer.vlog_begin >= footer.vlog_end
        || footer.logical_op_count == 0
        || footer.distinct_key_count == 0
        || footer.distinct_key_count > footer.logical_op_count
        || actual_count != footer.logical_op_count
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

fn validate_identity(commit_seq: u64, tx_uuid: [u8; 16]) -> Result<()> {
    if commit_seq == 0 || tx_uuid.iter().all(|byte| *byte == 0) {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

fn zeroed_record(encoded_len: u32) -> Result<Vec<u8>> {
    let len = usize::try_from(encoded_len)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let mut encoded = Vec::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::RecordBytes)?;
    encoded
        .try_reserve_exact(len)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    encoded.resize(len, 0);
    Ok(encoded)
}

fn write_record_header(encoded: &mut [u8], header: RecordHeader) -> Result<()> {
    let destination = encoded
        .get_mut(0..RECORD_HEADER_ENCODED_LEN)
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    destination.copy_from_slice(&header.encode()?);
    Ok(())
}

fn write_standard_record_crc(encoded: &mut [u8]) -> Result<()> {
    let crc_offset = encoded
        .len()
        .checked_sub(RECORD_CRC_LEN as usize)
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    let checksum = crc32c(
        encoded
            .get(0..crc_offset)
            .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?,
    );
    encoded
        .get_mut(crc_offset..)
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?
        .copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

fn verify_standard_record_crc(encoded: &[u8]) -> Result<()> {
    let crc_offset = encoded
        .len()
        .checked_sub(RECORD_CRC_LEN as usize)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let stored_crc = read_u32(encoded, crc_offset)?;
    let calculated_crc = crc32c(
        encoded
            .get(0..crc_offset)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
    );
    if stored_crc != calculated_crc {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EnvelopeCrc32c {
    checksum: u32,
}

impl EnvelopeCrc32c {
    pub(crate) fn new() -> Self {
        Self {
            checksum: crc32c(ENVELOPE_CRC_MAGIC),
        }
    }

    pub(crate) fn update_tx_begin(&mut self, begin: TxBegin) -> Result<()> {
        validate_tx_begin(begin)?;
        self.update_record_prefix(RecordType::TxBegin, 52);
        self.update_u64(begin.commit_seq);
        self.update(&begin.tx_uuid);
        self.update_position(begin.vlog_begin);
        self.update_u64(begin.logical_op_count);
        self.update_u64(begin.distinct_key_count);
        Ok(())
    }

    pub(crate) fn update_kv_record(&mut self, record: KvRecordRef<'_>) -> Result<()> {
        validate_identity(record.commit_seq, record.tx_uuid)?;
        let key_len = u16::try_from(record.key.len())
            .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
        let value_len = u16::try_from(record.value.len())
            .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
        let payload_len = record
            .key
            .len()
            .checked_add(record.value.len())
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        if key_len == 0 || payload_len > MAX_KEY_VALUE_SIZE as usize {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let body_len = 36_u32
            .checked_add(
                u32::try_from(payload_len)
                    .map_err(|_| decode_error(StorageErrorKind::Corruption))?,
            )
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        self.update_record_prefix(RecordType::KvRecord, body_len);
        self.update_u64(record.commit_seq);
        self.update(&record.tx_uuid);
        self.update_u64(record.op_index);
        self.update(&key_len.to_le_bytes());
        self.update(&value_len.to_le_bytes());
        self.update(record.key);
        self.update(record.value);
        Ok(())
    }

    pub(crate) fn update_delete_record(&mut self, record: DeleteRecordRef<'_>) -> Result<()> {
        validate_identity(record.commit_seq, record.tx_uuid)?;
        let key_len = u16::try_from(record.key.len())
            .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
        if key_len == 0 || record.key.len() > MAX_KEY_VALUE_SIZE as usize {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let body_len = 34_u32
            .checked_add(u32::from(key_len))
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
        self.update_record_prefix(RecordType::DeleteRecord, body_len);
        self.update_u64(record.commit_seq);
        self.update(&record.tx_uuid);
        self.update_u64(record.op_index);
        self.update(&key_len.to_le_bytes());
        self.update(record.key);
        Ok(())
    }

    pub(crate) fn finish_with_footer(mut self, footer: TxPreparedEnd) -> Result<u32> {
        validate_tx_prepared_end(footer)?;
        self.update_record_prefix(RecordType::TxPreparedEnd, 80);
        self.update_u64(footer.commit_seq);
        self.update(&footer.tx_uuid);
        self.update_position(footer.vlog_begin);
        self.update_position(footer.vlog_end);
        self.update_u64(footer.logical_op_count);
        self.update_u64(footer.distinct_key_count);
        self.update_u64(footer.kv_record_count);
        self.update_u64(footer.delete_record_count);
        Ok(self.checksum)
    }

    fn update_record_prefix(&mut self, record_type: RecordType, canonical_body_len: u32) {
        self.update(&[record_type as u8]);
        self.update(&canonical_body_len.to_le_bytes());
    }

    fn update_position(&mut self, position: VLogPosition) {
        self.update(&position.file_id.to_le_bytes());
        self.update(&position.offset.to_le_bytes());
    }

    fn update_u64(&mut self, value: u64) {
        self.update(&value.to_le_bytes());
    }

    fn update(&mut self, bytes: &[u8]) {
        self.checksum = crc32c_append(self.checksum, bytes);
    }
}

impl Default for EnvelopeCrc32c {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VLogGeometry {
    pub(crate) page_size: u64,
    pub(crate) max_file_size: u64,
    pub(crate) max_file_id: u32,
}

impl VLogGeometry {
    pub(crate) const PRODUCTION: Self = Self {
        page_size: VLOG_PAGE_SIZE,
        max_file_size: MAX_VLOG_FILE_SIZE,
        max_file_id: MAX_VLOG_FILE_ID,
    };

    pub(crate) fn test_only(page_size: u64, max_file_size: u64, max_file_id: u32) -> Result<Self> {
        let geometry = Self {
            page_size,
            max_file_size,
            max_file_id,
        };
        geometry.validate()?;
        Ok(geometry)
    }

    fn validate(self) -> Result<()> {
        if self.page_size <= FIRST_PAGE_RECORD_AREA_START
            || self.page_size > VLOG_PAGE_SIZE
            || self.max_file_size == 0
            || self.max_file_size > MAX_VLOG_FILE_SIZE
            || !self.max_file_size.is_multiple_of(self.page_size)
            || self.max_file_id > MAX_VLOG_FILE_ID
        {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        let page_count = self
            .max_file_size
            .checked_div(self.page_size)
            .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
        if page_count == 0 || page_count > 65_536 {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        Ok(())
    }

    fn validate_for_decode(self) -> Result<()> {
        self.validate()
            .map_err(|_| decode_error(StorageErrorKind::Corruption))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LayoutPrelude {
    PageHeader {
        position: VLogPosition,
        page_no: u32,
    },
    FileHeader {
        position: VLogPosition,
    },
    PageEnd {
        position: VLogPosition,
        encoded_len: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordPlacement {
    pub(crate) preludes: Vec<LayoutPrelude>,
    pub(crate) record_start: VLogPosition,
    pub(crate) encoded_len: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LayoutPlanner {
    geometry: VLogGeometry,
    position: VLogPosition,
}

impl LayoutPlanner {
    pub(crate) fn empty(geometry: VLogGeometry) -> Result<Self> {
        geometry.validate()?;
        Ok(Self {
            geometry,
            position: VLogPosition {
                file_id: 0,
                offset: 0,
            },
        })
    }

    pub(crate) fn from_position(geometry: VLogGeometry, position: VLogPosition) -> Result<Self> {
        geometry.validate()?;
        if position.file_id > geometry.max_file_id || position.offset > geometry.max_file_size {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        if position.offset != 0 && position.offset != geometry.max_file_size {
            let page_offset = position.offset % geometry.page_size;
            let minimum = if position.offset < geometry.page_size {
                FIRST_PAGE_RECORD_AREA_START
            } else {
                OTHER_PAGE_RECORD_AREA_OFFSET
            };
            if page_offset != 0 && page_offset < minimum {
                return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
            }
        }
        Ok(Self { geometry, position })
    }

    pub(crate) fn position(&self) -> VLogPosition {
        self.position
    }

    pub(crate) fn geometry(&self) -> VLogGeometry {
        self.geometry
    }

    pub(crate) fn plan_record(&mut self, encoded_len: u32) -> Result<RecordPlacement> {
        if encoded_len < PAGE_END_MIN_SIZE || u64::from(encoded_len) > self.geometry.page_size {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        let mut candidate = self.clone();
        let placement = candidate.plan_record_inner(encoded_len)?;
        *self = candidate;
        Ok(placement)
    }

    fn plan_record_inner(&mut self, encoded_len: u32) -> Result<RecordPlacement> {
        let mut preludes = Vec::new();
        inject_prepare_allocation_failure(PrepareAllocationFailureSite::PlacementPreludes)?;
        preludes
            .try_reserve(4)
            .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
        self.ensure_record_area(&mut preludes)?;

        let mut remaining = remaining_in_page(self.position.offset, self.geometry)?;
        let record_len = u64::from(encoded_len);
        let remainder = remaining.checked_sub(record_len);
        let needs_page_end = record_len > remaining || matches!(remainder, Some(1..=42));
        if needs_page_end {
            if remaining < u64::from(PAGE_END_MIN_SIZE) {
                return Err(decode_error(StorageErrorKind::InvalidLayout));
            }
            let page_end_len = u32::try_from(remaining)
                .map_err(|_| decode_error(StorageErrorKind::InvalidLayout))?;
            preludes.push(LayoutPrelude::PageEnd {
                position: self.position,
                encoded_len: page_end_len,
            });
            self.position.offset = self
                .position
                .offset
                .checked_add(remaining)
                .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
            self.ensure_record_area(&mut preludes)?;
            remaining = remaining_in_page(self.position.offset, self.geometry)?;
        }

        if record_len > remaining {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        let after = remaining
            .checked_sub(record_len)
            .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
        if after != 0 && after < u64::from(PAGE_END_MIN_SIZE) {
            return Err(decode_error(StorageErrorKind::InvalidLayout));
        }

        let record_start = self.position;
        self.position.offset = self
            .position
            .offset
            .checked_add(record_len)
            .ok_or_else(capacity_error)?;
        if self.position.offset > self.geometry.max_file_size {
            return Err(capacity_error());
        }
        Ok(RecordPlacement {
            preludes,
            record_start,
            encoded_len,
        })
    }

    fn ensure_record_area(&mut self, preludes: &mut Vec<LayoutPrelude>) -> Result<()> {
        if self.position.offset == self.geometry.max_file_size {
            if self.position.file_id >= self.geometry.max_file_id {
                return Err(capacity_error());
            }
            self.position.file_id = self
                .position
                .file_id
                .checked_add(1)
                .ok_or_else(capacity_error)?;
            self.position.offset = 0;
        }

        if self.position.offset == 0 {
            preludes.push(LayoutPrelude::PageHeader {
                position: self.position,
                page_no: 0,
            });
            let file_header_position = VLogPosition {
                file_id: self.position.file_id,
                offset: PAGE_HEADER_ENCODED_LEN as u64,
            };
            preludes.push(LayoutPrelude::FileHeader {
                position: file_header_position,
            });
            self.position.offset = FIRST_PAGE_RECORD_AREA_START;
        } else if self.position.offset.is_multiple_of(self.geometry.page_size) {
            let page_no_u64 = self
                .position
                .offset
                .checked_div(self.geometry.page_size)
                .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
            let page_no = u32::try_from(page_no_u64)
                .map_err(|_| decode_error(StorageErrorKind::InvalidLayout))?;
            preludes.push(LayoutPrelude::PageHeader {
                position: self.position,
                page_no,
            });
            self.position.offset = self
                .position
                .offset
                .checked_add(OTHER_PAGE_RECORD_AREA_OFFSET)
                .ok_or_else(capacity_error)?;
        }
        validate_record_start(self.position, self.geometry)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogicalOperationRef<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrepareAllocationFailureSite {
    DistinctKeys,
    RecordLengths,
    PlacementPreludes,
    Placements,
    Chunks,
    ValuePointers,
    RecordBytes,
    StructuralBytes,
}

fn operation_key<'a>(operation: LogicalOperationRef<'a>) -> &'a [u8] {
    match operation {
        LogicalOperationRef::Put { key, .. } | LogicalOperationRef::Delete { key } => key,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PhysicalChunk {
    pub(crate) position: VLogPosition,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedEnvelope {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) vlog_begin: VLogPosition,
    pub(crate) vlog_end: VLogPosition,
    pub(crate) envelope_crc32c: u32,
    pub(crate) chunks: Vec<PhysicalChunk>,
    pub(crate) value_pointers: Vec<Option<ValuePointer>>,
}

pub(crate) fn prepare_envelope(
    planner: &mut LayoutPlanner,
    database_uuid: [u8; 16],
    commit_seq: u64,
    tx_uuid: [u8; 16],
    operations: &[LogicalOperationRef<'_>],
) -> Result<PreparedEnvelope> {
    validate_identity(commit_seq, tx_uuid)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
    if database_uuid.iter().all(|byte| *byte == 0) || operations.is_empty() {
        return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
    }

    let logical_op_count = u64::try_from(operations.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
    let mut distinct_keys = HashSet::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::DistinctKeys)?;
    distinct_keys
        .try_reserve(operations.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    let mut kv_record_count = 0_u64;
    let mut delete_record_count = 0_u64;
    let mut record_lengths = Vec::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::RecordLengths)?;
    record_lengths
        .try_reserve(
            operations
                .len()
                .checked_add(2)
                .ok_or_else(|| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?,
        )
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    record_lengths.push(TX_BEGIN_ENCODED_LEN);
    for operation in operations.iter().copied() {
        let key = operation_key(operation);
        if key.is_empty() || key.len() > MAX_KEY_VALUE_SIZE as usize {
            return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
        }
        distinct_keys.insert(key);
        match operation {
            LogicalOperationRef::Put { value, .. } => {
                let combined_len = key
                    .len()
                    .checked_add(value.len())
                    .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                let key_len = u16::try_from(key.len())
                    .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                let value_len = u16::try_from(value.len())
                    .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                if key_len == 0 || combined_len > MAX_KEY_VALUE_SIZE as usize {
                    return Err(invalid_vlog_error(StorageErrorKind::InvalidArgument));
                }
                let encoded_len = KV_RECORD_FIXED_LEN_WITH_CRC
                    .checked_add(
                        u32::try_from(combined_len)
                            .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?,
                    )
                    .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                let _ = value_len;
                record_lengths.push(encoded_len);
                kv_record_count = kv_record_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
            }
            LogicalOperationRef::Delete { .. } => {
                let key_len = u16::try_from(key.len())
                    .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                let encoded_len = 53_u32
                    .checked_add(u32::from(key_len))
                    .ok_or_else(|| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                record_lengths.push(encoded_len);
                delete_record_count = delete_record_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
            }
        }
    }
    record_lengths.push(TX_PREPARED_END_ENCODED_LEN);
    let distinct_key_count = u64::try_from(distinct_keys.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;

    let vlog_begin = planner.position();
    let mut candidate = planner.clone();
    let mut placements = Vec::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::Placements)?;
    placements
        .try_reserve(record_lengths.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    for encoded_len in record_lengths {
        placements.push(candidate.plan_record(encoded_len)?);
    }
    let vlog_end = candidate.position();

    let begin = TxBegin {
        commit_seq,
        tx_uuid,
        vlog_begin,
        logical_op_count,
        distinct_key_count,
    };
    let mut footer = TxPreparedEnd {
        commit_seq,
        tx_uuid,
        vlog_begin,
        vlog_end,
        logical_op_count,
        distinct_key_count,
        kv_record_count,
        delete_record_count,
        envelope_crc32c: 0,
    };
    let mut envelope_crc = EnvelopeCrc32c::new();
    envelope_crc.update_tx_begin(begin)?;
    for (op_index, operation) in operations.iter().copied().enumerate() {
        let op_index = u64::try_from(op_index)
            .map_err(|_| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
        match operation {
            LogicalOperationRef::Put { key, value } => {
                envelope_crc.update_kv_record(KvRecordRef {
                    commit_seq,
                    tx_uuid,
                    op_index,
                    key,
                    value,
                })?;
            }
            LogicalOperationRef::Delete { key } => {
                envelope_crc.update_delete_record(DeleteRecordRef {
                    commit_seq,
                    tx_uuid,
                    op_index,
                    key,
                })?;
            }
        }
    }
    footer.envelope_crc32c = envelope_crc.finish_with_footer(footer)?;

    let chunk_capacity = placements
        .iter()
        .try_fold(placements.len(), |total, placement| {
            total.checked_add(placement.preludes.len())
        })
        .ok_or_else(|| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
    let mut chunks = Vec::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::Chunks)?;
    chunks
        .try_reserve(chunk_capacity)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    let mut value_pointers = Vec::new();
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::ValuePointers)?;
    value_pointers
        .try_reserve(operations.len())
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;

    let begin_bytes = encode_tx_begin(begin)?;
    let begin_placement = placements
        .first()
        .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
    append_placement(
        &mut chunks,
        begin_placement,
        begin_bytes,
        database_uuid,
        commit_seq,
        tx_uuid,
        planner.geometry(),
    )?;
    for (operation_index, operation) in operations.iter().copied().enumerate() {
        let placement_index = operation_index
            .checked_add(1)
            .ok_or_else(|| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
        let placement = placements
            .get(placement_index)
            .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
        let op_index = u64::try_from(operation_index)
            .map_err(|_| invalid_vlog_error(StorageErrorKind::CapacityExceeded))?;
        let record_bytes = match operation {
            LogicalOperationRef::Put { key, value } => {
                let bytes = encode_kv_record(KvRecordRef {
                    commit_seq,
                    tx_uuid,
                    op_index,
                    key,
                    value,
                })?;
                let record_offset = u32::try_from(placement.record_start.offset)
                    .map_err(|_| decode_error(StorageErrorKind::InvalidLayout))?;
                let value_len = u16::try_from(value.len())
                    .map_err(|_| invalid_vlog_error(StorageErrorKind::InvalidArgument))?;
                value_pointers.push(Some(ValuePointer {
                    format_version: VALUE_POINTER_FORMAT_VERSION,
                    file_id: placement.record_start.file_id,
                    record_offset,
                    record_len: placement.encoded_len,
                    value_len,
                }));
                bytes
            }
            LogicalOperationRef::Delete { key } => {
                value_pointers.push(None);
                encode_delete_record(DeleteRecordRef {
                    commit_seq,
                    tx_uuid,
                    op_index,
                    key,
                })?
            }
        };
        append_placement(
            &mut chunks,
            placement,
            record_bytes,
            database_uuid,
            commit_seq,
            tx_uuid,
            planner.geometry(),
        )?;
    }
    let footer_placement = placements
        .last()
        .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
    append_placement(
        &mut chunks,
        footer_placement,
        encode_tx_prepared_end(footer)?,
        database_uuid,
        commit_seq,
        tx_uuid,
        planner.geometry(),
    )?;

    *planner = candidate;
    Ok(PreparedEnvelope {
        commit_seq,
        tx_uuid,
        vlog_begin,
        vlog_end,
        envelope_crc32c: footer.envelope_crc32c,
        chunks,
        value_pointers,
    })
}

fn append_placement(
    chunks: &mut Vec<PhysicalChunk>,
    placement: &RecordPlacement,
    record_bytes: Vec<u8>,
    database_uuid: [u8; 16],
    commit_seq: u64,
    tx_uuid: [u8; 16],
    geometry: VLogGeometry,
) -> Result<()> {
    if record_bytes.len() != placement.encoded_len as usize {
        return Err(decode_error(StorageErrorKind::InvalidLayout));
    }
    for prelude in placement.preludes.iter().copied() {
        let chunk = match prelude {
            LayoutPrelude::PageHeader { position, page_no } => {
                let encoded = PageHeader {
                    file_id: position.file_id,
                    page_no,
                }
                .encode()?;
                PhysicalChunk {
                    position,
                    bytes: try_copy_structural_bytes(&encoded)?,
                }
            }
            LayoutPrelude::FileHeader { position } => {
                let encoded = VLogFileHeader::new(database_uuid, position.file_id).encode()?;
                PhysicalChunk {
                    position,
                    bytes: try_copy_structural_bytes(&encoded)?,
                }
            }
            LayoutPrelude::PageEnd {
                position,
                encoded_len,
            } => {
                let bytes = encode_page_end(commit_seq, tx_uuid, position, geometry)?;
                if bytes.len() != encoded_len as usize {
                    return Err(decode_error(StorageErrorKind::InvalidLayout));
                }
                PhysicalChunk { position, bytes }
            }
        };
        chunks.push(chunk);
    }
    chunks.push(PhysicalChunk {
        position: placement.record_start,
        bytes: record_bytes,
    });
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScannedEnvelope {
    pub(crate) commit_seq: u64,
    pub(crate) tx_uuid: [u8; 16],
    pub(crate) vlog_begin: VLogPosition,
    pub(crate) vlog_end: VLogPosition,
    pub(crate) logical_op_count: u64,
    pub(crate) distinct_key_count: u64,
    pub(crate) kv_record_count: u64,
    pub(crate) delete_record_count: u64,
    pub(crate) envelope_crc32c: u32,
}

pub(crate) fn scan_prepared_envelope(
    chunks: &[PhysicalChunk],
    geometry: VLogGeometry,
    database_uuid: [u8; 16],
    vlog_begin: VLogPosition,
    vlog_end: VLogPosition,
    expected_envelope_crc32c: Option<u32>,
) -> Result<ScannedEnvelope> {
    geometry.validate_for_decode()?;
    let first_chunk_position = next_chunk_position(vlog_begin, geometry)?;
    if chunks.is_empty()
        || database_uuid.iter().all(|byte| *byte == 0)
        || vlog_begin >= vlog_end
        || chunks.first().map(|chunk| chunk.position) != Some(first_chunk_position)
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }

    let mut physical_end = vlog_begin;
    let mut identity: Option<(u64, [u8; 16])> = None;
    let mut begin: Option<TxBegin> = None;
    let mut footer: Option<TxPreparedEnd> = None;
    let mut envelope_crc = EnvelopeCrc32c::new();
    let mut logical_op_count = 0_u64;
    let mut kv_record_count = 0_u64;
    let mut delete_record_count = 0_u64;
    let mut distinct_keys: HashSet<&[u8]> = HashSet::new();
    distinct_keys
        .try_reserve(chunks.len())
        .map_err(|_| decode_error(StorageErrorKind::ResourceExhausted))?;
    let mut previous_transaction_record_was_page_end = false;
    let mut expecting_file_header = false;

    for (chunk_index, chunk) in chunks.iter().enumerate() {
        let mut transaction_record = false;
        if chunk.bytes.is_empty() {
            return Err(decode_error(StorageErrorKind::Corruption));
        }
        let expected_start = if chunk_index == 0 {
            first_chunk_position
        } else {
            next_chunk_position(physical_end, geometry)?
        };
        if chunk.position != expected_start {
            return Err(decode_error(StorageErrorKind::Corruption));
        }

        let page_offset = chunk.position.offset % geometry.page_size;
        if page_offset == 0 {
            if expecting_file_header {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            if chunk.bytes.len() != PAGE_HEADER_ENCODED_LEN {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            let header = PageHeader::decode(&chunk.bytes)?;
            let expected_page_no = u32::try_from(
                chunk
                    .position
                    .offset
                    .checked_div(geometry.page_size)
                    .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
            )
            .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
            if header.file_id != chunk.position.file_id || header.page_no != expected_page_no {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            expecting_file_header = header.page_no == 0;
        } else if chunk.position.offset < geometry.page_size
            && chunk.position.offset == PAGE_HEADER_ENCODED_LEN as u64
        {
            if !expecting_file_header {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            if chunk.bytes.len() != FILE_HEADER_ENCODED_LEN {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            let header = VLogFileHeader::decode(&chunk.bytes)?;
            if header.database_uuid != database_uuid || header.file_id != chunk.position.file_id {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            expecting_file_header = false;
        } else {
            if expecting_file_header {
                return Err(decode_error(StorageErrorKind::Corruption));
            }
            let decoded = decode_record_at(&chunk.bytes, chunk.position, geometry)?;
            let header = RecordHeader::decode(&chunk.bytes)?;
            if let Some((commit_seq, tx_uuid)) = identity {
                if header.commit_seq != commit_seq || header.tx_uuid != tx_uuid {
                    return Err(decode_error(StorageErrorKind::Corruption));
                }
            } else {
                identity = Some((header.commit_seq, header.tx_uuid));
            }

            match decoded {
                DecodedRecord::PageEnd => {
                    if footer.is_some() || previous_transaction_record_was_page_end {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    previous_transaction_record_was_page_end = true;
                }
                DecodedRecord::TxBegin(found) => {
                    transaction_record = true;
                    if begin.is_some() || footer.is_some() || logical_op_count != 0 {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    if found.vlog_begin != vlog_begin {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    envelope_crc.update_tx_begin(found)?;
                    begin = Some(found);
                    previous_transaction_record_was_page_end = false;
                }
                DecodedRecord::KvRecord(found) => {
                    transaction_record = true;
                    if begin.is_none() || footer.is_some() || found.op_index != logical_op_count {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    envelope_crc.update_kv_record(found)?;
                    distinct_keys.insert(found.key);
                    logical_op_count = logical_op_count
                        .checked_add(1)
                        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
                    kv_record_count = kv_record_count
                        .checked_add(1)
                        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
                    previous_transaction_record_was_page_end = false;
                }
                DecodedRecord::DeleteRecord(found) => {
                    transaction_record = true;
                    if begin.is_none() || footer.is_some() || found.op_index != logical_op_count {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    envelope_crc.update_delete_record(found)?;
                    distinct_keys.insert(found.key);
                    logical_op_count = logical_op_count
                        .checked_add(1)
                        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
                    delete_record_count = delete_record_count
                        .checked_add(1)
                        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
                    previous_transaction_record_was_page_end = false;
                }
                DecodedRecord::TxPreparedEnd(found) => {
                    transaction_record = true;
                    let declared_begin =
                        begin.ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
                    let distinct_key_count = u64::try_from(distinct_keys.len())
                        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
                    if footer.is_some()
                        || found.commit_seq != declared_begin.commit_seq
                        || found.tx_uuid != declared_begin.tx_uuid
                        || found.vlog_begin != declared_begin.vlog_begin
                        || found.vlog_end != vlog_end
                        || found.logical_op_count != declared_begin.logical_op_count
                        || found.distinct_key_count != declared_begin.distinct_key_count
                        || found.logical_op_count != logical_op_count
                        || found.distinct_key_count != distinct_key_count
                        || found.kv_record_count != kv_record_count
                        || found.delete_record_count != delete_record_count
                        || chunk_index + 1 != chunks.len()
                    {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    let calculated_crc = envelope_crc.finish_with_footer(found)?;
                    if calculated_crc != found.envelope_crc32c
                        || expected_envelope_crc32c
                            .is_some_and(|expected| expected != calculated_crc)
                    {
                        return Err(decode_error(StorageErrorKind::Corruption));
                    }
                    footer = Some(found);
                    previous_transaction_record_was_page_end = false;
                }
            }
        }

        physical_end = chunk_end(chunk, geometry)?;
        if transaction_record {
            validate_transaction_record_end(physical_end, geometry)?;
        }
    }

    if physical_end != vlog_end {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let begin = begin.ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let footer = footer.ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    Ok(ScannedEnvelope {
        commit_seq: begin.commit_seq,
        tx_uuid: begin.tx_uuid,
        vlog_begin,
        vlog_end,
        logical_op_count,
        distinct_key_count: footer.distinct_key_count,
        kv_record_count,
        delete_record_count,
        envelope_crc32c: footer.envelope_crc32c,
    })
}

fn validate_transaction_record_end(record_end: VLogPosition, geometry: VLogGeometry) -> Result<()> {
    if record_end.file_id > geometry.max_file_id || record_end.offset > geometry.max_file_size {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    if record_end.offset == geometry.max_file_size
        || record_end.offset.is_multiple_of(geometry.page_size)
    {
        return Ok(());
    }
    let remaining = remaining_in_page(record_end.offset, geometry)
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    if remaining < u64::from(PAGE_END_MIN_SIZE) {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LocatedFooter {
    pub(crate) record_start: VLogPosition,
    pub(crate) footer: TxPreparedEnd,
}

pub(crate) fn locate_footer_from_end(
    tail_start: VLogPosition,
    tail: &[u8],
    vlog_end: VLogPosition,
    geometry: VLogGeometry,
) -> Result<LocatedFooter> {
    geometry.validate_for_decode()?;
    if tail_start.file_id != vlog_end.file_id || tail.len() < 8 {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let tail_len =
        u64::try_from(tail.len()).map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    let calculated_tail_end = tail_start
        .offset
        .checked_add(tail_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if calculated_tail_end != vlog_end.offset || vlog_end.offset > geometry.max_file_size {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let trailer_offset = tail
        .len()
        .checked_sub(8)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if tail.get(trailer_offset + 4..trailer_offset + 8) != Some(END_TRAILER_MAGIC) {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let end_record_len = read_u32(tail, trailer_offset)?;
    let record_start_offset = vlog_end
        .offset
        .checked_sub(u64::from(end_record_len))
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if record_start_offset < tail_start.offset
        || record_start_offset / geometry.page_size
            != vlog_end
                .offset
                .checked_sub(1)
                .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
                / geometry.page_size
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let relative_start = usize::try_from(
        record_start_offset
            .checked_sub(tail_start.offset)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
    )
    .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    let record = tail
        .get(relative_start..)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let record_start = VLogPosition {
        file_id: vlog_end.file_id,
        offset: record_start_offset,
    };
    match decode_record_at(record, record_start, geometry)? {
        DecodedRecord::TxPreparedEnd(footer) if footer.vlog_end == vlog_end => Ok(LocatedFooter {
            record_start,
            footer,
        }),
        _ => Err(decode_error(StorageErrorKind::Corruption)),
    }
}

fn validate_record_start(position: VLogPosition, geometry: VLogGeometry) -> Result<()> {
    if position.file_id > geometry.max_file_id || position.offset >= geometry.max_file_size {
        return Err(decode_error(StorageErrorKind::InvalidLayout));
    }
    let page_offset = position.offset % geometry.page_size;
    let record_area_offset = if position.offset < geometry.page_size {
        FIRST_PAGE_RECORD_AREA_START
    } else {
        OTHER_PAGE_RECORD_AREA_OFFSET
    };
    if page_offset < record_area_offset {
        return Err(decode_error(StorageErrorKind::InvalidLayout));
    }
    Ok(())
}

fn validate_record_bounds(
    encoded: &[u8],
    record_start: VLogPosition,
    geometry: VLogGeometry,
) -> Result<()> {
    validate_record_start(record_start, geometry)
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    let encoded_len =
        u64::try_from(encoded.len()).map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    let record_end = record_start
        .offset
        .checked_add(encoded_len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if record_end > geometry.max_file_size || record_end > page_end(record_start.offset, geometry)?
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(())
}

fn remaining_in_page(offset: u64, geometry: VLogGeometry) -> Result<u64> {
    page_end(offset, geometry)?
        .checked_sub(offset)
        .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))
}

fn page_end(offset: u64, geometry: VLogGeometry) -> Result<u64> {
    if offset >= geometry.max_file_size {
        return Err(decode_error(StorageErrorKind::InvalidLayout));
    }
    let page_no = offset
        .checked_div(geometry.page_size)
        .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))?;
    page_no
        .checked_add(1)
        .and_then(|value| value.checked_mul(geometry.page_size))
        .filter(|end| *end <= geometry.max_file_size)
        .ok_or_else(|| decode_error(StorageErrorKind::InvalidLayout))
}

fn chunk_end(chunk: &PhysicalChunk, geometry: VLogGeometry) -> Result<VLogPosition> {
    if chunk.position.file_id > geometry.max_file_id {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    let len =
        u64::try_from(chunk.bytes.len()).map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    let offset = chunk
        .position
        .offset
        .checked_add(len)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    if offset > geometry.max_file_size {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(VLogPosition {
        file_id: chunk.position.file_id,
        offset,
    })
}

fn next_chunk_position(previous_end: VLogPosition, geometry: VLogGeometry) -> Result<VLogPosition> {
    if previous_end.offset < geometry.max_file_size {
        return Ok(previous_end);
    }
    if previous_end.offset != geometry.max_file_size || previous_end.file_id >= geometry.max_file_id
    {
        return Err(decode_error(StorageErrorKind::Corruption));
    }
    Ok(VLogPosition {
        file_id: previous_end
            .file_id
            .checked_add(1)
            .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?,
        offset: 0,
    })
}

fn read_array_16(encoded: &[u8], offset: usize) -> Result<[u8; 16]> {
    let end = offset
        .checked_add(16)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    encoded
        .get(offset..end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
        .try_into()
        .map_err(|_| decode_error(StorageErrorKind::Corruption))
}

fn read_u16(encoded: &[u8], offset: usize) -> Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let bytes: [u8; 2] = encoded
        .get(offset..end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
        .try_into()
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(encoded: &[u8], offset: usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let bytes: [u8; 4] = encoded
        .get(offset..end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
        .try_into()
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(encoded: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?;
    let bytes: [u8; 8] = encoded
        .get(offset..end)
        .ok_or_else(|| decode_error(StorageErrorKind::Corruption))?
        .try_into()
        .map_err(|_| decode_error(StorageErrorKind::Corruption))?;
    Ok(u64::from_le_bytes(bytes))
}

fn try_copy_structural_bytes<const N: usize>(bytes: &[u8; N]) -> Result<Vec<u8>> {
    inject_prepare_allocation_failure(PrepareAllocationFailureSite::StructuralBytes)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(N)
        .map_err(|_| invalid_vlog_error(StorageErrorKind::ResourceExhausted))?;
    owned.extend_from_slice(bytes);
    Ok(owned)
}

#[cfg(test)]
mod prepare_allocation_failure {
    use std::cell::Cell;

    use super::PrepareAllocationFailureSite;

    thread_local! {
        static NEXT_FAILURE: Cell<Option<PrepareAllocationFailureSite>> = const { Cell::new(None) };
    }

    pub(super) fn inject(site: PrepareAllocationFailureSite) {
        NEXT_FAILURE.with(|next| assert!(next.replace(Some(site)).is_none()));
    }

    pub(super) fn should_fail(site: PrepareAllocationFailureSite) -> bool {
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
pub(crate) fn inject_prepare_allocation_failure_for_test(site: PrepareAllocationFailureSite) {
    prepare_allocation_failure::inject(site);
}

fn inject_prepare_allocation_failure(site: PrepareAllocationFailureSite) -> Result<()> {
    #[cfg(test)]
    if prepare_allocation_failure::should_fail(site) {
        return Err(invalid_vlog_error(StorageErrorKind::ResourceExhausted));
    }
    let _ = site;
    Ok(())
}

fn invalid_vlog_error(kind: StorageErrorKind) -> StorageError {
    let retry_advice = if kind == StorageErrorKind::ResourceExhausted {
        RetryAdvice::RetrySameInstance
    } else if kind == StorageErrorKind::IncompatibleFormat {
        RetryAdvice::DoNotRetry
    } else {
        RetryAdvice::FixRequestAndRetrySameInstance
    };
    StorageError::codec_error(
        kind,
        Operation::WriteBatch,
        ProtocolStage::VLogAppend,
        Some(WriteOutcome::NotCommitted),
        retry_advice,
    )
}

fn capacity_error() -> StorageError {
    StorageError::codec_error(
        StorageErrorKind::CapacityExceeded,
        Operation::WriteBatch,
        ProtocolStage::VLogAppend,
        Some(WriteOutcome::NotCommitted),
        RetryAdvice::DoNotRetry,
    )
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
