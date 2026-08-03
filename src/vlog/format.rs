#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "format primitives are consumed by later Value Log stages"
    )
)]

use crate::{Error, Result};

pub(crate) const PAGE_SIZE: u64 = 64 * 1024; // Vlog Data File 的页大小
pub(crate) const MAX_FILE_SIZE: u64 = 1_u64 << 32; // Vlog Data File 文件大小
pub(crate) const MAX_KEY_VALUE_SIZE: u64 = 60_000; // key/value 最大大小限制
pub(crate) const MAX_FILE_ID: u32 = 999_999;
pub(crate) const MAX_PAGE_NO: u32 = 65_535; // Vlog Data File 包括多少个页
pub(crate) const PAGE_HEADER_SIZE: usize = 16; // Page_Header 大小
pub(crate) const RECORD_HEADER_SIZE: usize = 12; // Record记录头大小
pub(crate) const VALUE_POINTER_SIZE: usize = 12; // ValuePointer指针大小

const PAGE_MAGIC: [u8; 4] = *b"RKVP";
const RECORD_MAGIC: [u8; 4] = *b"RKVR";

/*
   PageHeader
       page_magic: 4B; // [u8; 4], "RKVP"常量, 交由encode/decode实现序列化写入/读取
       file_id: 4B;
       page_no: 4B;
       header_crc32c: 4B;
*/
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageHeader {
    file_id: u32,       // page 所在文件
    page_no: u32,       // page 号
    header_crc32c: u32, // 前12字节CRC32校验码
}

impl PageHeader {
    pub(crate) fn new(file_id: u32, page_no: u32) -> Result<Self> {
        validate_file_id_for_write(file_id)?;
        validate_page_no_for_write(page_no)?;

        // 生成前12字节的CRC32校验码
        let mut prefix = [0_u8; 12];
        prefix[0..4].copy_from_slice(&PAGE_MAGIC);
        prefix[4..8].copy_from_slice(&file_id.to_le_bytes());
        prefix[8..12].copy_from_slice(&page_no.to_le_bytes());

        Ok(Self {
            file_id,
            page_no,
            header_crc32c: crc32c::crc32c(&prefix),
        })
    }

    pub(crate) fn encode(self) -> [u8; PAGE_HEADER_SIZE] {
        let mut encoded = [0_u8; PAGE_HEADER_SIZE];
        encoded[0..4].copy_from_slice(&PAGE_MAGIC);
        encoded[4..8].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[8..12].copy_from_slice(&self.page_no.to_le_bytes());
        encoded[12..16].copy_from_slice(&self.header_crc32c.to_le_bytes());
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_size(encoded, PAGE_HEADER_SIZE, "PageHeader")?;
        if encoded[0..4] != PAGE_MAGIC {
            return Err(Error::Corruption("invalid PageHeader magic".into()));
        }

        let file_id = read_u32(encoded, 4);
        let page_no = read_u32(encoded, 8);
        if file_id > MAX_FILE_ID {
            return Err(Error::Corruption(format!(
                "PageHeader file_id {file_id} exceeds {MAX_FILE_ID}"
            )));
        }
        if page_no > MAX_PAGE_NO {
            return Err(Error::Corruption(format!(
                "PageHeader page_no {page_no} exceeds {MAX_PAGE_NO}"
            )));
        }

        let expected_crc = crc32c::crc32c(&encoded[..12]);
        let actual_crc = read_u32(encoded, 12);
        if actual_crc != expected_crc {
            return Err(Error::Corruption(format!(
                "PageHeader CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            )));
        }

        Ok(Self {
            file_id,
            page_no,
            header_crc32c: actual_crc,
        })
    }

    pub(crate) fn file_id(self) -> u32 {
        self.file_id
    }

    pub(crate) fn page_no(self) -> u32 {
        self.page_no
    }

    pub(crate) fn header_crc32c(self) -> u32 {
        self.header_crc32c
    }
}

/*
   RecordHeader
       record_magic 4B; // [u8; 4] "RKVR" 常量
       key_len 2B;
       value_len 2B;
       payload_crc32c 4B;
*/
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordHeader {
    key_len: u16,   // 2B 0-65535
    value_len: u16, // 2B 0-65535
    payload_crc32c: u32,
}

impl RecordHeader {
    pub(crate) fn new(key: &[u8], value: &[u8]) -> Result<Self> {
        validate_key_value_for_write(key, value)?;
        let key_len = u16::try_from(key.len())
            .map_err(|_| Error::InvalidArgument("key length does not fit u16".into()))?;
        let value_len = u16::try_from(value.len())
            .map_err(|_| Error::InvalidArgument("value length does not fit u16".into()))?;

        Ok(Self {
            key_len,
            value_len,
            payload_crc32c: payload_crc32c(key, value),
        })
    }

    pub(crate) fn encode(self) -> [u8; RECORD_HEADER_SIZE] {
        let mut encoded = [0_u8; RECORD_HEADER_SIZE];
        encoded[0..4].copy_from_slice(&RECORD_MAGIC);
        encoded[4..6].copy_from_slice(&self.key_len.to_le_bytes());
        encoded[6..8].copy_from_slice(&self.value_len.to_le_bytes());
        encoded[8..12].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        encoded
    }

    // 反序列化
    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_size(encoded, RECORD_HEADER_SIZE, "RecordHeader")?;
        if encoded[0..4] != RECORD_MAGIC {
            return Err(Error::Corruption("invalid RecordHeader magic".into()));
        }

        let key_len = read_u16(encoded, 4);
        let value_len = read_u16(encoded, 6);
        validate_decoded_lengths(key_len, value_len, "RecordHeader")?;

        Ok(Self {
            key_len,
            value_len,
            payload_crc32c: read_u32(encoded, 8),
        })
    }

    // 由实际k v校验record_header.header_crc32字段是否正确
    pub(crate) fn validate_payload(self, key: &[u8], value: &[u8]) -> Result<()> {
        if u64::try_from(key.len()).ok() != Some(u64::from(self.key_len))
            || u64::try_from(value.len()).ok() != Some(u64::from(self.value_len))
        {
            return Err(Error::Corruption(
                "Record payload lengths do not match RecordHeader".into(),
            ));
        }

        let actual_crc = payload_crc32c(key, value);
        if actual_crc != self.payload_crc32c {
            return Err(Error::Corruption(format!(
                "Record payload CRC32C mismatch: expected {:#010x}, got {actual_crc:#010x}",
                self.payload_crc32c
            )));
        }

        Ok(())
    }

    pub(crate) fn key_len(self) -> u16 {
        self.key_len
    }

    pub(crate) fn value_len(self) -> u16 {
        self.value_len
    }

    pub(crate) fn payload_crc32c(self) -> u32 {
        self.payload_crc32c
    }

    pub(crate) fn record_len(self) -> u64 {
        u64::try_from(RECORD_HEADER_SIZE).expect("header size fits u64")
            + u64::from(self.key_len)
            + u64::from(self.value_len)
    }
}

/*
   ValuePointer
       file_id: 4B;
       record_offset: 4B; // file_id+record_offset 寻址
       key_len: 2B;
       value_len: 2B;
*/
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValuePointer {
    file_id: u32,
    record_offset: u32, // offset定位record_header, page_no其实能够直接得到
    key_len: u16,
    value_len: u16,
}

impl ValuePointer {
    pub(crate) fn new(
        file_id: u32,
        record_offset: u32,
        key_len: u16,
        value_len: u16,
    ) -> Result<Self> {
        validate_file_id_for_write(file_id)?;
        validate_pointer_fields(record_offset, key_len, value_len)
            .map_err(pointer_validation_as_invalid_argument)?;

        Ok(Self {
            file_id,
            record_offset,
            key_len,
            value_len,
        })
    }

    pub(crate) fn encode(self) -> [u8; VALUE_POINTER_SIZE] {
        let mut encoded = [0_u8; VALUE_POINTER_SIZE];
        encoded[0..4].copy_from_slice(&self.file_id.to_le_bytes());
        encoded[4..8].copy_from_slice(&self.record_offset.to_le_bytes());
        encoded[8..10].copy_from_slice(&self.key_len.to_le_bytes());
        encoded[10..12].copy_from_slice(&self.value_len.to_le_bytes());
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self> {
        require_exact_size(encoded, VALUE_POINTER_SIZE, "ValuePointer")?;
        let file_id = read_u32(encoded, 0);
        let record_offset = read_u32(encoded, 4);
        let key_len = read_u16(encoded, 8);
        let value_len = read_u16(encoded, 10);

        if file_id > MAX_FILE_ID {
            return Err(Error::Corruption(format!(
                "ValuePointer file_id {file_id} exceeds {MAX_FILE_ID}"
            )));
        }
        validate_pointer_fields(record_offset, key_len, value_len)?;

        Ok(Self {
            file_id,
            record_offset,
            key_len,
            value_len,
        })
    }

    pub(crate) fn file_id(self) -> u32 {
        self.file_id
    }

    pub(crate) fn record_offset(self) -> u32 {
        self.record_offset
    }

    pub(crate) fn key_len(self) -> u16 {
        self.key_len
    }

    pub(crate) fn value_len(self) -> u16 {
        self.value_len
    }

    pub(crate) fn record_len(self) -> u64 {
        u64::try_from(RECORD_HEADER_SIZE).expect("header size fits u64")
            + u64::from(self.key_len)
            + u64::from(self.value_len)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]

/*
   下次Record写入位置描述
       rollover: 切换vlog文件;
       padding_len: 填充0长度;
       page_header_offset: None复用写入该page,否则是新page_offset;
       record_offset: record写入的文件内偏移量;
       end_offset: record写入结束的文件偏移量;
*/
pub(crate) struct RecordPlacement {
    pub(crate) rollover: bool,
    pub(crate) padding_len: u64,
    pub(crate) page_header_offset: Option<u64>,
    pub(crate) record_offset: u64,
    pub(crate) end_offset: u64,
}

// 页号 -> 页偏移量
pub(crate) fn page_start(page_no: u32) -> Result<u64> {
    validate_page_no_for_write(page_no)?;
    u64::from(page_no)
        .checked_mul(PAGE_SIZE)
        .ok_or_else(|| Error::InvalidArgument("page start offset overflow".into()))
}

// 文件内offset -> 页内offset
pub(crate) fn page_offset(file_offset: u64) -> Result<u64> {
    if file_offset >= MAX_FILE_SIZE {
        return Err(Error::InvalidArgument(format!(
            "file offset {file_offset} is outside a Value Log file"
        )));
    }
    Ok(file_offset % PAGE_SIZE)
}

//
pub(crate) fn remaining_in_page(file_offset: u64) -> Result<u64> {
    Ok(PAGE_SIZE - page_offset(file_offset)?)
}

pub(crate) fn zero_padding_to_next_page(file_offset: u64) -> Result<u64> {
    let offset = page_offset(file_offset)?;
    Ok(if offset == 0 { 0 } else { PAGE_SIZE - offset })
}

pub(crate) fn plan_record(current_file_len: u64, record_len: u64) -> Result<RecordPlacement> {
    plan_record_with_limit(current_file_len, record_len, MAX_FILE_SIZE)
}

fn plan_record_with_limit(
    current_file_len: u64,
    record_len: u64,
    file_limit: u64,
) -> Result<RecordPlacement> {
    validate_file_limit(file_limit)?;
    validate_physical_record_len(record_len)?;
    if current_file_len > file_limit {
        return Err(Error::InvalidArgument(format!(
            "current file length {current_file_len} exceeds file limit {file_limit}"
        )));
    }

    let (padding_len, page_header_offset, record_offset) =
        candidate_in_current_file(current_file_len, record_len)?;
    let end_offset = record_offset
        .checked_add(record_len)
        .ok_or_else(|| Error::InvalidArgument("record end offset overflow".into()))?;

    // 不切换vlog文件
    if end_offset <= file_limit {
        return Ok(RecordPlacement {
            rollover: false,
            padding_len,
            page_header_offset,
            record_offset,
            end_offset,
        });
    }

    // 需要切换vlog文件
    let new_record_offset = u64::try_from(PAGE_HEADER_SIZE).expect("header size fits u64");
    let new_end_offset = new_record_offset
        .checked_add(record_len)
        .ok_or_else(|| Error::InvalidArgument("new file record end offset overflow".into()))?;
    if new_end_offset > file_limit {
        return Err(Error::InvalidArgument(format!(
            "record length {record_len} cannot fit in an empty file with limit {file_limit}"
        )));
    }

    Ok(RecordPlacement {
        rollover: true,
        padding_len,
        page_header_offset: Some(0),
        record_offset: new_record_offset,
        end_offset: new_end_offset,
    })
}

/*
   计算下次写入位置
   return (填充0长度, 需要填写page_header偏移量, kv记录写入offset)
*/
fn candidate_in_current_file(
    current_file_len: u64,
    record_len: u64,
) -> Result<(u64, Option<u64>, u64)> {
    let offset = current_file_len % PAGE_SIZE;
    let header_size = u64::try_from(PAGE_HEADER_SIZE).expect("header size fits u64");

    if offset == 0 {
        // 正好是页开头第一个
        let record_offset = current_file_len
            .checked_add(header_size)
            .ok_or_else(|| Error::InvalidArgument("record offset overflow".into()))?;
        return Ok((0, Some(current_file_len), record_offset));
    }

    if offset < header_size {
        return Err(Error::InvalidArgument(format!(
            "current file length {current_file_len} ends inside a PageHeader"
        )));
    }

    let remaining = PAGE_SIZE - offset;
    if record_len <= remaining {
        // 当前page能够容纳(复用页)
        return Ok((0, None, current_file_len));
    }

    // 当前页容量不够, 需要新页
    let next_page = current_file_len
        .checked_add(remaining)
        .ok_or_else(|| Error::InvalidArgument("next page offset overflow".into()))?;
    let record_offset = next_page
        .checked_add(header_size)
        .ok_or_else(|| Error::InvalidArgument("record offset overflow".into()))?;
    Ok((remaining, Some(next_page), record_offset))
}

/* ------------------- 辅助函数 ------------------- */
fn validate_key_value_for_write(key: &[u8], value: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument(
            "key must contain at least one byte".into(),
        ));
    }
    let payload_len = key
        .len()
        .checked_add(value.len())
        .ok_or_else(|| Error::InvalidArgument("key and value length overflow".into()))?;
    if payload_len > usize::try_from(MAX_KEY_VALUE_SIZE).expect("maximum size fits usize") {
        return Err(Error::InvalidArgument(format!(
            "key and value length {payload_len} exceeds {MAX_KEY_VALUE_SIZE}"
        )));
    }
    Ok(())
}

fn validate_decoded_lengths(key_len: u16, value_len: u16, structure: &str) -> Result<()> {
    if key_len == 0 {
        return Err(Error::Corruption(format!(
            "{structure} contains an empty key"
        )));
    }
    let payload_len = u64::from(key_len)
        .checked_add(u64::from(value_len))
        .ok_or_else(|| Error::Corruption(format!("{structure} payload length overflow")))?;
    if payload_len > MAX_KEY_VALUE_SIZE {
        return Err(Error::Corruption(format!(
            "{structure} payload length {payload_len} exceeds {MAX_KEY_VALUE_SIZE}"
        )));
    }
    Ok(())
}

fn validate_pointer_fields(record_offset: u32, key_len: u16, value_len: u16) -> Result<()> {
    // key + value <= 60000
    validate_decoded_lengths(key_len, value_len, "ValuePointer")?;
    let record_len = u64::try_from(RECORD_HEADER_SIZE).expect("header size fits u64")
        + u64::from(key_len)
        + u64::from(value_len);
    let start = u64::from(record_offset);
    let offset_in_page = start % PAGE_SIZE;
    let header_size = u64::try_from(PAGE_HEADER_SIZE).expect("header size fits u64");
    if offset_in_page < header_size {
        // offset 不在page_header中
        return Err(Error::Corruption(format!(
            "ValuePointer record_offset {record_offset} points into a PageHeader"
        )));
    }

    // 验证kv_record在page内部
    let end = start
        .checked_add(record_len)
        .ok_or_else(|| Error::Corruption("ValuePointer record range overflow".into()))?;
    let page_end = start
        .checked_sub(offset_in_page)
        .and_then(|page_start| page_start.checked_add(PAGE_SIZE))
        .ok_or_else(|| Error::Corruption("ValuePointer page range overflow".into()))?;
    if end > page_end {
        return Err(Error::Corruption(
            "ValuePointer record crosses a logical page".into(),
        ));
    }
    if end > MAX_FILE_SIZE {
        return Err(Error::Corruption(
            "ValuePointer record exceeds the 4GiB file limit".into(),
        ));
    }
    Ok(())
}

fn pointer_validation_as_invalid_argument(error: Error) -> Error {
    match error {
        Error::Corruption(message) => Error::InvalidArgument(message),
        other => other,
    }
}

fn validate_file_id_for_write(file_id: u32) -> Result<()> {
    if file_id > MAX_FILE_ID {
        return Err(Error::CapacityExceeded(format!(
            "file_id {file_id} exceeds {MAX_FILE_ID}"
        )));
    }
    Ok(())
}

fn validate_page_no_for_write(page_no: u32) -> Result<()> {
    if page_no > MAX_PAGE_NO {
        return Err(Error::InvalidArgument(format!(
            "page_no {page_no} exceeds {MAX_PAGE_NO}"
        )));
    }
    Ok(())
}

fn validate_physical_record_len(record_len: u64) -> Result<()> {
    let min = u64::try_from(RECORD_HEADER_SIZE).expect("header size fits u64") + 1;
    let max = u64::try_from(RECORD_HEADER_SIZE).expect("header size fits u64") + MAX_KEY_VALUE_SIZE;
    if !(min..=max).contains(&record_len) {
        return Err(Error::InvalidArgument(format!(
            "record length {record_len} must be between {min} and {max}"
        )));
    }
    Ok(())
}

fn validate_file_limit(file_limit: u64) -> Result<()> {
    if file_limit == 0 || file_limit > MAX_FILE_SIZE || !file_limit.is_multiple_of(PAGE_SIZE) {
        return Err(Error::InvalidArgument(format!(
            "file limit {file_limit} must be a non-zero multiple of {PAGE_SIZE} not exceeding {MAX_FILE_SIZE}"
        )));
    }
    Ok(())
}

fn require_exact_size(encoded: &[u8], expected: usize, structure: &str) -> Result<()> {
    if encoded.len() != expected {
        return Err(Error::Corruption(format!(
            "{structure} must contain exactly {expected} bytes, got {}",
            encoded.len()
        )));
    }
    Ok(())
}

fn read_u32(encoded: &[u8], offset: usize) -> u32 {
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(&encoded[offset..offset + 4]);
    u32::from_le_bytes(bytes)
}

fn read_u16(encoded: &[u8], offset: usize) -> u16 {
    let mut bytes = [0_u8; 2];
    bytes.copy_from_slice(&encoded[offset..offset + 2]);
    u16::from_le_bytes(bytes)
}

fn payload_crc32c(key: &[u8], value: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(key), value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_corruption<T>(result: Result<T>) {
        assert!(matches!(result, Err(Error::Corruption(_))));
    }

    fn assert_invalid_argument<T>(result: Result<T>) {
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn crc32c_matches_the_castagnoli_check_value_and_incremental_payload() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(
            payload_crc32c(b"1234", b"56789"),
            crc32c::crc32c(b"123456789")
        );
    }

    // crc 序列化 是否正确
    #[test]
    fn page_header_golden_encoding_and_round_trip() -> Result<()> {
        let header = PageHeader::new(1, 2)?;
        let encoded = header.encode();
        let expected = [
            b'R', b'K', b'V', b'P', 1, 0, 0, 0, 2, 0, 0, 0, 0x0f, 0x4e, 0xe6, 0x05,
        ];

        assert_eq!(encoded, expected);
        assert_eq!(header.header_crc32c(), 0x05e6_4e0f);
        assert_eq!(PageHeader::decode(&encoded)?, header);
        assert_eq!(header.file_id(), 1);
        assert_eq!(header.page_no(), 2);
        Ok(())
    }

    #[test]
    fn page_header_rejects_bad_size_magic_crc_and_boundaries() -> Result<()> {
        let encoded = PageHeader::new(0, 0)?.encode();
        assert_corruption(PageHeader::decode(&encoded[..15]));

        let mut bad_magic = encoded;
        bad_magic[0] ^= 1;
        assert_corruption(PageHeader::decode(&bad_magic));

        let mut bad_crc = encoded;
        bad_crc[12] ^= 1;
        assert_corruption(PageHeader::decode(&bad_crc));

        let mut bad_file = encoded;
        bad_file[4..8].copy_from_slice(&(MAX_FILE_ID + 1).to_le_bytes());
        assert_corruption(PageHeader::decode(&bad_file));

        let mut bad_page = encoded;
        bad_page[8..12].copy_from_slice(&(MAX_PAGE_NO + 1).to_le_bytes());
        assert_corruption(PageHeader::decode(&bad_page));

        assert!(matches!(
            PageHeader::new(MAX_FILE_ID + 1, 0),
            Err(Error::CapacityExceeded(_))
        ));
        assert_invalid_argument(PageHeader::new(0, MAX_PAGE_NO + 1));
        Ok(())
    }

    #[test]
    fn record_header_golden_encoding_round_trip_and_empty_value() -> Result<()> {
        let header = RecordHeader::new(b"key", b"")?;
        let encoded = header.encode();
        let expected = [b'R', b'K', b'V', b'R', 3, 0, 0, 0, 0x6d, 0x75, 0xa4, 0x40];

        assert_eq!(encoded, expected);
        assert_eq!(RecordHeader::decode(&encoded)?, header);
        assert_eq!(header.key_len(), 3);
        assert_eq!(header.value_len(), 0);
        assert_eq!(header.payload_crc32c(), 0x40a4_756d);
        assert_eq!(header.record_len(), 15);
        header.validate_payload(b"key", b"")?;
        Ok(())
    }

    #[test]
    fn record_header_rejects_invalid_input_and_corruption() -> Result<()> {
        assert_invalid_argument(RecordHeader::new(&vec![b'k'; 60_000], b"v"));

        let encoded = RecordHeader::new(b"k", b"value")?.encode();
        assert_corruption(RecordHeader::decode(&encoded[..11]));

        let mut bad_magic = encoded;
        bad_magic[0] ^= 1;
        assert_corruption(RecordHeader::decode(&bad_magic));

        let mut oversized = encoded;
        oversized[4..6].copy_from_slice(&60_000_u16.to_le_bytes());
        oversized[6..8].copy_from_slice(&1_u16.to_le_bytes());
        assert_corruption(RecordHeader::decode(&oversized));

        let header = RecordHeader::decode(&encoded)?;
        assert_corruption(header.validate_payload(b"k", b"changed"));
        assert_corruption(header.validate_payload(b"other", b"value"));
        Ok(())
    }

    #[test]
    fn key_value_size_boundaries_are_enforced() -> Result<()> {
        for payload_len in [59_999, 60_000] {
            let value = vec![b'v'; payload_len - 1];
            let header = RecordHeader::new(b"k", &value)?;
            assert_eq!(
                header.record_len(),
                u64::try_from(RECORD_HEADER_SIZE + payload_len).expect("length fits")
            );
        }

        let value = vec![b'v'; 60_000];
        assert_invalid_argument(RecordHeader::new(b"k", &value)); // k + v = 600001
        Ok(())
    }

    #[test]
    fn value_pointer_golden_encoding_and_round_trip() -> Result<()> {
        let pointer = ValuePointer::new(7, 16, 3, 5)?;
        let encoded = pointer.encode();
        let expected = [7, 0, 0, 0, 16, 0, 0, 0, 3, 0, 5, 0];

        assert_eq!(encoded, expected);
        assert_eq!(ValuePointer::decode(&encoded)?, pointer);
        assert_eq!(pointer.file_id(), 7);
        assert_eq!(pointer.record_offset(), 16);
        assert_eq!(pointer.key_len(), 3);
        assert_eq!(pointer.value_len(), 5);
        assert_eq!(pointer.record_len(), 20);
        Ok(())
    }

    #[test]
    fn value_pointer_rejects_bad_size_lengths_offsets_and_file_ids() {
        assert_corruption(ValuePointer::decode(&[0; VALUE_POINTER_SIZE - 1]));

        let valid = ValuePointer::new(0, 16, 1, 0).expect("valid pointer");
        let mut bad_file = valid.encode();
        bad_file[0..4].copy_from_slice(&(MAX_FILE_ID + 1).to_le_bytes());
        assert_corruption(ValuePointer::decode(&bad_file));

        let mut oversized = valid.encode();
        oversized[8..10].copy_from_slice(&60_000_u16.to_le_bytes());
        oversized[10..12].copy_from_slice(&1_u16.to_le_bytes());
        assert_corruption(ValuePointer::decode(&oversized));

        let mut page_header_offset = valid.encode();
        page_header_offset[4..8].copy_from_slice(&0_u32.to_le_bytes());
        assert_corruption(ValuePointer::decode(&page_header_offset));

        assert_invalid_argument(ValuePointer::new(0, u32::MAX, 1, 0));
        assert!(matches!(
            ValuePointer::new(MAX_FILE_ID + 1, 16, 1, 0),
            Err(Error::CapacityExceeded(_))
        ));
    }

    #[test]
    fn page_math_uses_checked_4gib_boundaries() -> Result<()> {
        assert_eq!(page_start(0)?, 0);
        assert_eq!(page_start(MAX_PAGE_NO)?, MAX_FILE_SIZE - PAGE_SIZE);
        assert_invalid_argument(page_start(MAX_PAGE_NO + 1));

        assert_eq!(page_offset(0)?, 0);
        assert_eq!(page_offset(PAGE_SIZE + 17)?, 17);
        assert_eq!(remaining_in_page(PAGE_SIZE + 17)?, PAGE_SIZE - 17);
        assert_eq!(zero_padding_to_next_page(PAGE_SIZE)?, 0);
        assert_eq!(zero_padding_to_next_page(PAGE_SIZE + 17)?, PAGE_SIZE - 17);
        assert_invalid_argument(page_offset(MAX_FILE_SIZE));
        Ok(())
    }

    #[test]
    fn placement_handles_new_pages_exact_fits_and_one_byte_short() -> Result<()> {
        let first = plan_record(0, 13)?;
        assert_eq!(
            first,
            RecordPlacement {
                rollover: false,
                padding_len: 0,
                page_header_offset: Some(0),
                record_offset: 16,
                end_offset: 29,
            }
        );

        let exact = plan_record(PAGE_SIZE - 13, 13)?;
        assert_eq!(exact.padding_len, 0);
        assert_eq!(exact.page_header_offset, None);
        assert_eq!(exact.end_offset, PAGE_SIZE);

        let next = plan_record(PAGE_SIZE, 13)?;
        assert_eq!(next.page_header_offset, Some(PAGE_SIZE));
        assert_eq!(next.record_offset, PAGE_SIZE + 16);

        let one_byte_short = PAGE_SIZE - 12;
        let moved = plan_record(one_byte_short, 13)?;
        assert_eq!(moved.padding_len, 12);
        assert_eq!(moved.page_header_offset, Some(PAGE_SIZE));
        assert_eq!(moved.record_offset, PAGE_SIZE + 16);
        Ok(())
    }

    #[test]
    fn placement_moves_when_less_than_a_record_header_remains() -> Result<()> {
        for remaining in 1..u64::try_from(RECORD_HEADER_SIZE).expect("header size fits") {
            let current = PAGE_SIZE - remaining;
            let placement = plan_record(current, 13)?;
            assert_eq!(placement.padding_len, remaining);
            assert_eq!(placement.page_header_offset, Some(PAGE_SIZE));
            assert_eq!(placement.record_offset, PAGE_SIZE + 16);
        }
        Ok(())
    }

    #[test]
    fn placement_rolls_files_without_crossing_the_limit() -> Result<()> {
        let one_page_limit = PAGE_SIZE;
        let placement = plan_record_with_limit(PAGE_SIZE - 1, 13, one_page_limit)?;
        assert!(placement.rollover);
        assert_eq!(placement.padding_len, 1);
        assert_eq!(placement.page_header_offset, Some(0));
        assert_eq!(placement.record_offset, 16);
        assert_eq!(placement.end_offset, 29);

        let full_file = plan_record(MAX_FILE_SIZE, 13)?;
        assert!(full_file.rollover);
        assert_eq!(full_file.padding_len, 0);
        assert_eq!(full_file.record_offset, 16);

        assert_invalid_argument(plan_record_with_limit(0, 13, PAGE_SIZE - 1));
        assert_invalid_argument(plan_record_with_limit(PAGE_SIZE + 1, 13, PAGE_SIZE));
        assert_invalid_argument(plan_record(0, 12));
        assert_invalid_argument(plan_record(1, 13));
        assert_invalid_argument(plan_record(u64::MAX, 13));
        Ok(())
    }
}
