#![allow(dead_code)]

#[path = "../src/error.rs"]
mod error;

pub(crate) use error::{
    Operation, ProtocolStage, Result, RetryAdvice, StorageError, StorageErrorKind, WriteOutcome,
};

#[path = "../src/vlog/format.rs"]
mod vlog_format;

use crc32c::{crc32c, crc32c_append};
use vlog_format::*;

fn tx_uuid() -> [u8; 16] {
    [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ]
}

fn database_uuid() -> [u8; 16] {
    [
        0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02,
        0x01,
    ]
}

fn assert_kind<T: std::fmt::Debug>(result: Result<T>, expected: StorageErrorKind) {
    let error = result.unwrap_err();
    assert_eq!(error.kind, expected);
    assert!(error.message.is_empty());
    assert!(error.source.is_none());
}

fn write_header_crc(bytes: &mut [u8]) {
    let checksum = crc32c(&bytes[0..35]);
    bytes[35..39].copy_from_slice(&checksum.to_le_bytes());
}

fn write_standard_crc(bytes: &mut [u8]) {
    let crc_offset = bytes.len() - 4;
    let checksum = crc32c(&bytes[0..crc_offset]);
    bytes[crc_offset..].copy_from_slice(&checksum.to_le_bytes());
}

fn write_footer_crc(bytes: &mut [u8]) {
    let checksum = crc32c_append(crc32c(&bytes[0..99]), &bytes[103..111]);
    bytes[99..103].copy_from_slice(&checksum.to_le_bytes());
}

fn assert_record_fields_detect_corruption(
    encoded: &[u8],
    position: VLogPosition,
    fields: &[(&str, usize)],
) {
    for (field, offset) in fields {
        let mut corrupted = encoded.to_vec();
        corrupted[*offset] ^= 0x80;
        let result = decode_record_at(&corrupted, position, VLogGeometry::PRODUCTION);
        assert!(
            matches!(result, Err(ref error) if error.kind == StorageErrorKind::Corruption),
            "field {field} was not detected as corruption: {result:?}"
        );
    }
}

fn begin() -> TxBegin {
    TxBegin {
        commit_seq: 0x0102_0304_0506_0708,
        tx_uuid: tx_uuid(),
        vlog_begin: VLogPosition {
            file_id: 3,
            offset: 64,
        },
        logical_op_count: 3,
        distinct_key_count: 2,
    }
}

fn footer() -> TxPreparedEnd {
    TxPreparedEnd {
        commit_seq: 0x0102_0304_0506_0708,
        tx_uuid: tx_uuid(),
        vlog_begin: VLogPosition {
            file_id: 3,
            offset: 64,
        },
        vlog_end: VLogPosition {
            file_id: 3,
            offset: 1_111,
        },
        logical_op_count: 3,
        distinct_key_count: 2,
        kv_record_count: 2,
        delete_record_count: 1,
        envelope_crc32c: 0x1122_3344,
    }
}

#[test]
fn crc32c_standard_check_value_is_castagnoli() {
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(crc32c_append(crc32c(b"1234"), b"56789"), 0xe306_9283);
}

#[test]
fn page_header_v0_is_exactly_16_bytes() {
    let header = PageHeader {
        file_id: 0x0001_0203,
        page_no: 0x0000_0405,
    };
    let encoded = header.encode().unwrap();
    let mut expected = [0_u8; 16];
    expected[0..4].copy_from_slice(b"RKVP");
    expected[4..8].copy_from_slice(&0x0001_0203_u32.to_le_bytes());
    expected[8..12].copy_from_slice(&0x0000_0405_u32.to_le_bytes());
    let checksum = crc32c(&expected[0..12]);
    expected[12..16].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(encoded, expected);
    assert_eq!(PageHeader::decode(&encoded).unwrap(), header);

    let mut corrupted = encoded;
    corrupted[8] ^= 0x80;
    assert_kind(PageHeader::decode(&corrupted), StorageErrorKind::Corruption);
}

#[test]
fn file_header_v0_is_exactly_48_bytes() {
    let header = VLogFileHeader::new(database_uuid(), 7);
    let encoded = header.encode().unwrap();
    let mut expected = [0_u8; 48];
    expected[0..8].copy_from_slice(b"RKVLOG00");
    expected[8..12].copy_from_slice(&0_u32.to_le_bytes());
    expected[12..28].copy_from_slice(&database_uuid());
    expected[28..32].copy_from_slice(&7_u32.to_le_bytes());
    expected[32..36].copy_from_slice(&65_536_u32.to_le_bytes());
    expected[36..44].copy_from_slice(&(1_u64 << 32).to_le_bytes());
    let checksum = crc32c(&expected[0..44]);
    expected[44..48].copy_from_slice(&checksum.to_le_bytes());
    assert_eq!(encoded, expected);
    assert_eq!(VLogFileHeader::decode(&encoded).unwrap(), header);

    let mut incompatible = encoded;
    incompatible[8..12].copy_from_slice(&1_u32.to_le_bytes());
    let checksum = crc32c(&incompatible[0..44]);
    incompatible[44..48].copy_from_slice(&checksum.to_le_bytes());
    assert_kind(
        VLogFileHeader::decode(&incompatible),
        StorageErrorKind::IncompatibleFormat,
    );

    let mut wrong_uuid = encoded;
    wrong_uuid[12..28].fill(0);
    let checksum = crc32c(&wrong_uuid[0..44]);
    wrong_uuid[44..48].copy_from_slice(&checksum.to_le_bytes());
    assert_kind(
        VLogFileHeader::decode(&wrong_uuid),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn tx_begin_v0_is_exactly_71_bytes() {
    let begin = begin();
    let encoded = encode_tx_begin(begin).unwrap();
    let mut expected = vec![0_u8; 71];
    expected[0..4].copy_from_slice(b"RKVR");
    expected[4..6].copy_from_slice(&0_u16.to_le_bytes());
    expected[6] = 0x01;
    expected[7..11].copy_from_slice(&71_u32.to_le_bytes());
    expected[11..19].copy_from_slice(&begin.commit_seq.to_le_bytes());
    expected[19..35].copy_from_slice(&begin.tx_uuid);
    write_header_crc(&mut expected);
    expected[39..43].copy_from_slice(&begin.vlog_begin.file_id.to_le_bytes());
    expected[43..51].copy_from_slice(&begin.vlog_begin.offset.to_le_bytes());
    expected[51..59].copy_from_slice(&begin.logical_op_count.to_le_bytes());
    expected[59..67].copy_from_slice(&begin.distinct_key_count.to_le_bytes());
    write_standard_crc(&mut expected);
    assert_eq!(encoded, expected);
    assert_eq!(
        decode_record_at(
            &encoded,
            VLogPosition {
                file_id: 3,
                offset: 64
            },
            VLogGeometry::PRODUCTION
        )
        .unwrap(),
        DecodedRecord::TxBegin(begin)
    );
}

#[test]
fn kv_record_v0_is_exactly_60_bytes_and_keeps_binary_bytes() {
    let record = KvRecordRef {
        commit_seq: 9,
        tx_uuid: tx_uuid(),
        op_index: 4,
        key: &[0x00, 0xff],
        value: &[0xff, 0x00, 0x7f],
    };
    let encoded = encode_kv_record(record).unwrap();
    let mut expected = vec![0_u8; 60];
    expected[0..4].copy_from_slice(b"RKVR");
    expected[4..6].copy_from_slice(&0_u16.to_le_bytes());
    expected[6] = 0x02;
    expected[7..11].copy_from_slice(&60_u32.to_le_bytes());
    expected[11..19].copy_from_slice(&9_u64.to_le_bytes());
    expected[19..35].copy_from_slice(&tx_uuid());
    write_header_crc(&mut expected);
    expected[39..47].copy_from_slice(&4_u64.to_le_bytes());
    expected[47..49].copy_from_slice(&2_u16.to_le_bytes());
    expected[49..51].copy_from_slice(&3_u16.to_le_bytes());
    expected[51..53].copy_from_slice(record.key);
    expected[53..56].copy_from_slice(record.value);
    write_standard_crc(&mut expected);
    assert_eq!(encoded, expected);
    assert_eq!(
        decode_record_at(
            &encoded,
            VLogPosition {
                file_id: 0,
                offset: 64
            },
            VLogGeometry::PRODUCTION
        )
        .unwrap(),
        DecodedRecord::KvRecord(record)
    );

    let empty_value = encode_kv_record(KvRecordRef {
        value: &[],
        ..record
    })
    .unwrap();
    match decode_record_at(
        &empty_value,
        VLogPosition {
            file_id: 0,
            offset: 64,
        },
        VLogGeometry::PRODUCTION,
    )
    .unwrap()
    {
        DecodedRecord::KvRecord(decoded) => assert!(decoded.value.is_empty()),
        other => panic!("unexpected record: {other:?}"),
    }
}

#[test]
fn delete_record_v0_is_exactly_56_bytes() {
    let record = DeleteRecordRef {
        commit_seq: 10,
        tx_uuid: tx_uuid(),
        op_index: 5,
        key: &[0x00, 0x7f, 0xff],
    };
    let encoded = encode_delete_record(record).unwrap();
    let mut expected = vec![0_u8; 56];
    expected[0..4].copy_from_slice(b"RKVR");
    expected[4..6].copy_from_slice(&0_u16.to_le_bytes());
    expected[6] = 0x03;
    expected[7..11].copy_from_slice(&56_u32.to_le_bytes());
    expected[11..19].copy_from_slice(&10_u64.to_le_bytes());
    expected[19..35].copy_from_slice(&tx_uuid());
    write_header_crc(&mut expected);
    expected[39..47].copy_from_slice(&5_u64.to_le_bytes());
    expected[47..49].copy_from_slice(&3_u16.to_le_bytes());
    expected[49..52].copy_from_slice(record.key);
    write_standard_crc(&mut expected);
    assert_eq!(encoded, expected);
    assert_eq!(
        decode_record_at(
            &encoded,
            VLogPosition {
                file_id: 0,
                offset: 64
            },
            VLogGeometry::PRODUCTION
        )
        .unwrap(),
        DecodedRecord::DeleteRecord(record)
    );
}

#[test]
fn tx_prepared_end_v0_is_exactly_111_bytes_with_reverse_trailer() {
    let footer = footer();
    let encoded = encode_tx_prepared_end(footer).unwrap();
    let mut expected = vec![0_u8; 111];
    expected[0..4].copy_from_slice(b"RKVR");
    expected[4..6].copy_from_slice(&0_u16.to_le_bytes());
    expected[6] = 0x04;
    expected[7..11].copy_from_slice(&111_u32.to_le_bytes());
    expected[11..19].copy_from_slice(&footer.commit_seq.to_le_bytes());
    expected[19..35].copy_from_slice(&footer.tx_uuid);
    write_header_crc(&mut expected);
    expected[39..43].copy_from_slice(&footer.vlog_begin.file_id.to_le_bytes());
    expected[43..51].copy_from_slice(&footer.vlog_begin.offset.to_le_bytes());
    expected[51..55].copy_from_slice(&footer.vlog_end.file_id.to_le_bytes());
    expected[55..63].copy_from_slice(&footer.vlog_end.offset.to_le_bytes());
    expected[63..71].copy_from_slice(&footer.logical_op_count.to_le_bytes());
    expected[71..79].copy_from_slice(&footer.distinct_key_count.to_le_bytes());
    expected[79..87].copy_from_slice(&footer.kv_record_count.to_le_bytes());
    expected[87..95].copy_from_slice(&footer.delete_record_count.to_le_bytes());
    expected[95..99].copy_from_slice(&footer.envelope_crc32c.to_le_bytes());
    expected[103..107].copy_from_slice(&111_u32.to_le_bytes());
    expected[107..111].copy_from_slice(b"RKTE");
    write_footer_crc(&mut expected);
    assert_eq!(encoded, expected);

    let start = VLogPosition {
        file_id: 3,
        offset: 1_000,
    };
    assert_eq!(
        decode_record_at(&encoded, start, VLogGeometry::PRODUCTION).unwrap(),
        DecodedRecord::TxPreparedEnd(footer)
    );
    assert_kind(
        decode_record_at(
            &encoded,
            VLogPosition {
                file_id: 3,
                offset: 999,
            },
            VLogGeometry::PRODUCTION,
        ),
        StorageErrorKind::Corruption,
    );
    assert_eq!(
        locate_footer_from_end(start, &encoded, footer.vlog_end, VLogGeometry::PRODUCTION).unwrap(),
        LocatedFooter {
            record_start: start,
            footer
        }
    );
}

#[test]
fn page_end_v0_minimum_is_exactly_43_bytes_and_padding_is_zero() {
    let position = VLogPosition {
        file_id: 4,
        offset: 65_536 - 43,
    };
    let encoded = encode_page_end(11, tx_uuid(), position, VLogGeometry::PRODUCTION).unwrap();
    let mut expected = vec![0_u8; 43];
    expected[0..4].copy_from_slice(b"RKVR");
    expected[4..6].copy_from_slice(&0_u16.to_le_bytes());
    expected[6] = 0x05;
    expected[7..11].copy_from_slice(&43_u32.to_le_bytes());
    expected[11..19].copy_from_slice(&11_u64.to_le_bytes());
    expected[19..35].copy_from_slice(&tx_uuid());
    write_header_crc(&mut expected);
    write_standard_crc(&mut expected);
    assert_eq!(encoded, expected);
    assert_eq!(
        decode_record_at(&encoded, position, VLogGeometry::PRODUCTION).unwrap(),
        DecodedRecord::PageEnd
    );

    let first_page_max = encode_page_end(
        11,
        tx_uuid(),
        VLogPosition {
            file_id: 4,
            offset: 64,
        },
        VLogGeometry::PRODUCTION,
    )
    .unwrap();
    assert_eq!(first_page_max.len(), 65_472);
    assert_eq!(
        decode_record_at(
            &first_page_max,
            VLogPosition {
                file_id: 4,
                offset: 64,
            },
            VLogGeometry::PRODUCTION,
        )
        .unwrap(),
        DecodedRecord::PageEnd
    );
    let other_page_max = encode_page_end(
        11,
        tx_uuid(),
        VLogPosition {
            file_id: 4,
            offset: 65_536 + 16,
        },
        VLogGeometry::PRODUCTION,
    )
    .unwrap();
    assert_eq!(other_page_max.len(), 65_520);
    assert_eq!(
        decode_record_at(
            &other_page_max,
            VLogPosition {
                file_id: 4,
                offset: 65_536 + 16,
            },
            VLogGeometry::PRODUCTION,
        )
        .unwrap(),
        DecodedRecord::PageEnd
    );
}

#[test]
fn record_decoder_rejects_header_before_trusting_lengths_and_types() {
    let encoded = encode_kv_record(KvRecordRef {
        commit_seq: 1,
        tx_uuid: tx_uuid(),
        op_index: 0,
        key: b"k",
        value: b"v",
    })
    .unwrap();
    let position = VLogPosition {
        file_id: 0,
        offset: 64,
    };

    let mut bad_magic = encoded.clone();
    bad_magic[0] ^= 1;
    assert_kind(
        decode_record_at(&bad_magic, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );

    let mut bad_version = encoded.clone();
    bad_version[4..6].copy_from_slice(&1_u16.to_le_bytes());
    write_header_crc(&mut bad_version);
    assert_kind(
        decode_record_at(&bad_version, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::IncompatibleFormat,
    );

    let mut bad_type = encoded.clone();
    bad_type[6] = 6;
    write_header_crc(&mut bad_type);
    assert_kind(
        decode_record_at(&bad_type, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );

    let mut malicious_len = encoded.clone();
    malicious_len[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
    write_header_crc(&mut malicious_len);
    assert_kind(
        decode_record_at(&malicious_len, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );

    let mut bad_record_crc = encoded;
    let last = bad_record_crc.len() - 1;
    bad_record_crc[last] ^= 1;
    assert_kind(
        decode_record_at(&bad_record_crc, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn key_value_boundaries_are_strict() {
    for payload_len in [59_999_usize, 60_000] {
        let value = vec![0xff; payload_len - 1];
        let encoded = encode_kv_record(KvRecordRef {
            commit_seq: 1,
            tx_uuid: tx_uuid(),
            op_index: 0,
            key: &[0],
            value: &value,
        })
        .unwrap();
        assert_eq!(encoded.len(), 55 + payload_len);
    }
    let too_large = vec![0; 60_000];
    assert_kind(
        encode_kv_record(KvRecordRef {
            commit_seq: 1,
            tx_uuid: tx_uuid(),
            op_index: 0,
            key: &[1],
            value: &too_large,
        }),
        StorageErrorKind::InvalidArgument,
    );
    assert_kind(
        encode_kv_record(KvRecordRef {
            commit_seq: 1,
            tx_uuid: tx_uuid(),
            op_index: 0,
            key: &[],
            value: &[],
        }),
        StorageErrorKind::InvalidArgument,
    );
}

#[test]
fn page_end_rejects_nonzero_padding_wrong_boundary_and_wrong_crc() {
    let position = VLogPosition {
        file_id: 0,
        offset: 65_536 - 100,
    };
    let encoded = encode_page_end(1, tx_uuid(), position, VLogGeometry::PRODUCTION).unwrap();

    let mut nonzero_padding = encoded.clone();
    nonzero_padding[40] = 1;
    write_standard_crc(&mut nonzero_padding);
    assert_kind(
        decode_record_at(&nonzero_padding, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );

    assert_kind(
        decode_record_at(
            &encoded,
            VLogPosition {
                file_id: 0,
                offset: position.offset - 1,
            },
            VLogGeometry::PRODUCTION,
        ),
        StorageErrorKind::Corruption,
    );

    let mut wrong_crc = encoded;
    let last = wrong_crc.len() - 1;
    wrong_crc[last] ^= 1;
    assert_kind(
        decode_record_at(&wrong_crc, position, VLogGeometry::PRODUCTION),
        StorageErrorKind::Corruption,
    );
}

#[test]
fn envelope_crc_uses_only_canonical_logical_bytes() {
    let begin = TxBegin {
        commit_seq: 7,
        tx_uuid: tx_uuid(),
        vlog_begin: VLogPosition {
            file_id: 0,
            offset: 0,
        },
        logical_op_count: 2,
        distinct_key_count: 1,
    };
    let kv = KvRecordRef {
        commit_seq: 7,
        tx_uuid: tx_uuid(),
        op_index: 0,
        key: b"k",
        value: b"v",
    };
    let delete = DeleteRecordRef {
        commit_seq: 7,
        tx_uuid: tx_uuid(),
        op_index: 1,
        key: b"k",
    };
    let footer = TxPreparedEnd {
        commit_seq: 7,
        tx_uuid: tx_uuid(),
        vlog_begin: begin.vlog_begin,
        vlog_end: VLogPosition {
            file_id: 0,
            offset: 400,
        },
        logical_op_count: 2,
        distinct_key_count: 1,
        kv_record_count: 1,
        delete_record_count: 1,
        envelope_crc32c: 0,
    };
    let mut incremental = EnvelopeCrc32c::new();
    incremental.update_tx_begin(begin).unwrap();
    incremental.update_kv_record(kv).unwrap();
    incremental.update_delete_record(delete).unwrap();
    let checksum = incremental.finish_with_footer(footer).unwrap();

    let mut canonical = Vec::new();
    canonical.extend_from_slice(b"RKENV0");
    canonical.push(0x01);
    canonical.extend_from_slice(&52_u32.to_le_bytes());
    canonical.extend_from_slice(&7_u64.to_le_bytes());
    canonical.extend_from_slice(&tx_uuid());
    canonical.extend_from_slice(&0_u32.to_le_bytes());
    canonical.extend_from_slice(&0_u64.to_le_bytes());
    canonical.extend_from_slice(&2_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u64.to_le_bytes());
    canonical.push(0x02);
    canonical.extend_from_slice(&38_u32.to_le_bytes());
    canonical.extend_from_slice(&7_u64.to_le_bytes());
    canonical.extend_from_slice(&tx_uuid());
    canonical.extend_from_slice(&0_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u16.to_le_bytes());
    canonical.extend_from_slice(&1_u16.to_le_bytes());
    canonical.extend_from_slice(b"kv");
    canonical.push(0x03);
    canonical.extend_from_slice(&35_u32.to_le_bytes());
    canonical.extend_from_slice(&7_u64.to_le_bytes());
    canonical.extend_from_slice(&tx_uuid());
    canonical.extend_from_slice(&1_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u16.to_le_bytes());
    canonical.extend_from_slice(b"k");
    canonical.push(0x04);
    canonical.extend_from_slice(&80_u32.to_le_bytes());
    canonical.extend_from_slice(&7_u64.to_le_bytes());
    canonical.extend_from_slice(&tx_uuid());
    canonical.extend_from_slice(&0_u32.to_le_bytes());
    canonical.extend_from_slice(&0_u64.to_le_bytes());
    canonical.extend_from_slice(&0_u32.to_le_bytes());
    canonical.extend_from_slice(&400_u64.to_le_bytes());
    canonical.extend_from_slice(&2_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u64.to_le_bytes());
    canonical.extend_from_slice(&1_u64.to_le_bytes());
    assert_eq!(checksum, crc32c(&canonical));
}

#[test]
fn malformed_lengths_never_panic_or_allocate_from_encoded_len() {
    let position = VLogPosition {
        file_id: 0,
        offset: 64,
    };
    for len in 0..112 {
        let bytes = vec![0xff; len];
        assert!(decode_record_at(&bytes, position, VLogGeometry::PRODUCTION).is_err());
    }
}

#[test]
fn every_page_and_file_header_field_detects_single_field_corruption() {
    let page_header = PageHeader {
        file_id: 7,
        page_no: 9,
    }
    .encode()
    .unwrap();
    for (field, offset) in [
        ("magic", 0),
        ("file_id", 4),
        ("page_no", 8),
        ("header_crc32c", 12),
    ] {
        let mut corrupted = page_header;
        corrupted[offset] ^= 0x80;
        let result = PageHeader::decode(&corrupted);
        assert!(
            matches!(result, Err(ref error) if error.kind == StorageErrorKind::Corruption),
            "PageHeader field {field} was not detected: {result:?}"
        );
    }

    let file_header = VLogFileHeader::new(database_uuid(), 7).encode().unwrap();
    for (field, offset) in [
        ("magic", 0),
        ("format_version", 8),
        ("database_uuid", 12),
        ("file_id", 28),
        ("page_size", 32),
        ("max_file_size", 36),
        ("header_crc32c", 44),
    ] {
        let mut corrupted = file_header;
        corrupted[offset] ^= 0x80;
        let result = VLogFileHeader::decode(&corrupted);
        assert!(
            matches!(result, Err(ref error) if error.kind == StorageErrorKind::Corruption),
            "VLogFileHeader field {field} was not detected: {result:?}"
        );
    }
}

#[test]
fn every_tx_begin_field_detects_single_field_corruption() {
    let encoded = encode_tx_begin(begin()).unwrap();
    assert_record_fields_detect_corruption(
        &encoded,
        VLogPosition {
            file_id: 3,
            offset: 64,
        },
        &[
            ("magic", 0),
            ("format_version", 4),
            ("record_type", 6),
            ("encoded_len", 7),
            ("commit_seq", 11),
            ("tx_uuid", 19),
            ("header_crc32c", 35),
            ("vlog_begin.file_id", 39),
            ("vlog_begin.offset", 43),
            ("logical_op_count", 51),
            ("distinct_key_count", 59),
            ("record_crc32c", 67),
        ],
    );
}

#[test]
fn every_kv_and_delete_record_field_detects_single_field_corruption() {
    let position = VLogPosition {
        file_id: 0,
        offset: 64,
    };
    let kv = encode_kv_record(KvRecordRef {
        commit_seq: 9,
        tx_uuid: tx_uuid(),
        op_index: 4,
        key: &[0x00, 0xff],
        value: &[0xff, 0x00, 0x7f],
    })
    .unwrap();
    assert_record_fields_detect_corruption(
        &kv,
        position,
        &[
            ("magic", 0),
            ("format_version", 4),
            ("record_type", 6),
            ("encoded_len", 7),
            ("commit_seq", 11),
            ("tx_uuid", 19),
            ("header_crc32c", 35),
            ("op_index", 39),
            ("key_len", 47),
            ("value_len", 49),
            ("key", 51),
            ("value", 53),
            ("record_crc32c", kv.len() - 4),
        ],
    );

    let delete = encode_delete_record(DeleteRecordRef {
        commit_seq: 10,
        tx_uuid: tx_uuid(),
        op_index: 5,
        key: &[0x00, 0x7f, 0xff],
    })
    .unwrap();
    assert_record_fields_detect_corruption(
        &delete,
        position,
        &[
            ("magic", 0),
            ("format_version", 4),
            ("record_type", 6),
            ("encoded_len", 7),
            ("commit_seq", 11),
            ("tx_uuid", 19),
            ("header_crc32c", 35),
            ("op_index", 39),
            ("key_len", 47),
            ("key", 49),
            ("record_crc32c", delete.len() - 4),
        ],
    );
}

#[test]
fn every_footer_and_page_end_field_detects_single_field_corruption() {
    let footer = footer();
    let footer_bytes = encode_tx_prepared_end(footer).unwrap();
    assert_record_fields_detect_corruption(
        &footer_bytes,
        VLogPosition {
            file_id: 3,
            offset: 1_000,
        },
        &[
            ("magic", 0),
            ("format_version", 4),
            ("record_type", 6),
            ("encoded_len", 7),
            ("commit_seq", 11),
            ("tx_uuid", 19),
            ("header_crc32c", 35),
            ("vlog_begin.file_id", 39),
            ("vlog_begin.offset", 43),
            ("vlog_end.file_id", 51),
            ("vlog_end.offset", 55),
            ("logical_op_count", 63),
            ("distinct_key_count", 71),
            ("kv_record_count", 79),
            ("delete_record_count", 87),
            ("envelope_crc32c", 95),
            ("record_crc32c", 99),
            ("end_record_len", 103),
            ("tail_magic", 107),
        ],
    );

    let page_end_position = VLogPosition {
        file_id: 4,
        offset: VLOG_PAGE_SIZE - 100,
    };
    let page_end =
        encode_page_end(11, tx_uuid(), page_end_position, VLogGeometry::PRODUCTION).unwrap();
    assert_record_fields_detect_corruption(
        &page_end,
        page_end_position,
        &[
            ("magic", 0),
            ("format_version", 4),
            ("record_type", 6),
            ("encoded_len", 7),
            ("commit_seq", 11),
            ("tx_uuid", 19),
            ("header_crc32c", 35),
            ("zero_padding", 39),
            ("record_crc32c", page_end.len() - 4),
        ],
    );
}
